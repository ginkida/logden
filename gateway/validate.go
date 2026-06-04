package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// row — одна строка в формате колонок таблицy logs.logs (JSONEachRow).
// Context хранится JSON-строкой; Timestamp опускается, если клиент не прислал
// валидное время — тогда ClickHouse подставит DEFAULT now64(3).
type row struct {
	Timestamp string `json:"timestamp,omitempty"`
	Project   string `json:"project"`
	Level     string `json:"level"`
	Message   string `json:"message"`
	Context   string `json:"context"`
	SourceIP  string `json:"source_ip"`
}

type inEvent struct {
	Project   string          `json:"project"`
	Level     string          `json:"level"`
	Message   string          `json:"message"`
	Context   json.RawMessage `json:"context"`
	Timestamp json.RawMessage `json:"timestamp"`
}

var projectRe = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

var levelAliases = map[string]string{
	"warn": "warning", "err": "error", "fatal": "critical", "panic": "emergency", "trace": "debug",
}

// PSR-3 / syslog severities — совпадает с уровнями в queries.sql.
var allowedLevels = map[string]bool{
	"debug": true, "info": true, "notice": true, "warning": true,
	"error": true, "critical": true, "alert": true, "emergency": true,
}

// readBody распаковывает gzip (с защитой от gzip-bomb) и ограничивает размер.
func (s *server) readBody(r *http.Request) ([]byte, int, string) {
	limit := s.cfg.maxBodyBytes
	var src io.Reader = io.LimitReader(r.Body, limit+1)
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(io.LimitReader(r.Body, limit+1))
		if err != nil {
			return nil, http.StatusBadRequest, "bad_gzip"
		}
		defer gz.Close()
		src = io.LimitReader(gz, limit+1) // ограничиваем РАСПАКОВАННЫЙ объём
	}
	data, err := io.ReadAll(src)
	if err != nil {
		return nil, http.StatusBadRequest, "read_error"
	}
	if int64(len(data)) > limit {
		return nil, http.StatusRequestEntityTooLarge, "too_large"
	}
	return data, 0, ""
}

// parseBatch принимает один объект, JSON-массив или NDJSON и возвращает строки.
func (s *server) parseBatch(r *http.Request) ([]row, int, string) {
	data, code, reason := s.readBody(r)
	if code != 0 {
		return nil, code, reason
	}
	trimmed := bytes.TrimLeft(data, " \t\r\n")
	if len(trimmed) == 0 {
		return nil, http.StatusBadRequest, "empty"
	}

	var raws []json.RawMessage
	if trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &raws); err != nil {
			return nil, http.StatusBadRequest, "bad_json"
		}
	} else {
		// Один объект или NDJSON: декодер читает последовательность JSON-значений,
		// разделённых пробелами/переводами строк.
		dec := json.NewDecoder(bytes.NewReader(trimmed))
		for {
			var raw json.RawMessage
			err := dec.Decode(&raw)
			if err == io.EOF {
				break
			}
			if err != nil {
				// Синтаксически битый ввод => 400 (наблюдаемо), а не тихая потеря
				// хвоста после break. Семантически невалидные (но валидный JSON)
				// элементы пропускаются поэлементно ниже.
				return nil, http.StatusBadRequest, "bad_json"
			}
			raws = append(raws, raw)
			if len(raws) > s.cfg.maxBatchEvents {
				return nil, http.StatusRequestEntityTooLarge, "too_many_events"
			}
		}
	}

	if len(raws) == 0 {
		return nil, http.StatusBadRequest, "empty"
	}
	if len(raws) > s.cfg.maxBatchEvents {
		return nil, http.StatusRequestEntityTooLarge, "too_many_events"
	}

	// Частичный приём: один битый элемент не должен ронять весь батч (иначе одна
	// плохая строка теряет тысячи хороших, а ретрай повторяет ту же ошибку).
	rows := make([]row, 0, len(raws))
	for _, raw := range raws {
		var e inEvent
		if err := json.Unmarshal(raw, &e); err != nil {
			s.m.rejected.inc(`reason="invalid_event"`)
			continue
		}
		rw, ok := s.buildRow(e)
		if !ok {
			s.m.rejected.inc(`reason="invalid_event"`)
			continue
		}
		rows = append(rows, rw)
	}
	if len(rows) == 0 {
		return nil, http.StatusBadRequest, "invalid_event"
	}
	return rows, 0, ""
}

func (s *server) buildRow(e inEvent) (row, bool) {
	project := strings.TrimSpace(e.Project)
	if !projectRe.MatchString(project) {
		return row{}, false
	}
	if strings.TrimSpace(e.Message) == "" {
		return row{}, false
	}
	msg := e.Message
	if len(msg) > s.cfg.maxMessageBytes {
		const suffix = "…[truncated]"
		if s.cfg.maxMessageBytes <= len(suffix) {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes], "")
		} else {
			msg = strings.ToValidUTF8(msg[:s.cfg.maxMessageBytes-len(suffix)], "") + suffix
		}
		// итог всегда <= maxMessageBytes
	}
	return row{
		Timestamp: normalizeTimestamp(e.Timestamp, s.cfg.retention),
		Project:   project,
		Level:     normalizeLevel(e.Level),
		Message:   msg,
		Context:   normalizeContext(e.Context, s.cfg.maxContextBytes),
	}, true
}

func normalizeLevel(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" {
		return "info"
	}
	if a, ok := levelAliases[s]; ok {
		s = a
	}
	if allowedLevels[s] {
		return s
	}
	return "info"
}

func normalizeContext(raw json.RawMessage, max int) string {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || string(t) == "null" {
		return "{}"
	}
	if !json.Valid(t) {
		return `{"_invalid_json":true}`
	}
	if len(t) > max {
		return `{"_truncated":true,"_orig_bytes":` + strconv.Itoa(len(t)) + `}`
	}
	return string(t)
}

// normalizeTimestamp принимает RFC3339 или unix (сек/мс). Возвращает время в
// формате ClickHouse или "" (тогда CH поставит время вставки). Защита от
// мусора: будущее >+5мин и старше ретеншна отбрасываются.
func normalizeTimestamp(raw json.RawMessage, retention time.Duration) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || string(raw) == "null" {
		return ""
	}
	var t time.Time
	if raw[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return ""
		}
		parsed, err := time.Parse(time.RFC3339Nano, strings.TrimSpace(str))
		if err != nil {
			return ""
		}
		t = parsed
	} else {
		f, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return ""
		}
		if f > 1e12 {
			t = time.UnixMilli(int64(f))
		} else {
			sec := int64(f)
			t = time.Unix(sec, int64((f-float64(sec))*1e9)) // сохраняем дробные секунды
		}
	}
	t = t.UTC()
	now := time.Now().UTC()
	if t.After(now.Add(5*time.Minute)) || t.Before(now.Add(-retention)) {
		return ""
	}
	return t.Format("2006-01-02 15:04:05.000")
}
