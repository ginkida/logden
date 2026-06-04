package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// chStatusError — ClickHouse ответил HTTP-статусом ошибки (в отличие от
// транспортного сбоя, когда ответа не было вовсе).
type chStatusError struct {
	code int
	msg  string
}

func (e *chStatusError) Error() string { return fmt.Sprintf("clickhouse %d: %s", e.code, e.msg) }

// ingester — конвейер приёма: ограниченный буфер -> батч-воркер -> вставка в
// ClickHouse с ретраями. При исчерпании ретраев батч уходит в дисковый спул
// (если включён) и переигрывается позже. Это закрывает потерю логов на
// кратких сбоях/деплоях и переполнении.
type ingester struct {
	cfg       config
	m         *metrics
	ch        chan []byte
	client    *http.Client
	insertURL string
	wg        sync.WaitGroup
	enqueueMu sync.Mutex
	closed    bool // под enqueueMu: канал закрыт, enqueue больше не шлёт

	spoolMu    sync.Mutex
	spoolSeq   uint64
	stopReplay chan struct{}
	draining   atomic.Bool
}

func newIngester(cfg config, m *metrics) *ingester {
	transport := &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 64,
		IdleConnTimeout:     90 * time.Second,
	}
	insertURL := cfg.chBaseURL + "/?" + url.Values{
		"query": {fmt.Sprintf(
			"INSERT INTO %s.%s (timestamp, project, level, message, context, source_ip) FORMAT JSONEachRow",
			cfg.chDatabase, cfg.chTable)},
		"async_insert":          {"1"},
		"wait_for_async_insert": {"1"}, // ждём подтверждения флаша => знаем об ошибке
	}.Encode()

	return &ingester{
		cfg:        cfg,
		m:          m,
		ch:         make(chan []byte, cfg.bufferSize),
		client:     &http.Client{Timeout: 30 * time.Second, Transport: transport},
		insertURL:  insertURL,
		stopReplay: make(chan struct{}),
	}
}

func (ing *ingester) depth() int    { return len(ing.ch) }
func (ing *ingester) capacity() int { return cap(ing.ch) }

// enqueue сериализует строки, проставляет source_ip и неблокирующе кладёт в
// буфер. Возвращает число принятых и отброшенных (буфер полон).
func (ing *ingester) enqueue(rows []row, ip string) (accepted, dropped int) {
	// Сериализуем строки вне блокировки (CPU не держит мьютекс).
	lines := make([][]byte, 0, len(rows))
	for i := range rows {
		rows[i].SourceIP = ip
		if line, err := json.Marshal(&rows[i]); err == nil {
			lines = append(lines, line)
		} else {
			dropped++
		}
	}
	// Всё-или-ничего ПОД мьютексом: проверка вместимости и постановка атомарны
	// относительно других хендлеров (иначе ретрай батча давал бы дубликаты уже
	// принятых строк). Воркер только вычитывает, так что под локом место не убывает.
	ing.enqueueMu.Lock()
	defer ing.enqueueMu.Unlock()
	if ing.closed || len(ing.ch)+len(lines) > cap(ing.ch) {
		return 0, dropped + len(lines)
	}
	for _, line := range lines {
		select {
		case ing.ch <- line:
			accepted++
		default:
			dropped++
		}
	}
	return accepted, dropped
}

func (ing *ingester) start() {
	ing.wg.Add(1)
	go ing.worker()

	if ing.cfg.spoolDir != "" {
		if err := os.MkdirAll(ing.cfg.spoolDir, 0o750); err != nil {
			slog.Error("cannot create spool dir", "dir", ing.cfg.spoolDir, "err", err)
		} else {
			ing.cleanupTmp() // до воркера/реплея: мёртвые .tmp от крэша между записью и rename
			ing.updateSpoolGauge()
			ing.wg.Add(1)
			go ing.replayLoop()
		}
	}
}

// stop вызывается после остановки HTTP-сервера (handlers уже не пишут):
// закрываем буфер, воркер дренирует остаток и флашит, ждём завершения.
func (ing *ingester) stop() {
	ing.draining.Store(true) // на дренаже не залипаем в долгих ретраях — сразу спул
	close(ing.stopReplay)    // остановить реплей ДО дренажа (без гонки за каталог)
	// Закрываем канал СИНХРОННО с enqueue: иначе send-on-closed-channel паника
	// (Shutdown мог истечь по таймауту, оставив медленный handler в enqueue).
	ing.enqueueMu.Lock()
	ing.closed = true
	close(ing.ch)
	ing.enqueueMu.Unlock()
	ing.wg.Wait() // ждём и worker, и replayLoop
}

func (ing *ingester) worker() {
	defer ing.wg.Done()
	ticker := time.NewTicker(ing.cfg.flushInterval)
	defer ticker.Stop()

	batch := make([][]byte, 0, ing.cfg.batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		ing.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case line, ok := <-ing.ch:
			if !ok {
				flush()
				return
			}
			batch = append(batch, line)
			if len(batch) >= ing.cfg.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (ing *ingester) flush(batch [][]byte) {
	body := bytes.Join(batch, []byte("\n"))
	if err := ing.insertWithRetry(body); err != nil {
		ing.m.insertFailed.Add(1)
		slog.Error("insert failed after retries", "events", len(batch), "err", err)
		ing.spool(body, len(batch))
		return
	}
	ing.m.inserted.Add(int64(len(batch)))
}

func (ing *ingester) insertWithRetry(body []byte) error {
	var err error
	retries := ing.cfg.maxRetries
	if ing.draining.Load() {
		retries = 0 // shutdown: одна попытка, остаток в спул (durable), не залипаем
	}
	backoff := 200 * time.Millisecond
	for attempt := 0; attempt <= retries; attempt++ {
		if attempt > 0 {
			ing.m.insertRetries.Add(1)
			time.Sleep(backoff)
			if backoff < 5*time.Second {
				backoff *= 2
			}
		}
		if err = ing.insertOnce(body); err == nil {
			return nil
		}
	}
	return err
}

func (ing *ingester) insertOnce(body []byte) error {
	timeout := 15 * time.Second
	if ing.draining.Load() {
		timeout = 3 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ing.insertURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("X-ClickHouse-User", ing.cfg.chUser)
	if ing.cfg.chKey != "" {
		req.Header.Set("X-ClickHouse-Key", ing.cfg.chKey)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	start := time.Now()
	resp, err := ing.client.Do(req)
	ing.m.insertDur.observe(time.Since(start).Seconds())
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &chStatusError{code: resp.StatusCode, msg: string(bytes.TrimSpace(b))}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// --- дисковый спул ---

func (ing *ingester) spool(body []byte, n int) {
	if ing.cfg.spoolDir == "" {
		ing.m.dropped.Add(int64(n))
		return
	}
	if int(ing.m.spoolFiles.Load()) >= ing.cfg.spoolMaxFiles {
		ing.m.dropped.Add(int64(n))
		slog.Error("spool full, dropping batch", "events", n)
		return
	}

	ing.spoolMu.Lock()
	ing.spoolSeq++
	seq := ing.spoolSeq
	ing.spoolMu.Unlock()

	name := filepath.Join(ing.cfg.spoolDir, fmt.Sprintf("%d-%012d.ndjson", os.Getpid(), seq))
	tmp := name + ".tmp"
	if err := os.WriteFile(tmp, body, 0o640); err != nil {
		slog.Error("spool write failed", "err", err)
		ing.m.dropped.Add(int64(n))
		return
	}
	if err := os.Rename(tmp, name); err != nil {
		slog.Error("spool rename failed", "err", err)
		ing.m.dropped.Add(int64(n))
		return
	}
	ing.updateSpoolGauge()
}

func (ing *ingester) replayLoop() {
	defer ing.wg.Done()
	interval := ing.cfg.replayInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ing.stopReplay:
			return
		case <-ticker.C:
			ing.replayOnce()
		}
	}
}

func (ing *ingester) replayOnce() {
	names := ing.spoolFilesList()
	for _, name := range names {
		select {
		case <-ing.stopReplay: // shutdown: не залипаем в полном свипе спула
			return
		default:
		}
		path := filepath.Join(ing.cfg.spoolDir, name)
		body, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		if err := ing.insertOnce(body); err != nil {
			// 400 = ClickHouse разобрал запрос и отверг сами данные (битый файл,
			// несовместимость после миграции). Ретраи бесполезны, а вечный return
			// блокировал бы весь спул за этим файлом — карантиним в .bad и идём
			// дальше. Остальные ошибки (транспорт, 5xx, auth) — временные/общие:
			// прерываем свип до следующего тика, как раньше.
			var se *chStatusError
			if errors.As(err, &se) && se.code == http.StatusBadRequest {
				ing.quarantine(path, name, body, err)
				continue
			}
			slog.Warn("spool replay failed, will retry later", "file", name, "err", err)
			return // ClickHouse ещё недоступен — следующий тик
		}
		n := bytes.Count(body, []byte("\n")) + 1
		ing.m.inserted.Add(int64(n))
		_ = os.Remove(path)
		ing.updateSpoolGauge()
	}
}

// quarantine убирает отвергнутый ClickHouse'ом файл из очереди реплея,
// переименовывая его в *.bad (suffix-фильтр спула его больше не видит).
// Файл остаётся на диске для ручного разбора — см. RUNBOOK.
func (ing *ingester) quarantine(path, name string, body []byte, cause error) {
	bad := path + ".bad"
	if err := os.Rename(path, bad); err != nil {
		slog.Error("spool quarantine failed", "file", name, "err", err)
		return
	}
	n := bytes.Count(body, []byte("\n")) + 1
	ing.m.dropped.Add(int64(n)) // доставка не состоялась — это потеря, алерт должен сработать
	ing.m.spoolQuarantined.Add(1)
	slog.Error("spool batch rejected by clickhouse, quarantined", "file", name+".bad", "events", n, "err", cause)
	ing.updateSpoolGauge()
}

// cleanupTmp удаляет осиротевшие *.tmp (крэш между WriteFile и Rename):
// их никто не реплеит, лимит SPOOL_MAX_FILES их не учитывает.
func (ing *ingester) cleanupTmp() {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".tmp") {
			_ = os.Remove(filepath.Join(ing.cfg.spoolDir, e.Name()))
		}
	}
}

func (ing *ingester) spoolFilesList() []string {
	entries, err := os.ReadDir(ing.cfg.spoolDir)
	if err != nil {
		return nil
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".ndjson") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

func (ing *ingester) updateSpoolGauge() {
	ing.m.spoolFiles.Store(int64(len(ing.spoolFilesList())))
}
