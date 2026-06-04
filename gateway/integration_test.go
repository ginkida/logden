//go:build integration

package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Run against a real ClickHouse. Run with:
//
//	CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=default \
//	  go test -tags=integration ./...
func TestIntegrationInsert(t *testing.T) {
	chURL := os.Getenv("CLICKHOUSE_URL")
	if chURL == "" {
		t.Skip("CLICKHOUSE_URL not set")
	}
	chUser := envOr("CLICKHOUSE_USER", "default")
	chKey := os.Getenv("CLICKHOUSE_PASSWORD")

	applySchema(t, chURL, chUser, chKey)

	cfg := testConfig()
	cfg.chBaseURL = chURL
	cfg.chUser = chUser
	cfg.chKey = chKey
	cfg.batchSize = 1
	cfg.flushInterval = 50 * time.Millisecond
	s := newServer(cfg)
	s.ingest.start()
	defer s.ingest.stop()

	project := "itest_" + strconv.FormatInt(time.Now().UnixNano(), 36)
	body := fmt.Sprintf(`{"project":%q,"level":"error","message":"integration","context":{"k":1}}`, project)
	if rr := doLogs(s, "POST", "secret", body, nil); rr.Code != http.StatusNoContent {
		t.Fatalf("ingest: want 204 got %d (%s)", rr.Code, rr.Body.String())
	}

	deadline := time.Now().Add(15 * time.Second)
	var count string
	for time.Now().Before(deadline) {
		count = strings.TrimSpace(chQuery(t, chURL, chUser, chKey,
			fmt.Sprintf("SELECT count() FROM logs.logs WHERE project='%s'", project)))
		if count == "1" {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if count != "1" {
		t.Fatalf("expected 1 row for %s, got %q", project, count)
	}

	got := strings.TrimSpace(chQuery(t, chURL, chUser, chKey,
		fmt.Sprintf("SELECT level || '|' || message || '|' || context FROM logs.logs WHERE project='%s' LIMIT 1", project)))
	if got != `error|integration|{"k":1}` {
		t.Fatalf("row mismatch: %q", got)
	}
}

// applySchema applies clickhouse/schema.sql (several DDL statements split on ';').
func applySchema(t *testing.T, base, user, key string) {
	t.Helper()
	data, err := os.ReadFile("../clickhouse/schema.sql")
	if err != nil {
		t.Fatalf("read schema.sql: %v", err)
	}
	for _, stmt := range strings.Split(string(data), ";") {
		if isCommentOnly(stmt) {
			continue // splitting on ';' may carve off a comment — skip chunks with no SQL
		}
		chQuery(t, base, user, key, stmt)
	}
}

func isCommentOnly(s string) bool {
	for _, ln := range strings.Split(s, "\n") {
		t := strings.TrimSpace(ln)
		if t != "" && !strings.HasPrefix(t, "--") {
			return false
		}
	}
	return true
}

func chQuery(t *testing.T, base, user, key, q string) string {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+"/", bytes.NewReader([]byte(q)))
	req.Header.Set("X-ClickHouse-User", user)
	if key != "" {
		req.Header.Set("X-ClickHouse-Key", key)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ch query: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("ch query %d: %s", resp.StatusCode, b)
	}
	return string(b)
}
