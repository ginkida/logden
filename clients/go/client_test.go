package logden

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestClientSendAndBatch(t *testing.T) {
	var total atomic.Int64
	var sawAuth atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer tok" {
			sawAuth.Store(true)
		}
		b, _ := io.ReadAll(r.Body)
		var arr []map[string]any
		_ = json.Unmarshal(b, &arr)
		total.Add(int64(len(arr)))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	// без батчинга — отправка сразу
	c := New(srv.URL, "tok", "proj")
	if err := c.Error("boom", map[string]any{"x": 1}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if total.Load() != 1 {
		t.Fatalf("non-batch: want 1 got %d", total.Load())
	}
	if !sawAuth.Load() {
		t.Fatal("Authorization header not propagated")
	}

	// батчинг по размеру
	bc := New(srv.URL, "tok", "proj", WithBatch(3, time.Hour))
	_ = bc.Info("a", nil)
	_ = bc.Info("b", nil)
	if total.Load() != 1 {
		t.Fatalf("should not flush before reaching batch size, got %d", total.Load())
	}
	_ = bc.Info("c", nil) // достигли 3 -> флаш
	if total.Load() != 4 {
		t.Fatalf("batch flush: want 4 got %d", total.Load())
	}
	_ = bc.Warn("d", nil)
	if err := bc.Close(); err != nil { // дослать остаток
		t.Fatalf("close: %v", err)
	}
	if total.Load() != 5 {
		t.Fatalf("close flush: want 5 got %d", total.Load())
	}
	if err := bc.Close(); err != nil { // повторный Close должен быть безопасен
		t.Fatalf("second close: %v", err)
	}
}
