package main

import (
	"fmt"
	"net/http/httptest"
	"os"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// Opt-in memory probe: drives the gateway into the worst case its byte caps
// admit — the buffer full to BUFFER_MAX_BYTES while MAX_INFLIGHT_BODY_BYTES of
// max-size requests are being parsed — and checks the peak stays inside the
// container limit. Not part of the default suite: the numbers depend on the GC,
// the Go version and the host, so it is a tool to re-run when a cap changes, not
// a CI gate.
//
//	cd gateway && LOGDEN_MEM_PROBE=1 GOMEMLIMIT=80MiB go test -run WorstCaseHeap -v
func TestWorstCaseHeapBudget(t *testing.T) {
	if os.Getenv("LOGDEN_MEM_PROBE") == "" {
		t.Skip("set LOGDEN_MEM_PROBE=1 to run the memory probe")
	}
	// docker-compose.yml mem_limit for the gateway.
	const memLimit = 128 << 20

	cfg := config{
		listenAddr:       ":0",
		tokens:           []string{"secret"},
		chDatabase:       "logs",
		chTable:          "logs",
		bufferSize:       2000,
		bufferMaxBytes:   32 << 20,
		batchSize:        500,
		maxRetries:       0,
		maxBodyBytes:     4 << 20,
		maxInflightBytes: 16 << 20,
		maxMessageBytes:  64 << 10,
		maxContextBytes:  64 << 10,
		maxBatchEvents:   1000,
		retention:        720 * 3600 * 1e9,
	}
	s := newServer(cfg) // worker not started: the buffer fills to its byte cap

	// Worst realistic body: quote/backslash-dense so rows re-serialize larger.
	msg := strings.Repeat(`\"a\\b`, 1300) // sized so 2000 rows reach the 32MiB byte cap
	ctx := `{"k":"` + strings.Repeat(`\"x\\y`, 1300) + `"}`
	one := fmt.Sprintf(`{"project":"p","message":"%s","context":%s}`, msg, ctx)
	var b strings.Builder
	b.WriteString("[")
	for b.Len() < (4<<20)-len(one)-8 {
		if b.Len() > 1 {
			b.WriteString(",")
		}
		b.WriteString(one)
	}
	b.WriteString("]")
	body := b.String()
	t.Logf("body = %d bytes", len(body))

	var peak, peakRSS uint64
	var mu sync.Mutex
	sample := func() {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		rss := ms.Sys - ms.HeapReleased // what the cgroup actually charges us
		mu.Lock()
		if ms.HeapAlloc > peak {
			peak = ms.HeapAlloc
		}
		if rss > peakRSS {
			peakRSS = rss
		}
		mu.Unlock()
	}

	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(2 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				sample()
			}
		}
	}()

	const conc = 4 // 16MiB inflight / 4MiB per gzip-or-chunked request
	for round := 0; round < 6; round++ {
		var wg sync.WaitGroup
		for i := 0; i < conc; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req := httptest.NewRequest("POST", "/logs", strings.NewReader(body))
				req.Header.Set("Authorization", "Bearer secret")
				req.ContentLength = -1 // chunked: worst-case reservation
				s.mux().ServeHTTP(httptest.NewRecorder(), req)
			}()
		}
		wg.Wait()
		t.Logf("round %d: buffer events=%d bytes=%d", round, s.ingest.depth(), s.ingest.depthBytes())
	}
	close(stop)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	t.Logf("PEAK HeapAlloc = %.1f MiB; PEAK RSS-proxy = %.1f MiB; final HeapAlloc = %.1f MiB; Sys = %.1f MiB; released = %.1f MiB; buffer = %.1f MiB",
		float64(peak)/(1<<20), float64(peakRSS)/(1<<20), float64(ms.HeapAlloc)/(1<<20),
		float64(ms.Sys)/(1<<20), float64(ms.HeapReleased)/(1<<20), float64(s.ingest.depthBytes())/(1<<20))

	// RSS is what the cgroup kills for: Sys minus the pages already handed back.
	if peakRSS > memLimit {
		t.Fatalf("peak RSS %.1f MiB exceeds the container limit of %d MiB — lower the byte caps or raise mem_limit",
			float64(peakRSS)/(1<<20), memLimit>>20)
	}
}
