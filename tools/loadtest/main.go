// Load generator for the logden ingest gateway (stdlib only).
//
//	go run . -token $LOG_TOKEN -duration 10s -concurrency 8 -batch 50
//
// Sends batches to POST /logs from N workers for the given time and prints throughput.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	url := flag.String("url", "http://localhost:8080", "gateway base URL")
	token := flag.String("token", os.Getenv("LOG_TOKEN"), "auth token (or LOG_TOKEN)")
	project := flag.String("project", "loadtest", "project name")
	conc := flag.Int("concurrency", 8, "concurrent workers")
	dur := flag.Duration("duration", 10*time.Second, "test duration")
	batch := flag.Int("batch", 50, "events per request")
	flag.Parse()
	if *token == "" {
		fmt.Fprintln(os.Stderr, "set -token or LOG_TOKEN")
		os.Exit(2)
	}

	events := make([]map[string]any, *batch)
	for i := range events {
		events[i] = map[string]any{
			"project": *project, "level": "info",
			"message": "load test event", "context": map[string]any{"i": i},
		}
	}
	body, _ := json.Marshal(events)

	client := &http.Client{
		Timeout:   10 * time.Second,
		Transport: &http.Transport{MaxIdleConns: *conc * 2, MaxIdleConnsPerHost: *conc * 2},
	}
	var sent, errs atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var wg sync.WaitGroup
	start := time.Now()
	for w := 0; w < *conc; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, *url+"/logs", bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+*token)
				req.Header.Set("Content-Type", "application/json")
				resp, err := client.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				_ = resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					sent.Add(int64(*batch))
				} else {
					errs.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	total := sent.Load()
	fmt.Printf("sent=%d errors=%d duration=%s throughput=%.0f events/s\n",
		total, errs.Load(), elapsed.Round(time.Millisecond), float64(total)/elapsed.Seconds())
}
