package main

import (
	"errors"
	"net/url"
	"strings"
)

// validate checks configuration invariants at startup so we fail fast with a
// clear error instead of hitting a ticker/slice panic under load.
func (c config) validate() error {
	var errs []string
	add := func(cond bool, msg string) {
		if cond {
			errs = append(errs, msg)
		}
	}

	add(c.listenAddr == "", "LISTEN_ADDR must not be empty")
	add(c.bufferSize <= 0, "BUFFER_SIZE must be > 0")
	add(c.batchSize <= 0, "BATCH_SIZE must be > 0")
	add(c.flushInterval <= 0, "FLUSH_INTERVAL must be > 0")
	add(c.replayInterval <= 0, "REPLAY_INTERVAL must be > 0")
	add(c.maxRetries < 0, "MAX_RETRIES must be >= 0")
	add(c.maxBodyBytes <= 0, "MAX_BODY_BYTES must be > 0")
	// >= 16 so the truncation suffix fits and the len<=limit invariant holds.
	add(c.maxMessageBytes < 16, "MAX_MESSAGE_BYTES must be >= 16")
	add(c.maxContextBytes <= 0, "MAX_CONTEXT_BYTES must be > 0")
	add(c.maxBatchEvents <= 0, "MAX_BATCH_EVENTS must be > 0")
	add(c.retention <= 0, "RETENTION must be > 0")
	add(c.rateLimit < 0, "RATE_LIMIT_RPS must be >= 0")
	add(c.spoolDir != "" && c.spoolMaxFiles <= 0, "SPOOL_MAX_FILES must be > 0 when SPOOL_DIR is set")

	if u, err := url.Parse(c.chBaseURL); err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		errs = append(errs, "CLICKHOUSE_URL must be a valid http(s) URL")
	}

	if len(errs) > 0 {
		return errors.New("invalid config: " + strings.Join(errs, "; "))
	}
	return nil
}
