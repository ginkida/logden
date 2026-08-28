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
	// Byte caps: 0 disables a cap; a non-zero cap must admit at least one
	// max-size request, otherwise the gateway rejects everything.
	add(c.bufferMaxBytes < 0, "BUFFER_MAX_BYTES must be >= 0 (0 = unlimited)")
	add(c.spoolMaxBytes < 0, "SPOOL_MAX_BYTES must be >= 0 (0 = unlimited)")
	add(c.maxInflightBytes < 0, "MAX_INFLIGHT_BODY_BYTES must be >= 0 (0 = unlimited)")
	// Rows re-serialize LARGER than the raw body (context is re-escaped, a
	// per-row source_ip is added), by up to ~2x — so the buffer floor is 2×
	// MAX_BODY_BYTES to keep one max-size request always enqueueable on an empty
	// buffer. It is a floor, not a proven ceiling: any payload whose every byte
	// re-encodes 2:1 lands a hair above it (measured at 2.00x for a context of
	// raw U+2028/U+2029, which the encoder always escapes, and for invalid bytes
	// alternating with valid ones; quote-dense ASCII is milder at ~1.7x), so a
	// config pinned exactly at the floor answers 503 to such a request instead of
	// buffering it — the shipped defaults leave 8×. What parseBatch's run-wise
	// sanitizer removed is the shape that beat the floor outright: a solid run of
	// invalid UTF-8 reached ~3x, because the JSON decoder expands every bad byte
	// in `message` into a 3-byte U+FFFD. The inflight floor only needs 1×: that
	// budget is charged for the raw bytes read, not for the rows.
	add(c.bufferMaxBytes > 0 && c.bufferMaxBytes < 2*c.maxBodyBytes,
		"BUFFER_MAX_BYTES must be >= 2× MAX_BODY_BYTES (serialized rows are larger than the raw body)")
	add(c.maxInflightBytes > 0 && c.maxInflightBytes < c.maxBodyBytes,
		"MAX_INFLIGHT_BODY_BYTES must be >= MAX_BODY_BYTES (one max-size request must fit)")

	u, err := url.Parse(c.chBaseURL)
	switch {
	case err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https"):
		errs = append(errs, "CLICKHOUSE_URL must be a valid http(s) URL")
	case u.Path != "" && u.Path != "/":
		// The insert URL is built as chBaseURL + "/?query=...", so a path prefix
		// silently points every insert at something that is not ClickHouse's
		// query handler.
		errs = append(errs, "CLICKHOUSE_URL must not contain a path")
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.User != nil:
		// Same concatenation, same breakage: a fragment swallows the entire
		// "?query=INSERT ..." — ClickHouse then parses the NDJSON body as SQL,
		// answers 400, and every batch is spooled and quarantined as .bad — while
		// a query string (or a bare "?") makes the join emit a second "?", so the
		// settings arrive as one unknown parameter and ClickHouse refuses them.
		// Credentials cannot work either: the gateway authenticates with the
		// X-ClickHouse-* headers, and the URL is logged verbatim at startup.
		errs = append(errs, "CLICKHOUSE_URL must be scheme://host[:port] only - no query, fragment or credentials")
	}

	if len(errs) > 0 {
		return errors.New("invalid config: " + strings.Join(errs, "; "))
	}
	return nil
}
