package main

import (
	"testing"
	"time"
)

func validBaseConfig() config {
	return config{
		listenAddr:      ":8080",
		tokens:          []string{"t"},
		chBaseURL:       "http://127.0.0.1:8123",
		chUser:          "writer",
		chDatabase:      "logs",
		chTable:         "logs",
		bufferSize:      2000,
		batchSize:       500,
		flushInterval:   time.Second,
		replayInterval:  30 * time.Second,
		maxRetries:      3,
		maxBodyBytes:    4 << 20,
		maxMessageBytes: 64 << 10,
		maxContextBytes: 64 << 10,
		maxBatchEvents:  1000,
		retention:       30 * 24 * time.Hour,
		spoolMaxFiles:   1000,
	}
}

func TestConfigValidate(t *testing.T) {
	if err := validBaseConfig().validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	cases := map[string]func(*config){
		"zero buffer":        func(c *config) { c.bufferSize = 0 },
		"zero batch":         func(c *config) { c.batchSize = 0 },
		"tiny message limit": func(c *config) { c.maxMessageBytes = 8 },
		"zero flush":         func(c *config) { c.flushInterval = 0 },
		"zero replay":        func(c *config) { c.replayInterval = 0 },
		"neg retries":        func(c *config) { c.maxRetries = -1 },
		"zero retention":     func(c *config) { c.retention = 0 },
		"bad url":            func(c *config) { c.chBaseURL = "not a url" },
		"wrong scheme":       func(c *config) { c.chBaseURL = "ftp://x" },
		"spool no max":       func(c *config) { c.spoolDir = "/spool"; c.spoolMaxFiles = 0 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			c := validBaseConfig()
			mut(&c)
			if err := c.validate(); err == nil {
				t.Errorf("expected validation error for %q", name)
			}
		})
	}
}
