# Contributing

## Project principles (hard rules)

- **Gateway — Go stdlib only.** No external dependencies in `gateway/go.mod`
  (test-only excepted, if truly unavoidable). This keeps the image ~10 MB and the audit simple.
- **Resource budget.** Target host — ~1 GB RAM. Changes must not noticeably
  increase memory use of the gateway or ClickHouse.
- **Simple contract.** `POST /logs` stays trivial for any language.

## Local development

```bash
make fmt           # gofmt
make vet           # go vet
make lint          # vet + staticcheck
make test          # unit tests with -race
make up            # bring up the stack
LOG_TOKEN=... make smoke
make down
```

Integration tests (need ClickHouse; the module lives in `gateway/`, run from there):
```bash
cd gateway && CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=default \
  go test -tags=integration ./...
```

Memory probe (opt-in, not part of `make test`): drives the gateway into the worst
case its byte caps admit and fails if the peak RSS leaves the container limit.
Re-run it whenever you change `BUFFER_MAX_BYTES`, `MAX_INFLIGHT_BODY_BYTES`,
`MAX_BODY_BYTES` or the parse path:
```bash
cd gateway && LOGDEN_MEM_PROBE=1 GOMEMLIMIT=80MiB go test -run WorstCaseHeap -v
```

## Before a PR

- `gofmt` and `go vet` clean; `make test` green (or `cd gateway && go test -race ./...`).
- Changing the `/logs` contract or the ClickHouse schema → add an entry to [CHANGELOG.md](CHANGELOG.md).
- Changing a memory cap or the parse path → re-run the memory probe above.
- Changing the schema on a live table → ALTER in `clickhouse/migrations.sql`, don't edit `schema.sql`.
- Commits — short and to the point.

CI runs gofmt, vet, staticcheck, unit (race+coverage), integration against a
real ClickHouse, and the image build.
