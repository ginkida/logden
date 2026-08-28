# Contributing

## Project principles (hard rules)

- **Gateway — Go stdlib only.** No external dependencies in `gateway/go.mod`
  (test-only excepted, if truly unavoidable). This keeps the image ~10 MB and the audit simple.
- **Clients — zero dependencies too.** No npm or pip packages, and their tests use
  the stdlib runners (`go test`, `node --test`, `unittest`). A client that needs an
  install step is a client nobody adds to a running service.
- **Resource budget.** Target host — ~1 GB RAM. Changes must not noticeably
  increase memory use of the gateway or ClickHouse.
- **Simple contract.** `POST /logs` stays trivial for any language.
- **The repository is in English** — docs, comments, commit messages.
- **Explain the WHY of anything non-obvious**, in a tight comment naming the failure
  mode it prevents. Most of the invariants here look like arbitrary choices until you
  know what broke; the comment is what keeps the next change from undoing them.

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

Integration tests (need ClickHouse; the module lives in `gateway/`, run from there —
the schema is read as `../clickhouse/schema.sql`). Without `CLICKHOUSE_URL` they
`t.Skip` silently, which is why CI asserts a `TestIntegration*` actually ran:
```bash
cd gateway && CLICKHOUSE_URL=http://localhost:8123 CLICKHOUSE_USER=default \
  CLICKHOUSE_PASSWORD=ci go test -tags=integration ./...
```

The client suites are separate modules/runners and are **not** in `make test`:
```bash
cd clients/go && go test ./...
node --test 'clients/node/**/*.test.mjs'
cd clients/python && python3 -m unittest
```

Fuzz targets over the request-parsing surface (`gateway/fuzz_test.go`):
`FuzzParseBatch`, `FuzzNormalizeTimestamp`, `FuzzNormalizeContext`,
`FuzzMessageTruncation`. `make test` runs their seed corpora; extend a run by hand
after touching the parse or validation path:
```bash
cd gateway && go test -run=Fuzz -fuzz=FuzzParseBatch -fuzztime=60s
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
- Changing a metric name → don't. `deploy/alerts.yml` keys on these names; a rename
  silently breaks the alerts that watch for log loss.
- Changing one client → change all three, or say in the PR why not. They are meant to
  read as one client in three languages (see [clients/README.md](clients/README.md)), and
  the capability matrix there is where an asymmetry has to be visible. Run all three suites.
- Commits — short and to the point.

## What CI enforces

`.github/workflows/ci.yml`, four jobs; everything below is a merge gate.

- **gateway** — `gofmt -l`, `go vet`, staticcheck (pinned to `v0.7.0`, never
  `@latest`, and `Makefile`'s `lint` target must stay on the same version), unit tests
  with `-race -covermode=atomic` plus a coverage summary, `go build`, the Go client
  suite, `gofmt`/`vet`/`build` for `tools/loadtest`, and `govulncheck` (pinned) across
  all three modules — the toolchain and stdlib are this repo's entire dependency
  surface, so that is the only dependency advisory it can have.
- **integration** — against a real ClickHouse service container, with a guard that
  fails the build if no `TestIntegration*` ran or any of them skipped (an all-skipped
  package reports `ok`, which used to make an unreachable database look green). Keep
  the `TestIntegration` prefix on new integration tests.
- **docker** — builds the gateway image, then `docker compose config -q` on the real
  compose file and on the override example, so a typo in the file operators deploy
  fails here rather than on the VPS.
- **lint-extra** — hadolint (fails at `warning`), shellcheck, yamllint `--strict`,
  the Node and Python client suites (each asserting a non-zero test count, for the
  same silent-pass reason as the integration guard), `php -l` on the Laravel example,
  and `promtool check rules` + `check config` on `deploy/alerts.yml` and
  `deploy/prometheus.yml.example`.

Releases are cut only from a `v*` git tag (`release.yml`), which calls this workflow
first, so the published image is never an untested build.
