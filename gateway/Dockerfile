# ---- build ----
FROM golang:1.26-alpine@sha256:f23e8b227fb4493eabe03bede4d5a32d04092da71962f1fb79b5f7d1e6c2a17f AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY *.go ./
ARG VERSION=dev
ARG COMMIT=none
ARG BUILD_DATE=unknown
RUN CGO_ENABLED=0 go build \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.buildDate=${BUILD_DATE}" \
      -o /logden . \
 && mkdir -p /spool

# ---- run ----  (статичный бинарь в минимальном distroless-образе)
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639
LABEL org.opencontainers.image.title="logden" \
      org.opencontainers.image.source="https://github.com/ginkida/logden" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /logden /logden
# /spool владеет nonroot (65532) — пустой named volume унаследует этого владельца
COPY --from=build --chown=65532:65532 /spool /spool
EXPOSE 8080
ENTRYPOINT ["/logden"]
