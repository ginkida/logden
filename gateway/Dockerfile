# ---- build ----
FROM golang:1.27-alpine@sha256:4c9fe60190a2a3350ddc51de80d0224b8a6698d12bdfc999fee45ea9d6c46dbc AS build
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

# ---- run ----  (static binary in a minimal distroless image)
FROM gcr.io/distroless/static-debian12:nonroot@sha256:d093aa3e30dbadd3efe1310db061a14da60299baff8450a17fe0ccc514a16639
LABEL org.opencontainers.image.title="logden" \
      org.opencontainers.image.source="https://github.com/ginkida/logden" \
      org.opencontainers.image.licenses="MIT"
COPY --from=build /logden /logden
# /spool is owned by nonroot (65532) — an empty named volume inherits this owner
COPY --from=build --chown=65532:65532 /spool /spool
EXPOSE 8080
ENTRYPOINT ["/logden"]
