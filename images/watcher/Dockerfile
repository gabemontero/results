# Docker configuration to create Tekton Results watcher image
FROM  quay.io/rh_ee_tsebasti/golang-alpine-1.22.4:mod-threshold-3s AS builder

WORKDIR /opt/app-root/src

COPY go.mod go.mod
COPY cmd/ cmd/
COPY proto/ proto/
COPY pkg/ pkg/
RUN go mod tidy
RUN go mod vendor

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o watcher cmd/watcher/main.go

FROM registry.access.redhat.com/ubi9-minimal:9.1.0

COPY --from=builder /opt/app-root/src/watcher /usr/local/bin/watcher

USER 65532:65532

ENTRYPOINT ["watcher"]
