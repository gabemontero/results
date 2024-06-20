# Docker configuration to build Tekton Results apiserver image
#FROM registry.access.redhat.com/ubi9/go-toolset:1.19 AS builder
FROM  quay.io/rh_ee_tsebasti/golang-alpine-1.22.4:mod-threshold-3s AS builder

WORKDIR /opt/app-root/src

COPY go.mod go.mod
COPY go.sum go.sum
COPY vendor/ vendor/
COPY cmd/ cmd/
COPY proto/ proto/
COPY pkg/ pkg/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -o api cmd/api/main.go

FROM registry.access.redhat.com/ubi9-minimal:9.1.0
COPY --from=builder /opt/app-root/src/api /usr/local/bin/api
USER 65532:65532

ENTRYPOINT ["api"]
