# syntax = docker/dockerfile:1.4

ARG GO_VERSION

FROM golang:${GO_VERSION}-alpine AS build-go
WORKDIR /src
ENV CGO_ENABLED=0
COPY --link go.* ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY --link . .

FROM build-go AS build-worker
ARG VERSION_PATH
ARG VERSION_LONG
ARG VERSION_SHORT
ARG VERSION_GIT_HASH
RUN --mount=type=cache,target=/go/pkg/mod \
	--mount=type=cache,target=/root/.cache/go-build \
	go build \
	-trimpath \
	-ldflags="-X '${VERSION_PATH}.Long=${VERSION_LONG}' -X '${VERSION_PATH}.Short=${VERSION_SHORT}' -X '${VERSION_PATH}.GitCommit=${VERSION_GIT_HASH}'" \
	-o /out/cva-enduro-worker \
	./cmd/worker

FROM alpine:3.18.2 AS base
ARG USER_ID=1000
ARG GROUP_ID=1000
RUN addgroup -g ${GROUP_ID} -S enduro
RUN adduser -u ${USER_ID} -S -D enduro enduro
USER enduro
RUN mkdir /home/enduro/reports

FROM base AS enduro-worker
COPY --from=build-worker --link /out/cva-enduro-worker /home/enduro/bin/cva-enduro-worker
CMD ["/home/enduro/bin/cva-enduro-worker"]