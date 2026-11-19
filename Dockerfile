# One image, four binaries (api, worker, migrate, mockprovider).
# docker-compose picks the binary per service with `command`.

FROM golang:1.27-alpine AS build
WORKDIR /src
# Download modules in their own layer so code changes do not re-download them.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.22
# wget (busybox) is used by the compose health checks.
RUN adduser -D -u 10001 app
COPY --from=build /out/ /app/
USER app
EXPOSE 8080 9090 9091
CMD ["/app/api"]
