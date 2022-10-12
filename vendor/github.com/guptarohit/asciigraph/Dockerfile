FROM golang:1.17-alpine AS builder
WORKDIR /app
COPY cmd ./cmd
COPY go.mod ./
COPY *.go ./
RUN GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o /app/asciigraph ./cmd/asciigraph/main.go

FROM scratch
COPY --from=builder /app/asciigraph /asciigraph
ENTRYPOINT ["/asciigraph"]
