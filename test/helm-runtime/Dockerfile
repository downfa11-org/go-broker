FROM golang:1.26.6 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go test -c -o /smoke.test ./test/helm-runtime

FROM alpine:3.20
COPY --from=builder /smoke.test /smoke.test
USER 1000:1000
ENTRYPOINT ["/smoke.test", "-test.v", "-test.timeout=3m"]
