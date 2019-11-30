FROM golang:1.13.4-alpine as builder

RUN apk update && apk add --no-cache git ca-certificates && update-ca-certificates

RUN adduser -D -g '' app

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags="-w -s" -o main .

### Create new image from scratch

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/passwd
COPY --from=builder /app/main /app/main

USER app

CMD ["/app/main"]