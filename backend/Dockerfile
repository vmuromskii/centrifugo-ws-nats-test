FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /backend main.go

FROM gcr.io/distroless/static:nonroot

COPY --from=builder /backend /backend

EXPOSE 8080 8081
CMD ["/backend"]