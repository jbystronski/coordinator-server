# Staege 1: build the GO binary

FROM golang:1.24-alpine AS builder

WORKDIR /app

# Cache go modules first

COPY go.mod go.sum ./
RUN go.mod download

# Copy source files

COPY . .

# Build the binary statically

RUN CGO_ENABLED=0 GOOS=linux go build -o coordinator .

# Stage 2: run

FROM alpine:3.19

WORKDIR /app

# Copy binary from builder stage

COPY --from=builder /app/coordinator .

# Server listens on

EXPOSE 8090

# Run the binary

CMD ["./coordinator"]
