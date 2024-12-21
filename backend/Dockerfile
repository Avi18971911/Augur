# Choose the base image
FROM golang:1.22.2 as builder

# Set work directory
WORKDIR /app

# Copy the local package files to the container's workspace.
ADD . /app

# Build argument to specify which main to use
ARG MAIN_DIR

# Compile the binary, we disable cgo to create a statically linked binary.
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o app ./cmd/$MAIN_DIR

# Set execute permissions
RUN chmod +x ./app

# Use a small image
FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/app .
CMD ["./app"]