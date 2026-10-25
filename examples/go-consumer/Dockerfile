FROM golang:1.25-alpine
WORKDIR /src
COPY go.mod go.sum ./
COPY pkg ./pkg
COPY examples/go-consumer ./examples/go-consumer
RUN go build -o /bin/consumer ./examples/go-consumer
CMD ["/bin/consumer"]
