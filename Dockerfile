FROM golang:1.21 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /mockserver ./cmd/mockserver

FROM scratch
COPY --from=build /mockserver /mockserver
ENTRYPOINT ["/mockserver", "--config", "/etc/chaosserve/run.yaml"]
