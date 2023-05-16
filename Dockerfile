# Badger Dockerfile
FROM golang:1.19-alpine

LABEL maintainer="Dgraph Labs <contact@dgraph.io>"

WORKDIR /app

RUN apk add --no-cache git

COPY . .

WORKDIR badger

RUN go install

RUN go build -o badger

CMD ["badger"] # Shows the badger version and commands available.
