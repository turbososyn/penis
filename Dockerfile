FROM golang:1.19

WORKDIR /opt/practice-4

COPY . .

RUN chmod +x ./entry.sh

RUN go build -o ./bin/server ./cmd/server
RUN go build -o ./bin/db ./cmd/db
RUN go build -o ./bin/lb ./cmd/lb
RUN go build -o ./bin/client ./cmd/client

ENTRYPOINT ["/opt/practice-4/entry.sh"]