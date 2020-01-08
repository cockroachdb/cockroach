FROM cockroachdb/builder:20191218-092712

RUN mkdir -p $GOPATH/src/github.com/cockroachdb/cockroach
WORKDIR $GOPATH/src/github.com/cockroachdb/cockroach

COPY . .

RUN make
