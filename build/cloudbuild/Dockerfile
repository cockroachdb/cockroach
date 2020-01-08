FROM cockroachdb/builder:20191218-092712

RUN mkdir -p $GOPATH/src/github.com/cockroachdb/cockroach
WORKDIR $GOPATH/src/github.com/cockroachdb/cockroach

COPY . .

RUN pwd
RUN ls
RUN git init
RUN git submodule update --init --recursive
RUN cd vendor && pwd && ls && git status

RUN make
