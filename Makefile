GO ?= go

GOPATH  := $(CURDIR)/_vendor:$(GOPATH)
ROCKSDB := $(CURDIR)/_vendor/rocksdb

CGO_CFLAGS  := "-I$(ROCKSDB)/include"
CGO_LDFLAGS := "-L$(ROCKSDB)"

CGO_FLAGS := CGO_LDFLAGS=$(CGO_LDFLAGS) \
             CGO_CFLAGS=$(CGO_CFLAGS)

all: build test

rocksdb:
	cd $(ROCKSDB); make static_lib

build: goget rocksdb
	$(CGO_FLAGS) $(GO) build

goget:
	$(CGO_FLAGS) $(GO) get ./...

test:
	$(CGO_FLAGS) $(GO) test ./...

testrace:
	$(CGO_FLAGS) $(GO) test -race ./...

coverage:
	$(CGO_FLAGS) $(GO) test -cover ./...

clean:
	$(GO) clean
	cd $(ROCKSDB); make clean
