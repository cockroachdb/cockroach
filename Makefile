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

build: rocksdb
	$(CGO_FLAGS) $(GO) build

test:
	$(CGO_FLAGS) $(GO) test ./...

clean:
	$(GO) clean
	cd $(ROCKSDB); make clean
