GO ?= go

GOPATH  := $(CURDIR)/_vendor:$(GOPATH)
ROCKSDB := $(CURDIR)/_vendor/rocksdb

CGO_CFLAGS  := "-I$(ROCKSDB)/include"
CGO_LDFLAGS := "-L$(ROCKSDB)"

CGO_FLAGS := CGO_LDFLAGS=$(CGO_LDFLAGS) \
             CGO_CFLAGS=$(CGO_CFLAGS)

PKG := "./..."
TESTS := ".*"

all: build test

rocksdb:
	cd $(ROCKSDB); make static_lib

build: rocksdb
	$(CGO_FLAGS) $(GO) build

goget:
	$(CGO_FLAGS) $(GO) get ./...

test:
	$(CGO_FLAGS) $(GO) test -run $(TESTS) $(PKG)

testrace:
	$(CGO_FLAGS) $(GO) test -race -run $(TESTS) $(PKG)

coverage:
	$(CGO_FLAGS) $(GO) test -cover -run $(TESTS) $(PKG)

clean:
	$(GO) clean
	cd $(ROCKSDB); make clean
