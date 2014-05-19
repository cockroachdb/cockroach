GO ?= go

GOPATH  := $(CURDIR)/_vendor:$(GOPATH)
ROCKSDB := $(CURDIR)/_vendor/rocksdb

CGO_CFLAGS  := "-I$(ROCKSDB)/include"
CGO_LDFLAGS := "-L$(ROCKSDB)"

DYLD_LIBRARY_PATH := $(ROCKSDB)

BUILD_FLAGS := CGO_LDFLAGS=$(CGO_LDFLAGS) \
	             CGO_CFLAGS=$(CGO_CFLAGS)
RUN_FLAGS   := $(BUILD_FLAGS) \
	             DYLD_LIBRARY_PATH=$(DYLD_LIBRARY_PATH)

all: build test

rocksdb:
	cd $(ROCKSDB); make shared_lib

build: rocksdb
	$(BUILD_FLAGS) $(GO) build

test:
	$(RUN_FLAGS) $(GO) test ./...

clean:
	$(GO) clean
	cd $(ROCKSDB); make clean
