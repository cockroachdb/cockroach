// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package engineccl

import (
	"errors"
	"io/ioutil"
	"os"
	"unsafe"

	"golang.org/x/net/context"

	// rocksdbccl only exports cgo code.
	_ "github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/rocksdbccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo !darwin LDFLAGS: -Wl,-unresolved-symbols=ignore-all
// #cgo linux LDFLAGS: -lrt
//
// #include <stdlib.h>
// #include "rocksdbccl/db.h"
import "C"

// RocksDBSstFileReader allows iteration over a number of non-overlapping
// sstables exported by `RocksDBSstFileWriter`.
type RocksDBSstFileReader struct {
	// TODO(dan): This currently works by creating a RocksDB instance in a
	// temporary directory that's cleaned up on `Close`. It doesn't appear that
	// we can use an in-memory RocksDB with this, because AddFile doesn't then
	// work with files on disk. This should also work with overlapping files.

	dir     string
	rocksDB *engine.RocksDB
}

// MakeRocksDBSstFileReader creates a RocksDBSstFileReader that uses a scratch
// directory which is cleaned up by `Close`.
func MakeRocksDBSstFileReader() (RocksDBSstFileReader, error) {
	dir, err := ioutil.TempDir("", "RocksDBSstFileReader")
	if err != nil {
		return RocksDBSstFileReader{}, err
	}

	// TODO(dan): I pulled all these magic numbers out of nowhere. Make them
	// less magic.
	cache := engine.NewRocksDBCache(1 << 20)
	rocksDB, err := engine.NewRocksDB(
		roachpb.Attributes{}, dir, cache, 512<<20, engine.DefaultMaxOpenFiles)
	if err != nil {
		return RocksDBSstFileReader{}, err
	}
	return RocksDBSstFileReader{dir, rocksDB}, nil
}

// AddFile links the file at the given path into a database. See the RocksDB
// documentation on `AddFile` for the various restrictions on what can be added.
func (fr *RocksDBSstFileReader) AddFile(path string) error {
	if fr.rocksDB == nil {
		return errors.New("cannot call AddFile on a closed reader")
	}
	return statusToError(C.DBEngineAddFile(fr.rocksDB.RawEngine(), goToCSlice([]byte(path))))
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair.
func (fr *RocksDBSstFileReader) Iterate(
	start, end engine.MVCCKey, f func(engine.MVCCKeyValue) (bool, error),
) error {
	if fr.rocksDB == nil {
		return errors.New("cannot call Iterate on a closed reader")
	}
	return fr.rocksDB.Iterate(start, end, f)
}

// Close finishes the reader.
func (fr *RocksDBSstFileReader) Close() {
	if fr.rocksDB == nil {
		return
	}
	fr.rocksDB.Close()
	fr.rocksDB = nil
	if err := os.RemoveAll(fr.dir); err != nil {
		log.Warningf(context.TODO(), "error removing temp rocksdb directory %q: %s", fr.dir, err)
	}
}

// RocksDBSstFileWriter creates a file suitable for importing with
// RocksDBSstFileReader.
type RocksDBSstFileWriter struct {
	fw *C.DBSstFileWriter
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
}

// MakeRocksDBSstFileWriter creates a new RocksDBSstFileWriter with the default
// configuration.
func MakeRocksDBSstFileWriter() RocksDBSstFileWriter {
	return RocksDBSstFileWriter{C.DBSstFileWriterNew(), 0}
}

// Open creates a file at the given path for output of an sstable.
func (fw *RocksDBSstFileWriter) Open(path string) error {
	if fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	return statusToError(C.DBSstFileWriterOpen(fw.fw, goToCSlice([]byte(path))))
}

// Add puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Open` must have been called. `Close`
// cannot have been called.
func (fw *RocksDBSstFileWriter) Add(kv engine.MVCCKeyValue) error {
	if fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	fw.DataSize += int64(len(kv.Key.Key)) + int64(len(kv.Value))
	return statusToError(C.DBSstFileWriterAdd(fw.fw, goToCKey(kv.Key), goToCSlice(kv.Value)))
}

// Close finishes the writer, flushing any remaining writes to disk. At least
// one kv entry must have been added.
func (fw *RocksDBSstFileWriter) Close() error {
	if fw.fw == nil {
		return errors.New("writer is already closed")
	}
	err := statusToError(C.DBSstFileWriterClose(fw.fw))
	fw.fw = nil
	return err
}

// TODO(dan): The following are all duplicated from storage/engine/rocksdb.go,
// but if you export the ones there and reuse them here, it doesn't work.
//
// `cannot use engine.GoToCKey(kv.Key) (type engine.C.struct___2) as type C.struct___2 in argument to _Cfunc_DBSstFileWriterAdd`

// goToCSlice converts a go byte slice to a DBSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCSlice(b []byte) C.DBSlice {
	if len(b) == 0 {
		return C.DBSlice{data: nil, len: 0}
	}
	return C.DBSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.int(len(b)),
	}
}

func goToCKey(key engine.MVCCKey) C.DBKey {
	return C.DBKey{
		key:       goToCSlice(key.Key),
		wall_time: C.int64_t(key.Timestamp.WallTime),
		logical:   C.int32_t(key.Timestamp.Logical),
	}
}

func cStringToGoString(s C.DBString) string {
	if s.data == nil {
		return ""
	}
	result := C.GoStringN(s.data, s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func statusToError(s C.DBStatus) error {
	if s.data == nil {
		return nil
	}
	return errors.New(cStringToGoString(s))
}
