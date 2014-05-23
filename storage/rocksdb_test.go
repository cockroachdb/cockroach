package storage

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestRocksDBEnginePutGetDelete(t *testing.T) {
	loc := fmt.Sprintf("%s/data_%d", os.TempDir(), time.Now().UnixNano())
	engine, err := NewRocksDB(SSD, loc)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(t *testing.T) {
		engine.close()
		if err := engine.destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)
	testCases := []struct {
		key, value []byte
	}{
		{[]byte("dog"), []byte("woof")},
		{[]byte("cat"), []byte("meow")},
		{[]byte("server"), []byte("42")},
	}
	for _, c := range testCases {
		val, err := engine.get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if len(val.Bytes) != 0 {
			t.Errorf("expected key %q value.Bytes to be nil: got %+v", c.key, val)
		}
		if err := engine.put(c.key, Value{Bytes: c.value}); err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
		val, err = engine.get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if !bytes.Equal(val.Bytes, c.value) {
			t.Errorf("expected key value %s to be %+v: got %+v", c.key, c.value, val)
		}
		if err := engine.del(c.key); err != nil {
			t.Errorf("delete: expected no error, but got %s", err)
		}
		val, err = engine.get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if len(val.Bytes) != 0 {
			t.Errorf("expected key %s value.Bytes to be nil: got %+v", c.key, val)
		}
	}
}
