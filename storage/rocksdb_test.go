package storage

import (
	"bytes"
	"fmt"
	"os"
	"sort"
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

	// Test for correct handling of empty keys, which should produce errors.
	for _, err := range []error{
		engine.put([]byte(""), Value{}),
		engine.put(nil, Value{}),
		func() error {
			_, err := engine.get([]byte(""))
			return err
		}(),
		engine.del(nil),
		func() error {
			_, err := engine.get(nil)
			return err
		}(),
		engine.del(nil),
		engine.del([]byte("")),
	} {
		if err == nil {
			t.Fatalf("illegal handling of empty key")
		}
	}

	// Test for allowed keys, which should go through.
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

func TestRocksDBEngineScan(t *testing.T) {
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
		{[]byte("french"), []byte("Allô?")},
		{[]byte("german"), []byte("hallo")},
		{[]byte("chinese"), []byte("你好")},
	}
	keyMap := map[string][]byte{}
	for _, c := range testCases {
		if err := engine.put(c.key, Value{Bytes: c.value}); err != nil {
			t.Errorf("could not put key %q: %v", c.key, err)
		}
		keyMap[string(c.key)] = c.value
	}
	sortedKeys := make([]string, len(testCases))
	for i, t := range testCases {
		sortedKeys[i] = string(t.key)
	}
	sort.Strings(sortedKeys)

	keyvals, err := engine.scan([]byte("chinese"), []byte("german"), 0)
	if err != nil {
		t.Fatalf("could not run scan: %v", err)
	}
	ensureRangeEqual(t, sortedKeys[1:4], keyMap, keyvals)

	keyvals, err = engine.scan([]byte("chinese"), []byte("german"), 2)
	if err != nil {
		t.Fatalf("could not run scan: %v", err)
	}
	ensureRangeEqual(t, sortedKeys[1:3], keyMap, keyvals)

	// Should return all key/value pairs in lexicographic order.
	// Note that []byte("") is the lowest key possible and is
	// a special case in engine.scan, that's why we test it here.
	startKeys := [][]byte{[]byte("cat"), []byte("")}
	for _, startKey := range startKeys {
		keyvals, err := engine.scan([]byte(startKey), nil, 0)
		if err != nil {
			t.Fatalf("could not run scan: %v", err)
		}
		ensureRangeEqual(t, sortedKeys, keyMap, keyvals)
	}
}

func ensureRangeEqual(t *testing.T, sortedKeys []string, keyMap map[string][]byte, keyvals []KeyValue) {
	if len(keyvals) != len(sortedKeys) {
		t.Errorf("length mismatch. expected %d, got %d", len(sortedKeys), len(keyvals))
	}
	t.Log("---")
	for i, kv := range keyvals {
		t.Logf("index: %d\tk: %q\tv: %q\n", i, kv.Key, kv.Value.Bytes)
		if sortedKeys[i] != string(kv.Key) {
			t.Errorf("key mismatch at index %d: expected %q, got %q", i, sortedKeys[i], kv.Key)
		}
		if !bytes.Equal(keyMap[sortedKeys[i]], kv.Value.Bytes) {
			t.Errorf("value mismatch at index %d: expected %q, got %q", i, keyMap[sortedKeys[i]], kv.Value.Bytes)
		}
	}
}
