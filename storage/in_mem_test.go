// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Andrew Bonventre (andybons@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"runtime"
	"testing"
)

func TestInMemEnginePutGetDelete(t *testing.T) {
	engine := NewInMem(Attributes{}, 1<<20)
	testCases := []struct {
		key, value []byte
	}{
		{[]byte("dog"), []byte("woof")},
		{[]byte("cat"), []byte("meow")},
		{[]byte("server"), []byte("42")},
		{[]byte("empty1"), nil},
		{[]byte("empty2"), []byte("")},
	}
	for _, c := range testCases {
		val, err := engine.get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if len(val.Bytes) != 0 {
			t.Errorf("expected key %q value.Bytes to be nil: got %+v", c.key, val)
		}
		err = engine.put(c.key, Value{Bytes: c.value})
		if err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
		val, err = engine.get(c.key)
		if err != nil {
			t.Errorf("get: expected no error, but got %s", err)
		}
		if !bytes.Equal(val.Bytes, c.value) {
			t.Errorf("expected key value %s to be %+v: got %+v", c.key, c.value, val)
		}
		err = engine.del(c.key)
		if err != nil {
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

func TestInMemCapacity(t *testing.T) {
	engine := NewInMem(Attributes{}, 1<<20)
	c, err := engine.capacity()
	if err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Capacity != 1<<20 {
		t.Errorf("expected capacity to be %d, got %d", 1<<20, c.Capacity)
	}
	if c.Available != 1<<20 {
		t.Errorf("expected available to be %d, got %d", 1<<20, c.Available)
	}

	bytes := []byte("0123456789")

	// Add a key.
	err = engine.put(Key(bytes), Value{Bytes: bytes})
	if err != nil {
		t.Errorf("put: expected no error, but got %s", err)
	}
	if c, err = engine.capacity(); err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Capacity != 1<<20 {
		t.Errorf("expected capacity to be %d, got %d", 1<<20, c.Capacity)
	}
	if !(c.Available < 1<<20) {
		t.Errorf("expected available to be < %d, got %d", 1<<20, c.Available)
	}

	// Remove key.
	err = engine.del(Key(bytes))
	if err != nil {
		t.Errorf("delete: expected no error, but got %s", err)
	}
	if c, err = engine.capacity(); err != nil {
		t.Errorf("unexpected error fetching capacity: %v", err)
	}
	if c.Available != 1<<20 {
		t.Errorf("expected available to be %d, got %d", 1<<20, c.Available)
	}
}

func TestInMemOverCapacity(t *testing.T) {
	engine := NewInMem(Attributes{}, 120 /* 120 bytes only -- enough for one node, not two */)
	bytes := []byte("0123456789")
	var err error
	if err = engine.put(Key("1"), Value{Bytes: bytes}); err != nil {
		t.Errorf("put: expected no error, but got %s", err)
	}
	if err = engine.put(Key("2"), Value{Bytes: bytes}); err == nil {
		t.Error("put: expected error, but got none")
	}
}

func TestInMemIncrement(t *testing.T) {
	engine := NewInMem(Attributes{}, 1<<20)
	// Start with increment of an empty key.
	val, err := increment(engine, Key("a"), 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if val != 1 {
		t.Errorf("expected increment to be %d; got %d", 1, val)
	}
	// Increment same key by 1.
	if val, err = increment(engine, Key("a"), 1, 0); err != nil {
		t.Fatal(err)
	}
	if val != 2 {
		t.Errorf("expected increment to be %d; got %d", 2, val)
	}
	// Increment same key by 2.
	if val, err = increment(engine, Key("a"), 2, 0); err != nil {
		t.Fatal(err)
	}
	if val != 4 {
		t.Errorf("expected increment to be %d; got %d", 4, val)
	}
	// Decrement same key by -1.
	if val, err = increment(engine, Key("a"), -1, 0); err != nil {
		t.Fatal(err)
	}
	if val != 3 {
		t.Errorf("expected increment to be %d; got %d", 3, val)
	}
	// Increment same key by max int64 value to cause overflow; should return error.
	if val, err = increment(engine, Key("a"), math.MaxInt64, 0); err == nil {
		t.Error("expected an overflow error")
	}
	if val, err = increment(engine, Key("a"), 0, 0); err != nil {
		t.Fatal(err)
	}
	if val != 3 {
		t.Errorf("expected increment to be %d; got %d", 3, val)
	}
}

func verifyScan(start, end Key, max int64, expKeys []Key, engine Engine, t *testing.T) {
	kvs, err := engine.scan(start, end, max)
	if err != nil {
		t.Errorf("scan %q-%q: expected no error, but got %s", string(start), string(end), err)
	}
	if len(kvs) != len(expKeys) {
		t.Errorf("scan %q-%q: expected scanned keys mismatch %d != %d: %v",
			start, end, len(kvs), len(expKeys), kvs)
	}
	for i, kv := range kvs {
		if !bytes.Equal(kv.Key, expKeys[i]) {
			t.Errorf("scan %q-%q: expected keys equal %q != %q", string(start), string(end),
				string(kv.Key), string(expKeys[i]))
		}
	}
}

func TestInMemScan(t *testing.T) {
	engine := NewInMem(Attributes{}, 1<<20)
	keys := []Key{
		Key("a"),
		Key("aa"),
		Key("aaa"),
		Key("ab"),
		Key("abc"),
		KeyMax,
	}

	// Add keys to store in random order (make sure they sort!).
	order := rand.Perm(len(keys))
	for idx := range order {
		if err := engine.put(keys[idx], Value{Bytes: []byte("value")}); err != nil {
			t.Errorf("put: expected no error, but got %s", err)
		}
	}

	// Scan all keys (non-inclusive of final key).
	verifyScan(KeyMin, KeyMax, 10, keys[0:5], engine, t)
	verifyScan(Key("a"), KeyMax, 10, keys[0:5], engine, t)

	// Scan sub range.
	verifyScan(Key("aab"), Key("abcc"), 10, keys[3:5], engine, t)
	verifyScan(Key("aa0"), Key("abcc"), 10, keys[2:5], engine, t)

	// Scan with max values.
	verifyScan(KeyMin, KeyMax, 3, keys[0:3], engine, t)
	verifyScan(Key("a0"), KeyMax, 3, keys[1:4], engine, t)

	// Scan with max value 0 gets all values.
	verifyScan(KeyMin, KeyMax, 0, keys[0:5], engine, t)
}

func BenchmarkCapacity(b *testing.B) {
	engine := NewInMem(Attributes{}, 1<<30)
	bytes := []byte("0123456789")
	for i := 0; i < b.N; i++ {
		if err := engine.put(Key(fmt.Sprintf("%d", i)), Value{Bytes: bytes}); err != nil {
			b.Fatalf("put: expected no error, but got %s", err)
		}
		if i%10000 == 0 {
			c, err := engine.capacity()
			if err != nil {
				b.Errorf("unexpected error fetching capacity: %v", err)
			}
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			fmt.Printf("bytes in use: engine=%d process=%d\n", c.Capacity-c.Available, m.Alloc)
		}
	}
}
