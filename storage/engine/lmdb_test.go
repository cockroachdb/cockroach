// Copyright 2016 The Cockroach Authors.
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
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package engine

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestLMDBEngine(t *testing.T) {
	defer leaktest.AfterTest(t)()
	path := util.CreateTempDir(t, ".lmdbtest")
	defer func() {
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
	}()

	l := NewLMDB(10485760, path)
	if err := l.Open(); err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	k1 := MVCCKeyValue{
		Key: MVCCKey{
			Key:       roachpb.Key("test"),
			Timestamp: roachpb.ZeroTimestamp.Add(5, 6),
		},
		Value: []byte("test"),
	}

	if v, err := l.Get(k1.Key); err != nil || v != nil {
		t.Fatalf("unexpected error or nonempty slice: %v, %q", err, v)
	}
	var keysSeen []string
	appendSeen := func(kv MVCCKeyValue) (bool, error) {
		keysSeen = append(keysSeen, fmt.Sprintf("%s@%s", kv.Key.Key, kv.Key.Timestamp))
		return false, nil
	}
	keyMin := MakeMVCCMetadataKey(roachpb.KeyMin)
	expKeysSeen := []string{`"boo"@0.000000001,2`, `"test"@0.000000000,0`, `"test"@0.000000005,6`}

	for _, test := range []struct {
		make  func() Engine
		close func(Engine)
	}{
		{func() Engine { return l }, func(Engine) {}},
		{func() Engine { return l.NewBatch() }, func(e Engine) { e.Close() }},
	} {
		func() {
			eng := test.make()
			defer test.close(eng)
			if err := eng.Put(k1.Key, k1.Value); err != nil {
				t.Fatal(err)
			}

			if v, err := eng.Get(k1.Key); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(v, k1.Value) {
				t.Fatalf("expected %q, got %q", k1.Value, v)
			}

			k2 := k1
			k2.Key.Key = roachpb.Key("boo")
			k2.Key.Timestamp = roachpb.ZeroTimestamp.Add(1, 2)
			k2.Value = []byte("foo")

			if err := eng.Put(k2.Key, k2.Value); err != nil {
				t.Fatal(err)
			}

			m1 := MVCCKeyValue{
				Key: MVCCKey{
					Key:       roachpb.Key("test"),
					Timestamp: roachpb.ZeroTimestamp,
				},
				Value: []byte("goo"),
			}
			if err := eng.Put(m1.Key, m1.Value); err != nil {
				t.Fatal(err)
			}

			keysSeen = keysSeen[:0]
			if err := eng.Iterate(keyMin, MVCCKeyMax, appendSeen); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expKeysSeen, keysSeen) {
				t.Fatalf("expected:\n%v,\ngot:\n%v", expKeysSeen, keysSeen)
			}
		}()
	}

	{
		snap := l.NewSnapshot()
		batch := l.NewBatch()
		for _, eng := range []Engine{snap, batch} {
			keysSeen = keysSeen[:0]
			if err := eng.Iterate(keyMin, MVCCKeyMax, appendSeen); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(expKeysSeen, keysSeen) {
				t.Fatalf("expected:\n%v,\ngot:\n%v", expKeysSeen, keysSeen)
			}
		}
		batch.Close()
		snap.Close()
	}
	{
		iter := l.NewIterator(nil)
		iter.Seek(keyMin)
		defer iter.Close()
		keysSeen = keysSeen[:0]
		for ; iter.Valid(); iter.Next() {
			_, _ = appendSeen(MVCCKeyValue{
				Key:   iter.Key(),
				Value: iter.Value(),
			})
		}
		if iter.Error() != nil {
			t.Fatal(iter.Error())
		}

		if !reflect.DeepEqual(expKeysSeen, keysSeen) {
			t.Fatalf("expected:\n%v,\ngot:\n%v", expKeysSeen, keysSeen)
		}
	}
	{
		iter := l.NewIterator(nil)
		iter.SeekReverse(keyMin) // convention for seek-to-last
		defer iter.Close()
		keysSeen = keysSeen[:0]
		for ; iter.Valid(); iter.Prev() {
			_, _ = appendSeen(MVCCKeyValue{
				Key:   iter.Key(),
				Value: iter.Value(),
			})
		}
		if iter.Error() != nil {
			t.Fatal(iter.Error())
		}
		revSeen := make([]string, len(keysSeen))
		for i := 0; i < len(revSeen); i++ {
			revSeen[i] = keysSeen[len(keysSeen)-1-i]
		}

		if !reflect.DeepEqual(expKeysSeen, revSeen) {
			t.Fatalf("expected:\n%v,\ngot:\n%v", expKeysSeen, revSeen)
		}
	}
}
