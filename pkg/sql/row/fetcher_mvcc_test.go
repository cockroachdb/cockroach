// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/errors"
)

func slurpUserDataKVs(t testing.TB, e storage.Engine) []roachpb.KeyValue {
	t.Helper()

	// Scan meta keys directly from engine. We put this in a retry loop
	// because the application of all of a transactions committed writes
	// is not always synchronous with it committing.
	var kvs []roachpb.KeyValue
	testutils.SucceedsSoon(t, func() error {
		kvs = nil
		it := e.NewMVCCIterator(storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		defer it.Close()
		for it.SeekGE(storage.MVCCKey{Key: keys.UserTableDataMin}); ; it.NextKey() {
			ok, err := it.Valid()
			if err != nil {
				t.Fatal(err)
			}
			if !ok {
				break
			}
			if !it.UnsafeKey().IsValue() {
				return errors.Errorf("found intent key %v", it.UnsafeKey())
			}
			kvs = append(kvs, roachpb.KeyValue{
				Key:   it.Key().Key,
				Value: roachpb.Value{RawBytes: it.Value(), Timestamp: it.UnsafeKey().Timestamp},
			})
		}
		return nil
	})
	return kvs
}
