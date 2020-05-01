// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
)

// TestRocksDBErrorSafeMessage verifies that RocksDB errors have a chance of
// being reported safely.
func TestRocksDBErrorSafeMessage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, dirCleanup := testutils.TempDir(t)
	defer dirCleanup()

	open := func() (*RocksDB, error) {
		return NewRocksDB(
			RocksDBConfig{
				StorageConfig: base.StorageConfig{
					Settings: cluster.MakeTestingClusterSettings(),
					Dir:      dir,
				},
			},
			RocksDBCache{},
		)
	}

	// Provoke a RocksDB error by opening two instances for the same directory.
	r1, err := open()
	if err != nil {
		t.Fatal(err)
	}
	defer r1.Close()
	r2, err := open()
	if err == nil {
		defer r2.Close()
		t.Fatal("expected error")
	}
	var rErr *Error
	if !errors.As(err, &rErr) {
		t.Fatalf("unexpected error: %+v", err)
	}

	for _, test := range []struct {
		err    *Error
		expMsg string
	}{
		{
			err: rErr,
			// "locks" is redacted because this last part of the message is actually from `strerror` (ANSI-C).
			expMsg: "io error lock <redacted> <redacted> no <redacted> available",
		},
		{
			// A real-world example.
			err: &Error{
				msg: "Corruption: block checksum mismatch: expected 4187431493, got 3338436330  " +
					"in /home/agent/activerecord-cockroachdb-adapter/cockroach-data/000012.sst " +
					"offset 59661 size 7425",
			},
			expMsg: "corruption block checksum mismatch expected <redacted> got <redacted> in <redacted> offset <redacted> size <redacted>",
		},
		{
			// An example that shows that paths containing dictionary words are still redacted.
			err: &Error{
				msg: "Corruption: block checksum mismatch in /block C:\\checksum /mismatch/corruption/Corruption C:\\checksum\\corruption",
			},
			expMsg: "corruption block checksum mismatch in <redacted> <redacted> <redacted> <redacted>",
		},
	} {
		if act := test.err.SafeMessage(); act != test.expMsg {
			t.Errorf("expected %q, got %q\nfrom original error %v", test.expMsg, act, test.err)
		}
	}
}
