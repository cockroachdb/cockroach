// Copyright 2017 The Cockroach Authors.
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

package engine

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
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
				Settings: cluster.MakeTestingClusterSettings(),
				Dir:      dir,
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
	rErr, ok := errors.Cause(err).(*RocksDBError)
	if !ok {
		t.Fatalf("unexpected error of cause %T: %s", errors.Cause(err), err)
	}

	for _, test := range []struct {
		err    *RocksDBError
		expMsg string
	}{
		{
			err: rErr,
			// "locks" is redacted because this last part of the message is actually from `strerror` (ANSI-C).
			expMsg: "io error while lock file <redacted> no <redacted> available",
		},
		{
			// A real-world example.
			err: &RocksDBError{
				msg: "Corruption: block checksum mismatch: expected 4187431493, got 3338436330  " +
					"in /home/agent/activerecord-cockroachdb-adapter/cockroach-data/000012.sst " +
					"offset 59661 size 7425",
			},
			expMsg: "corruption block checksum mismatch expected <redacted> got <redacted> in <redacted> offset <redacted> size <redacted>",
		},
		{
			// An example that shows that paths containing dictionary words are still redacted.
			err: &RocksDBError{
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
