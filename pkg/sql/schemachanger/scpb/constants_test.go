// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
)

func ExampleIterateStatuses() {
	err := IterateStatuses(func(s Status) error {
		fmt.Println(s)
		return errors.New("boom")
	})
	fmt.Println(err)
	i := 0
	fmt.Println(IterateStatuses(func(s Status) error {
		if i++; i <= 3 {
			fmt.Println(s)
			return nil
		}
		return iterutil.StopIteration()
	}))
	fmt.Println(IterateStatuses(func(s Status) error {
		fmt.Println(s)
		return nil
	}))
	// Output:
	// UNKNOWN
	// boom
	// UNKNOWN
	// ABSENT
	// PUBLIC
	// <nil>
	// UNKNOWN
	// ABSENT
	// PUBLIC
	// TRANSIENT
	// TXN_DROPPED
	// DROPPED
	// WRITE_ONLY
	// DELETE_ONLY
	// VALIDATED
	// MERGED
	// MERGE_ONLY
	// BACKFILLED
	// BACKFILL_ONLY
	// <nil>
}
