// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvpb_test

import (
	"os"
	"testing"

	// Import log so that the redaction hooks are set up correctly in the testing.
	_ "github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
