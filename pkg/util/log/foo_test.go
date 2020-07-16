// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"fmt"
	"os"
	"testing"
)

func TestFoo(t *testing.T) {
	defer Scope(t).Close(t)
	t.Log("i'm t.Logging")
	fmt.Println("i'm on stdout")
	fmt.Fprintln(os.Stderr, "i'm on stderr")
	panic("i panicked somewhere else")
}
