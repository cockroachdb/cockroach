// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package echotest

import (
	"testing"

	"github.com/cockroachdb/datadriven"
)

// Require checks that the string matches what is found in the file located at
// the provided path. The file must follow the datadriven format:
//
// echo
// ----
// <output of exp>
//
// The contents of the file can be updated automatically using datadriven's
// -rewrite flag.
func Require(t *testing.T, act, path string) {
	var ran bool
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd != "echo" {
			return "only 'echo' is supported"
		}
		ran = true
		return act
	})
	if !ran {
		// Guard against a possible error in which the file is created, then datadriven
		// is invoked with -rewrite to seed it (which it does not do, since there is
		// no directive in the file), and then also the tests pass despite not checking
		// anything.
		t.Errorf("no tests run for %s, is the file empty?", path)
	}
}
