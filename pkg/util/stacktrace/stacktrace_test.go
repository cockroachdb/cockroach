// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package stacktrace_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/stacktrace"
	"github.com/kr/pretty"
)

func genStack2() *stacktrace.StackTrace {
	return stacktrace.NewStackTrace(0)
}

func genStack1() *stacktrace.StackTrace {
	return genStack2()
}

func TestStackTrace(t *testing.T) {
	st := genStack1()

	t.Logf("Stack trace:\n%s", stacktrace.PrintStackTrace(st))

	encoded, err := stacktrace.EncodeStackTrace(st)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("encoded:\n%s", encoded)
	if !strings.Contains(encoded, `"function":"genStack1"`) ||
		!strings.Contains(encoded, `"function":"genStack2"`) {
		t.Fatalf("function genStack not in call stack:\n%s", encoded)
	}

	st2, err := stacktrace.DecodeStackTrace(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(st, st2) {
		t.Fatalf("stack traces not identical: %v", pretty.Diff(st, st2))
	}
}
