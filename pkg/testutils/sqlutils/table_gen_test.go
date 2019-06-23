// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlutils

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestIntToEnglish(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		val int
		exp string
	}{
		{0, "zero"},
		{1, "one"},
		{2, "two"},
		{3, "three"},
		{456, "four-five-six"},
		{70, "seven-zero"},
		{108, "one-zero-eight"},
		{9901, "nine-nine-zero-one"},
	}
	for _, c := range testCases {
		if res := IntToEnglish(c.val); res != c.exp {
			t.Errorf("expected %s, got %s", c.exp, res)
		}
	}
}

func TestGenValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf bytes.Buffer
	genValues(&buf, 7, 11, ToRowFn(RowIdxFn, RowModuloFn(3), RowEnglishFn), false /* shouldPrint */)
	expected := `(7:::INT8,1:::INT8,'seven':::STRING),(8:::INT8,2:::INT8,'eight':::STRING),(9:::INT8,0:::INT8,'nine':::STRING),(10:::INT8,1:::INT8,'one-zero':::STRING),(11:::INT8,2:::INT8,'one-one':::STRING)`
	if buf.String() != expected {
		t.Errorf("expected '%s', got '%s'", expected, buf.String())
	}
}
