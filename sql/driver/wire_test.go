// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package driver

import (
	"testing"

	"github.com/cockroachdb/cockroach/util/leaktest"
)

func dBool(v bool) Datum {
	return Datum{BoolVal: &v}
}

func dInt(v int64) Datum {
	return Datum{IntVal: &v}
}

func dFloat(v float64) Datum {
	return Datum{FloatVal: &v}
}

func dBytes(v []byte) Datum {
	return Datum{BytesVal: v}
}

func dString(v string) Datum {
	return Datum{StringVal: &v}
}

func TestDatumString(t *testing.T) {
	defer leaktest.AfterTest(t)

	testData := []struct {
		datum    Datum
		expected string
	}{
		{Datum{}, "NULL"},
		{dBool(false), "false"},
		{dBool(true), "true"},
		{dInt(-2), "-2"},
		{dFloat(4.5), "4.5"},
		{dBytes([]byte("6")), "6"},
		{dString("hello"), "hello"},
	}
	for i, d := range testData {
		s := d.datum.String()
		if d.expected != s {
			t.Errorf("%d: expected %s, but got %s", i, d.expected, s)
		}
	}
}
