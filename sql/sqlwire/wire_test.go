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

package sqlwire

import "testing"

func dBool(v bool) Datum {
	return Datum{BoolVal: &v}
}

func dInt(v int64) Datum {
	return Datum{IntVal: &v}
}

func dUint(v uint64) Datum {
	return Datum{UintVal: &v}
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

func TestDatumIsNull(t *testing.T) {
	testData := []struct {
		datum    Datum
		expected bool
	}{
		{Datum{}, true},
		{dBool(false), false},
		{dInt(0), false},
		{dUint(0), false},
		{dFloat(0), false},
		{dBytes([]byte{}), false},
		{dString(""), false},
	}
	for i, d := range testData {
		isNull := d.datum.IsNull()
		if d.expected != isNull {
			t.Errorf("%d: expected %v, but got %v: %s", i, d.expected, isNull, d.datum)
		}
	}
}

func TestDatumToInt(t *testing.T) {
	testData := []struct {
		datum    Datum
		expected int64
	}{
		{Datum{}, 0},
		{dBool(false), 0},
		{dBool(true), 1},
		{dInt(2), 2},
		{dUint(3), 3},
		{dFloat(4.5), 4},
		{dBytes([]byte("6")), 6},
		{dString("0xa"), 10},
		{dString("077"), 63},
	}
	for i, d := range testData {
		v, err := d.datum.ToInt()
		if err != nil {
			t.Errorf("%d: expected success, but found %v: %s", i, err, d.datum)
			continue
		}
		if d.expected != *v.IntVal {
			t.Errorf("%d: expected %d, but got %s: %s", i, d.expected, v, d.datum)
		}
	}
}

func TestDatumToUint(t *testing.T) {
	testData := []struct {
		datum    Datum
		expected uint64
	}{
		{Datum{}, 0},
		{dBool(false), 0},
		{dBool(true), 1},
		{dInt(-2), 18446744073709551614},
		{dUint(3), 3},
		{dFloat(4.5), 4},
		{dBytes([]byte("6")), 6},
		{dString("0xa"), 10},
		{dString("077"), 63},
	}
	for i, d := range testData {
		v, err := d.datum.ToUint()
		if err != nil {
			t.Errorf("%d: expected success, but found %v: %s", i, err, d.datum)
			continue
		}
		if d.expected != *v.UintVal {
			t.Errorf("%d: expected %d, but got %s: %s", i, d.expected, v, d.datum)
		}
	}
}

func TestDatumToFloat(t *testing.T) {
	testData := []struct {
		datum    Datum
		expected float64
	}{
		{Datum{}, 0},
		{dBool(false), 0},
		{dBool(true), 1},
		{dInt(-2), -2},
		{dUint(3), 3},
		{dFloat(4.5), 4.5},
		{dBytes([]byte("6")), 6},
		{dString("8.9"), 8.9},
		{dString("10e2"), 1000},
	}
	for i, d := range testData {
		v, err := d.datum.ToFloat()
		if err != nil {
			t.Errorf("%d: expected success, but found %v: %s", i, err, d.datum)
			continue
		}
		if d.expected != *v.FloatVal {
			t.Errorf("%d: expected %g, but got %s: %s", i, d.expected, v, d.datum)
		}
	}
}

func TestDatumToString(t *testing.T) {
	testData := []struct {
		datum    Datum
		expected string
	}{
		{Datum{}, "NULL"},
		{dBool(false), "false"},
		{dBool(true), "true"},
		{dInt(-2), "-2"},
		{dUint(3), "3"},
		{dFloat(4.5), "4.5"},
		{dBytes([]byte("6")), "6"},
		{dString("hello"), "hello"},
	}
	for i, d := range testData {
		v := d.datum.ToString()
		if d.expected != *v.StringVal {
			t.Errorf("%d: expected %s, but got %s: %s", i, d.expected, v, d.datum)
		}
	}
}
