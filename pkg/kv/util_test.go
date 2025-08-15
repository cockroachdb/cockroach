// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestMarshalKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name     string
		input    interface{}
		expected roachpb.Key
		wantErr  bool
	}{
		{
			name:     "roachpb.Key pointer",
			input:    &roachpb.Key("test"),
			expected: roachpb.Key("test"),
		},
		{
			name:     "roachpb.Key value",
			input:    roachpb.Key("test"),
			expected: roachpb.Key("test"),
		},
		{
			name:     "roachpb.RKey pointer",
			input:    &roachpb.RKey("test"),
			expected: roachpb.Key("test"),
		},
		{
			name:     "roachpb.RKey value",
			input:    roachpb.RKey("test"),
			expected: roachpb.Key("test"),
		},
		{
			name:     "string",
			input:    "test",
			expected: roachpb.Key("test"),
		},
		{
			name:     "byte slice",
			input:    []byte("test"),
			expected: roachpb.Key("test"),
		},
		{
			name:    "unsupported type",
			input:   123,
			wantErr: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: roachpb.Key(""),
		},
		{
			name:     "empty byte slice",
			input:    []byte{},
			expected: roachpb.Key{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := marshalKey(tc.input)
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "unable to marshal key")
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestMarshalValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name      string
		input     interface{}
		validator func(t *testing.T, v roachpb.Value)
		wantErr   bool
	}{
		{
			name:  "roachpb.Value pointer",
			input: func() *roachpb.Value { v := roachpb.MakeValueFromString("test"); return &v }(),
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("test"), bytes)
			},
		},
		{
			name:  "nil",
			input: nil,
			validator: func(t *testing.T, v roachpb.Value) {
				require.True(t, v.IsEmpty())
			},
		},
		{
			name:  "bool true",
			input: true,
			validator: func(t *testing.T, v roachpb.Value) {
				b, err := v.GetBool()
				require.NoError(t, err)
				require.True(t, b)
			},
		},
		{
			name:  "bool false",
			input: false,
			validator: func(t *testing.T, v roachpb.Value) {
				b, err := v.GetBool()
				require.NoError(t, err)
				require.False(t, b)
			},
		},
		{
			name:  "string",
			input: "test",
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("test"), bytes)
			},
		},
		{
			name:  "byte slice",
			input: []byte("test"),
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("test"), bytes)
			},
		},
		{
			name:  "apd.Decimal",
			input: func() apd.Decimal { d, _ := apd.NewFromString("123.45"); return *d }(),
			validator: func(t *testing.T, v roachpb.Value) {
				d, err := v.GetDecimal()
				require.NoError(t, err)
				expected, _ := apd.NewFromString("123.45")
				require.True(t, d.Cmp(expected) == 0)
			},
		},
		{
			name:  "roachpb.Key",
			input: roachpb.Key("test"),
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("test"), bytes)
			},
		},
		{
			name:  "time.Time",
			input: time.Unix(1234567890, 0),
			validator: func(t *testing.T, v roachpb.Value) {
				tm, err := v.GetTime()
				require.NoError(t, err)
				require.Equal(t, time.Unix(1234567890, 0), tm)
			},
		},
		{
			name:  "duration.Duration",
			input: duration.MakeDuration(123, 0, 0),
			validator: func(t *testing.T, v roachpb.Value) {
				d, err := v.GetDuration()
				require.NoError(t, err)
				require.Equal(t, duration.MakeDuration(123, 0, 0), d)
			},
		},
		{
			name:  "int",
			input: int(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "int8",
			input: int8(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "int16",
			input: int16(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "int32",
			input: int32(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "int64",
			input: int64(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uint",
			input: uint(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uint8",
			input: uint8(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uint16",
			input: uint16(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uint32",
			input: uint32(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uint64",
			input: uint64(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "uintptr",
			input: uintptr(123),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(123), i)
			},
		},
		{
			name:  "float32",
			input: float32(123.45),
			validator: func(t *testing.T, v roachpb.Value) {
				f, err := v.GetFloat()
				require.NoError(t, err)
				require.InDelta(t, 123.45, f, 0.01)
			},
		},
		{
			name:  "float64",
			input: float64(123.45),
			validator: func(t *testing.T, v roachpb.Value) {
				f, err := v.GetFloat()
				require.NoError(t, err)
				require.Equal(t, 123.45, f)
			},
		},
		{
			name:  "reflect bool",
			input: reflect.ValueOf(true).Interface(),
			validator: func(t *testing.T, v roachpb.Value) {
				b, err := v.GetBool()
				require.NoError(t, err)
				require.True(t, b)
			},
		},
		{
			name:  "reflect string",
			input: reflect.ValueOf("test").Interface(),
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("test"), bytes)
			},
		},
		{
			name:    "unsupported type",
			input:   make(chan int),
			wantErr: true,
		},
		{
			name:    "roachpb.Value by value",
			input:   roachpb.MakeValueFromString("test"),
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.wantErr {
				defer func() {
					if r := recover(); r != nil {
						require.Contains(t, fmt.Sprintf("%v", r), "unexpected type roachpb.Value")
					}
				}()
			}

			result, err := marshalValue(tc.input)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				tc.validator(t, result)
			}
		})
	}
}

func TestMarshalValueCustomTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type MyInt int
	type MyString string
	type MyBool bool

	testCases := []struct {
		name      string
		input     interface{}
		validator func(t *testing.T, v roachpb.Value)
	}{
		{
			name:  "custom int type",
			input: MyInt(42),
			validator: func(t *testing.T, v roachpb.Value) {
				i, err := v.GetInt()
				require.NoError(t, err)
				require.Equal(t, int64(42), i)
			},
		},
		{
			name:  "custom string type",
			input: MyString("hello"),
			validator: func(t *testing.T, v roachpb.Value) {
				bytes, err := v.GetBytes()
				require.NoError(t, err)
				require.Equal(t, []byte("hello"), bytes)
			},
		},
		{
			name:  "custom bool type",
			input: MyBool(true),
			validator: func(t *testing.T, v roachpb.Value) {
				b, err := v.GetBool()
				require.NoError(t, err)
				require.True(t, b)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := marshalValue(tc.input)
			require.NoError(t, err)
			tc.validator(t, result)
		})
	}
}
