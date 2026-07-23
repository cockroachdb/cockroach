// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb_test

import (
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/stretchr/testify/require"
)

func TestKeyClampTenants(t *testing.T) {
	// tp = TablePrefix
	tp := keys.MakeSQLCodec(roachpb.MustMakeTenantID(3)).TablePrefix
	lowTp := keys.MakeSQLCodec(roachpb.MustMakeTenantID(1)).TablePrefix
	highTp := keys.MakeSQLCodec(roachpb.MustMakeTenantID(5)).TablePrefix
	sysTp := keys.SystemSQLCodec.TablePrefix
	tests := []struct {
		name     string
		k, a, b  roachpb.Key
		expected roachpb.Key
	}{
		{"key within main tenant is unchanged", tp(5), tp(1), tp(10), tp(5)},
		{"low tenant codec gets clamped to lower bound", lowTp(5), tp(1), tp(10), tp(1)},
		{"high tenant codec gets clamped to upper bound", highTp(5), tp(1), tp(10), tp(10)},
		{"system codec occurs below the tenant table boundaries", sysTp(5), tp(1), tp(10), tp(1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.k.Clamp(tt.a, tt.b)
			require.NoError(t, err)
			if !result.Equal(tt.expected) {
				t.Errorf("Clamp(%v, %v, %v) = %v; want %v", tt.k, tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestKeyClampTables(t *testing.T) {
	// tp = TablePrefix
	tp := keys.MakeSQLCodec(roachpb.MustMakeTenantID(3)).TablePrefix
	tests := []struct {
		name     string
		k, a, b  roachpb.Key
		expected roachpb.Key
	}{
		{"table within prefix is unchanged", tp(5), tp(1), tp(10), tp(5)},
		{"low table gets clamped to lower bound", tp(0), tp(1), tp(10), tp(1)},
		{"high table gets clamped to upper bound", tp(11), tp(1), tp(10), tp(10)},
		{"low table on lower bound is unchanged", tp(1), tp(1), tp(10), tp(1)},
		{"high table on upper bound is unchanged", tp(10), tp(1), tp(10), tp(10)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.k.Clamp(tt.a, tt.b)
			require.NoError(t, err)
			if !result.Equal(tt.expected) {
				t.Errorf("Clamp(%v, %v, %v) = %v; want %v", tt.k, tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestKeyClampTenantTablespace(t *testing.T) {
	timeseriesKeyPrefix := encoding.EncodeVarintAscending(
		encoding.EncodeBytesAscending(
			append(roachpb.Key(nil), keys.TimeseriesPrefix...),
			[]byte("my.fake.metric"),
		),
		int64(10),
	)
	tsKey := func(source string, timestamp int64) roachpb.Key {
		return append(encoding.EncodeVarintAscending(timeseriesKeyPrefix, timestamp), source...)
	}

	tp := keys.MakeSQLCodec(roachpb.MustMakeTenantID(3)).TablePrefix
	lower := tp(0)
	upper := tp(math.MaxUint32)
	tests := []struct {
		name     string
		k, a, b  roachpb.Key
		expected roachpb.Key
	}{
		{"KeyMin gets clamped to lower", roachpb.KeyMin, lower, upper, lower},
		{"KeyMax gets clamped to upper", roachpb.KeyMax, lower, upper, upper},
		{"Meta1Prefix gets clamped to lower", keys.Meta1Prefix, lower, upper, lower},
		{"Meta2Prefix gets clamped to lower", keys.Meta2Prefix, lower, upper, lower},
		{"TableDataMin gets clamped to lower", keys.TableDataMin, lower, upper, lower},
		// below is an unexpected test case for a tenant codec
		{"TableDataMax also gets clamped to lower", keys.TableDataMax, lower, upper, lower},
		{"SystemPrefix gets clamped to lower", keys.SystemPrefix, lower, upper, lower},
		{"TimeseriesKey gets clamped to lower", tsKey("5", 123), lower, upper, lower},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.k.Clamp(tt.a, tt.b)
			require.NoError(t, err)
			if !result.Equal(tt.expected) {
				t.Errorf("Clamp(%v, %v, %v) = %v; want %v", tt.k, tt.a, tt.b, result, tt.expected)
			}
		})
	}
}

func TestKeyClampError(t *testing.T) {
	// verify that max < min throws error
	a, b := roachpb.Key([]byte{'a'}), roachpb.Key([]byte{'b'})
	expected := `cannot clamp when min '"b"' is larger than max '"a"'`
	_, err := a.Clamp(b, a)
	if !testutils.IsError(err, expected) {
		t.Fatalf("expected error to be '%s', got '%s'", expected, err)
	}

	// verify that max = min throws no error
	_, err = a.Clamp(a, a)
	require.NoError(t, err)
}
