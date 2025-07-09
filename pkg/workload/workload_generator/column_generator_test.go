// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"encoding/hex"
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetIntArg(t *testing.T) {
	m := map[string]interface{}{
		"i":   7,
		"i64": int64(13),
		"f":   5.0,
		"s":   "21",
	}
	assert.Equal(t, 7, getIntArg(m, "i", 0), "getIntArg int")
	assert.Equal(t, 13, getIntArg(m, "i64", 0), "getIntArg int64")
	assert.Equal(t, 5, getIntArg(m, "f", 0), "getIntArg float64")
	assert.Equal(t, 21, getIntArg(m, "s", 0), "getIntArg string")
	assert.Equal(t, 42, getIntArg(m, "missing", 42), "getIntArg default")
}

func TestGetFloatArg(t *testing.T) {
	m := map[string]interface{}{
		"f":   3.14,
		"i":   2,
		"i64": int64(4),
		"s":   "6.28",
	}
	assert.InDelta(t, 3.14, getFloatArg(m, "f", 0), 1e-9, "getFloatArg float64")
	assert.InDelta(t, 2.0, getFloatArg(m, "i", 0), 1e-9, "getFloatArg int")
	assert.InDelta(t, 4.0, getFloatArg(m, "i64", 0), 1e-9, "getFloatArg int64")
	assert.InDelta(t, 6.28, getFloatArg(m, "s", 0), 1e-9, "getFloatArg string")
	assert.InDelta(t, 9.9, getFloatArg(m, "missing", 9.9), 1e-9, "getFloatArg default")
}

func TestGetStringArg(t *testing.T) {
	m := map[string]interface{}{"a": "hello", "b": 123}
	assert.Equal(t, "hello", getStringArg(m, "a", "x"), "present")
	assert.Equal(t, "x", getStringArg(m, "b", "x"), "wrong type")
	assert.Equal(t, "y", getStringArg(m, "c", "y"), "missing")
}

func TestSplitmix64AndBatchSeed(t *testing.T) {
	x := uint64(12345)
	v1 := splitmix64(x)
	assert.Equal(t, v1, splitmix64(x), "deterministic")
	assert.NotEqual(t, v1, splitmix64(x+1), "no collision")
	seed := buildBatchSeed(1, 2)
	want := int64(splitmix64((uint64(1) << 32) | uint64(2)))
	assert.Equal(t, want, seed, "batch seed")
}

func TestSequenceGenerator(t *testing.T) {
	col := ColumnMeta{Args: map[string]interface{}{"start": 5}}
	gen := buildSequenceGenerator(col, 2, 10)
	assert.Equal(t, "25", gen.Next(), "first value")
	assert.Equal(t, "26", gen.Next(), "second value")
}

func TestIntegerAndFloatGenerators(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	icol := ColumnMeta{Args: map[string]interface{}{"min": 1, "max": 3, "null_pct": 0.0}}
	igen := buildIntegerGenerator(icol, rng)
	for i := 0; i < 10; i++ {
		v := igen.Next()
		n, err := strconv.Atoi(v)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, n, 1)
		assert.LessOrEqual(t, n, 3)
	}
	fcol := ColumnMeta{Args: map[string]interface{}{"min": 1.0, "max": 2.0, "round": 1, "null_pct": 0.0}}
	fgen := buildFloatGenerator(fcol, rng)
	for i := 0; i < 10; i++ {
		v := fgen.Next()
		assert.Contains(t, v, ".", "should have decimal place")
	}
}

func TestTimestampGenerator(t *testing.T) {
	col := ColumnMeta{Args: map[string]interface{}{"start": "2000-01-02", "end": "2000-01-03", "format": "%Y-%m-%d"}}
	rng := rand.New(rand.NewSource(0))
	gen := buildTimestampGenerator(col, rng)
	v := gen.Next()
	_, err := time.Parse("2006-01-02", v)
	assert.NoError(t, err, "parseable date")
}

func TestUuidGenerator(t *testing.T) {
	rng := rand.New(rand.NewSource(0))
	col := ColumnMeta{}
	gen := buildUuidGenerator(col, rng)
	v1 := gen.Next()
	v2 := gen.Next()
	assert.Len(t, v1, 32, "length")
	_, err := hex.DecodeString(v1)
	assert.NoError(t, err, "valid hex")
	assert.NotEqual(t, v1, v2, "varies")
}

func TestStringBoolJsonGenerators(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	scol := ColumnMeta{Args: map[string]interface{}{"min": 2, "max": 4, "null_pct": 0.0}}
	sgen := buildStringGenerator(scol, rng)
	for i := 0; i < 5; i++ {
		v := sgen.Next()
		assert.GreaterOrEqual(t, len(v), 2)
		assert.LessOrEqual(t, len(v), 4)
	}
	bcol := ColumnMeta{Args: map[string]interface{}{"null_pct": 0.0}}
	bgen := buildBooleanGenerator(bcol, rng)
	for i := 0; i < 5; i++ {
		v := bgen.Next()
		assert.Contains(t, []string{"0", "1"}, v)
	}
	jcol := ColumnMeta{Args: map[string]interface{}{"min": 1, "max": 1, "null_pct": 0.0}}
	jgen := buildJsonGenerator(jcol, rng)
	v := jgen.Next()
	assert.True(t, strings.HasPrefix(v, "{\"k\":\"") && strings.HasSuffix(v, "\"}"), "JSON object")
}

func TestWrappers(t *testing.T) {
	base := &SequenceGen{cur: 0}
	dw := NewDefaultWrapper(base, 1.0, "L", 42)
	assert.Equal(t, "L", dw.Next(), "DefaultWrapper literal")
	ug := NewUniqueWrapper(base, 2)
	v1 := ug.Next()
	v2 := ug.Next()
	assert.NotEqual(t, v1, v2, "UniqueWrapper uniqueness")
	fg := NewFkWrapper(base, 3)
	vals := []string{fg.Next(), fg.Next(), fg.Next(), fg.Next()}
	assert.Equal(t, vals[0], vals[1], "FkWrapper repeat")
	assert.Equal(t, vals[1], vals[2], "FkWrapper repeat")
	assert.NotEqual(t, vals[2], vals[3], "FkWrapper advance")
}

func TestMakeGeneratorAllTypes(t *testing.T) {
	schema := Schema{}
	types := []struct {
		typ     GeneratorType
		args    map[string]interface{}
		checker func(string) bool
	}{
		{"integer", map[string]interface{}{"min": 0, "max": 10, "null_pct": 0.0}, func(v string) bool {
			_, err := strconv.Atoi(v)
			return err == nil
		}},
		{"float", map[string]interface{}{"min": 0.0, "max": 1.0, "round": 2, "null_pct": 0.0}, func(v string) bool {
			return strings.Contains(v, ".")
		}},
		{"string", map[string]interface{}{"min": 1, "max": 3, "null_pct": 0.0}, func(v string) bool {
			return len(v) >= 1 && len(v) <= 3
		}},
		{"timestamp", map[string]interface{}{"start": "2000-01-01", "end": "2000-01-02", "format": "%Y-%m-%d"}, func(v string) bool {
			_, err := time.Parse("2006-01-02", v)
			return err == nil
		}},
		{"uuid", map[string]interface{}{}, func(v string) bool {
			b, err := hex.DecodeString(v)
			return err == nil && len(b) == 16
		}},
		{"bool", map[string]interface{}{"null_pct": 0.0}, func(v string) bool {
			return v == "0" || v == "1"
		}},
		{"json", map[string]interface{}{"min": 1, "max": 1, "null_pct": 0.0}, func(v string) bool {
			return strings.HasPrefix(v, "{\"k\":\"") && strings.HasSuffix(v, "\"}")
		}},
	}
	for _, tt := range types {
		gen := buildGenerator(ColumnMeta{Type: tt.typ, Args: tt.args}, 0, 1, schema)
		v := gen.Next()
		assert.True(t, tt.checker(v), "%s generator produced %q", tt.typ, v)
	}
}
