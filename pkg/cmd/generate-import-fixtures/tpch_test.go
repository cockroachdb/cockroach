// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestParseLong(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    int64
		wantErr bool
	}{
		{name: "valid", input: "42", want: 42},
		{name: "negative", input: "-7", want: -7},
		{name: "whitespace", input: "  123  ", want: 123},
		{name: "zero", input: "0", want: 0},
		{name: "non-numeric", input: "abc", wantErr: true},
		{name: "float", input: "3.14", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := parseLong(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, val)
		})
	}
}

func TestParseDouble(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    float64
		wantErr bool
	}{
		{name: "integer", input: "42", want: 42.0},
		{name: "decimal", input: "3.14", want: 3.14},
		{name: "negative", input: "-1.5", want: -1.5},
		{name: "whitespace", input: "  2.5  ", want: 2.5},
		{name: "scientific", input: "1e3", want: 1000.0},
		{name: "non-numeric", input: "abc", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := parseDouble(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.InDelta(t, tt.want, val, 0.001)
		})
	}
}

func TestParseDate(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Time
		wantErr bool
	}{
		{
			name:  "valid",
			input: "1995-03-15",
			want:  time.Date(1995, 3, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "epoch",
			input: "1970-01-01",
			want:  time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "whitespace",
			input: "  1998-12-01  ",
			want:  time.Date(1998, 12, 1, 0, 0, 0, 0, time.UTC),
		},
		{name: "invalid format", input: "03/15/1995", wantErr: true},
		{name: "empty", input: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			val, err := parseDate(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, val)
		})
	}
}

func TestColumnTypeString(t *testing.T) {
	require.Equal(t, "Long", Long.String())
	require.Equal(t, "Double", Double.String())
	require.Equal(t, "Text", Text.String())
	require.Equal(t, "Date", Date.String())
	require.Equal(t, "ColumnType(0)", ColumnType(0).String())
	require.Equal(t, "ColumnType(99)", ColumnType(99).String())
}

func TestAllTPCHTables(t *testing.T) {
	tables := allTPCHTables()
	require.Len(t, tables, 8)
	// Verify sorted order.
	for i := 1; i < len(tables); i++ {
		require.Less(t, tables[i-1], tables[i], "tables should be sorted")
	}
}

func TestGetTPCHTable(t *testing.T) {
	td, err := getTPCHTable("region")
	require.NoError(t, err)
	require.Equal(t, "region", td.Name)
	require.Len(t, td.Columns, 3)

	_, err = getTPCHTable("nonexistent")
	require.Error(t, err)
}
