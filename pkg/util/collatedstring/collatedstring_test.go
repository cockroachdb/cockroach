// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package collatedstring

import (
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/language"
)

func TestIsDeterministicCollation_NoPanic(t *testing.T) {
	makeTestLanguageTag := func(collation string) language.Tag {
		return language.Make(collation)
	}
	tests := []struct {
		input              string
		expIsDeterministic bool
	}{
		{
			input:              "en_US-u-ks-level3-kc-false-ka-noignore",
			expIsDeterministic: true,
		},
		{
			input:              "en_US",
			expIsDeterministic: true,
		},
		{
			input:              "en_US-u-ks-level1",
			expIsDeterministic: false,
		},
		{
			input:              "en_US-u-ks-level2",
			expIsDeterministic: false,
		},
		{
			input:              "en_US-u-ks-level1-ka-shifted",
			expIsDeterministic: false,
		},
		{
			input:              "en_US-u-ks-level2-ka-shifted",
			expIsDeterministic: false,
		}, {
			input:              "en_US-u-ka-shifted",
			expIsDeterministic: false,
		},
		{
			input:              "not-a-valid-input",
			expIsDeterministic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			isDeterministic := IsDeterministicCollation(makeTestLanguageTag(tt.input))
			require.Equal(t, tt.expIsDeterministic, isDeterministic)
		})
	}
}
