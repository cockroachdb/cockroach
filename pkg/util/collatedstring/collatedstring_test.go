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
		name               string
		collation          language.Tag
		expIsDeterministic bool
	}{
		{
			name:               "deterministic",
			collation:          makeTestLanguageTag("en_US-u-ks-level3-kc-false-ka-noignore"),
			expIsDeterministic: true,
		},
		{
			name:               "deterministic w/ default locale extensions",
			collation:          makeTestLanguageTag("en_US"),
			expIsDeterministic: true,
		},
		{
			name:               "nondeterministic - ks-level1",
			collation:          makeTestLanguageTag("en_US-u-ks-level1"),
			expIsDeterministic: false,
		},
		{
			name:               "nondeterministic - ks-level2",
			collation:          makeTestLanguageTag("en_US-u-ks-level2"),
			expIsDeterministic: false,
		},
		{
			name:               "nondeterministic - ka-shifted",
			collation:          makeTestLanguageTag("en_US-u-ka-shifted"),
			expIsDeterministic: false,
		},
		{
			name:               "nondeterministic - kc-true",
			collation:          makeTestLanguageTag("en_US-u-kc-true"),
			expIsDeterministic: false,
		},
		{
			name:               "invalid collation format",
			collation:          makeTestLanguageTag("not-a-valid-collation"),
			expIsDeterministic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isDeterministic := IsDeterministicCollation(tt.collation)
			require.Equal(t, tt.expIsDeterministic, isDeterministic)
		})
	}
}
