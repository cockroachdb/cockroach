package collatedstring

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDeterministicCollation_NoPanic(t *testing.T) {
	tests := []struct {
		name               string
		collation          string
		expIsDeterministic bool
		expErr             bool
	}{
		{
			name:               "deterministic",
			collation:          "en_US-u-ks-level3-kc-false-ka-noignore",
			expIsDeterministic: true,
			expErr:             false,
		},
		{
			name:               "deterministic w/ default locale extensions",
			collation:          "en_US",
			expIsDeterministic: true,
			expErr:             false,
		},
		{
			name:               "nondeterministic - ks-level1",
			collation:          "en_US-u-ks-level1",
			expIsDeterministic: false,
			expErr:             false,
		},
		{
			name:               "nondeterministic - ks-level2",
			collation:          "en_US-u-ks-level2",
			expIsDeterministic: false,
			expErr:             false,
		},
		{
			name:               "nondeterministic - ka-shifted",
			collation:          "en_US-u-ka-shifted",
			expIsDeterministic: false,
			expErr:             false,
		},
		{
			name:               "nondeterministic - kc-true",
			collation:          "en_US-u-kc-true",
			expIsDeterministic: false,
			expErr:             false,
		},
		{
			name:               "invalid collation format",
			collation:          "en_US-ks-level3-kc-false-ka-noignore",
			expIsDeterministic: false,
			expErr:             true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isDeterministic, err := IsDeterministicCollation(tt.collation)
			require.Equal(t, tt.expIsDeterministic, isDeterministic)
			require.Equal(t, err != nil, tt.expErr)
		})
	}
}
