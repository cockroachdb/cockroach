// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package settings

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestEncodedValueSafeFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		rv       EncodedValue
		redacted string
		regular  string
	}{
		{
			rv: EncodedValue{
				Value: "asdf",
				Type:  "b",
			},

			regular:  `"asdf" (b)`,
			redacted: `‹"asdf"› (b)`,
		},
	} {
		t.Run(tc.regular, func(t *testing.T) {
			require.Equal(t, tc.regular, tc.rv.String())
			require.Equal(t, tc.redacted, string(redact.Sprint(tc.rv)))
		})
	}
}
