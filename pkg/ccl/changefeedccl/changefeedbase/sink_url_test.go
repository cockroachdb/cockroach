// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedbase

import (
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateSinkURIParams(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, tc := range map[string]struct {
		rawURL  string
		wantErr string
	}{
		"absent topic_name": {
			rawURL: "kafka://localhost",
		},
		"non-empty topic_name": {
			rawURL: "kafka://localhost?topic_name=foo",
		},
		"explicitly empty topic_name": {
			rawURL:  "kafka://localhost?topic_name=",
			wantErr: "param topic_name must not be empty",
		},
		"explicitly empty topic_name with other params": {
			rawURL:  "kafka://localhost?topic_name=&tls_enabled=true",
			wantErr: "param topic_name must not be empty",
		},
	} {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(tc.rawURL)
			require.NoError(t, err)
			err = ValidateSinkURIParams(u)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
