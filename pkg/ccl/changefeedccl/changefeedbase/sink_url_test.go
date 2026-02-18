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

func TestConsumeParamRejectEmpty(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for name, tc := range map[string]struct {
		rawURL  string
		param   string
		wantVal string
		wantErr string
	}{
		"absent": {
			rawURL:  "kafka://localhost",
			param:   "topic_name",
			wantVal: "",
		},
		"explicitly empty": {
			rawURL:  "kafka://localhost?topic_name=",
			param:   "topic_name",
			wantErr: "param topic_name must not be empty",
		},
		"explicitly empty with other params": {
			rawURL:  "kafka://localhost?topic_name=&tls_enabled=true",
			param:   "topic_name",
			wantErr: "param topic_name must not be empty",
		},
		"with value": {
			rawURL:  "kafka://localhost?topic_name=foo",
			param:   "topic_name",
			wantVal: "foo",
		},
	} {
		t.Run(name, func(t *testing.T) {
			u, err := url.Parse(tc.rawURL)
			require.NoError(t, err)
			sinkURL := &SinkURL{URL: u}

			val, err := sinkURL.ConsumeParamRejectEmpty(tc.param)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.wantVal, val)
			}
		})
	}
}
