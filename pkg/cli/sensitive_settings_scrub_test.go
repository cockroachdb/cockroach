// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/stretchr/testify/require"
)

func TestSensitiveSettingNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	names := sensitiveSettingNames()
	require.NotEmpty(t, names)
	require.Contains(t, names, "server.oidc_authentication.client_secret")
	require.NotContains(t, names, "kv.range_merge.queue.enabled")
}

func TestScrubSensitiveSettingLogEntry(t *testing.T) {
	defer leaktest.AfterTest(t)()

	names := []string{"server.oidc_authentication.client_secret"}
	const tombstone = "REDACTEDBYZIP (test)"

	tests := []struct {
		name               string
		message            string
		redactable         bool
		expectedMessage    string
		expectedScrubbed   bool
		expectedTombstoned bool
	}{
		{
			name: "redactable entry keeps safe text and loses marked payloads",
			message: "set cluster setting ‹server.oidc_authentication.client_secret› " +
				"to ‹hunter2-secret›",
			redactable:       true,
			expectedMessage:  "set cluster setting ‹×› to ‹×›",
			expectedScrubbed: true,
		},
		{
			name:               "non-redactable entry is tombstoned",
			message:            "exec SET CLUSTER SETTING server.oidc_authentication.client_secret = 'hunter2-secret'",
			redactable:         false,
			expectedMessage:    tombstone,
			expectedScrubbed:   true,
			expectedTombstoned: true,
		},
		{
			name:               "sensitive setting named in upper case is caught",
			message:            "exec SET CLUSTER SETTING SERVER.OIDC_AUTHENTICATION.CLIENT_SECRET = 'hunter2-secret'",
			redactable:         false,
			expectedMessage:    tombstone,
			expectedScrubbed:   true,
			expectedTombstoned: true,
		},
		{
			name:            "entry without a sensitive name is untouched",
			message:         "exec SET CLUSTER SETTING cluster.label = 'hunter2-secret'",
			redactable:      false,
			expectedMessage: "exec SET CLUSTER SETTING cluster.label = 'hunter2-secret'",
		},
		{
			name: "role event with raw bound password is scrubbed",
			message: `{"EventType":"create_role","RoleName":"‹app›",` +
				`"PlaceholderValues":["‹'hunter2-secret'›"]}`,
			redactable:       true,
			expectedMessage:  `{"EventType":"create_role","RoleName":"‹×›","PlaceholderValues":["‹×›"]}`,
			expectedScrubbed: true,
		},
		{
			name: "role event with substituted password binds is untouched",
			message: `{"EventType":"alter_role","RoleName":"‹app›",` +
				`"PlaceholderValues":["‹'*****'›"]}`,
			redactable: true,
			expectedMessage: `{"EventType":"alter_role","RoleName":"‹app›",` +
				`"PlaceholderValues":["‹'*****'›"]}`,
		},
		{
			// The statement text renders any password option as '*****',
			// placeholder or not, so the marker there says nothing about the
			// binds.
			name: "role event with substituted statement but raw binds is scrubbed",
			message: `{"EventType":"create_role","Statement":"‹CREATE ROLE app WITH PASSWORD '*****'›",` +
				`"PlaceholderValues":["‹'hunter2-secret'›"]}`,
			redactable: true,
			expectedMessage: `{"EventType":"create_role","Statement":"‹×›",` +
				`"PlaceholderValues":["‹×›"]}`,
			expectedScrubbed: true,
		},
		{
			name:            "role event without placeholder values is untouched",
			message:         `{"EventType":"create_role","RoleName":"‹app›"}`,
			redactable:      true,
			expectedMessage: `{"EventType":"create_role","RoleName":"‹app›"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := logpb.Entry{Message: tc.message, Redactable: tc.redactable}
			scrubbed, tombstoned := scrubSensitiveSettingLogEntry(&e, names, tombstone)
			require.Equal(t, tc.expectedScrubbed, scrubbed)
			require.Equal(t, tc.expectedTombstoned, tombstoned)
			require.Equal(t, tc.expectedMessage, e.Message)
			if tc.expectedScrubbed {
				require.NotContains(t, e.Message, "hunter2-secret")
			}
		})
	}
}
