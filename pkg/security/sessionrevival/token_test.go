// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionrevival

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/stretchr/testify/require"
)

func timestampProto(t *testing.T, ts time.Time) *pbtypes.Timestamp {
	p, err := pbtypes.TimestampProto(ts)
	require.NoError(t, err)
	return p
}

func TestValidatePayloadContents(t *testing.T) {
	now := timeutil.NowNoMono().Add(-1 * time.Second)
	username := username.MakeSQLUsernameFromPreNormalizedString("testuser")
	testCases := []struct {
		description string
		payload     *sessiondatapb.SessionRevivalToken_Payload
		valid       bool
		errorText   string
	}{
		{
			description: "valid token",
			payload: &sessiondatapb.SessionRevivalToken_Payload{
				User:      username.Normalized(),
				IssuedAt:  timestampProto(t, now),
				ExpiresAt: timestampProto(t, now.Add(tokenLifetime)),
			},
			valid: true,
		},
		{
			description: "username is incorrect",
			payload: &sessiondatapb.SessionRevivalToken_Payload{
				User:      "anotheruser",
				IssuedAt:  timestampProto(t, now),
				ExpiresAt: timestampProto(t, now.Add(tokenLifetime)),
			},
			valid:     false,
			errorText: "token is for the wrong user \"anotheruser\", wanted \"testuser\"",
		},
		{
			description: "issued at is in the future",
			payload: &sessiondatapb.SessionRevivalToken_Payload{
				User:      username.Normalized(),
				IssuedAt:  timestampProto(t, now.Add(1*time.Minute)),
				ExpiresAt: timestampProto(t, now.Add(tokenLifetime)),
			},
			valid:     false,
			errorText: fmt.Sprintf("token issue time is in the future (%v)", now.Add(1*time.Minute)),
		},
		{
			description: "expires at is in the past",
			payload: &sessiondatapb.SessionRevivalToken_Payload{
				User:      username.Normalized(),
				IssuedAt:  timestampProto(t, now.Add(-10*time.Minute)),
				ExpiresAt: timestampProto(t, now.Add(-1*time.Minute)),
			},
			valid:     false,
			errorText: fmt.Sprintf("token expiration time is in the past (%v)", now.Add(-1*time.Minute)),
		},
		{
			description: "expires at is too far away",
			payload: &sessiondatapb.SessionRevivalToken_Payload{
				User:      username.Normalized(),
				IssuedAt:  timestampProto(t, now),
				ExpiresAt: timestampProto(t, now.Add(1*time.Hour)),
			},
			valid:     false,
			errorText: fmt.Sprintf("token expiration time is too far in the future (%v)", now.Add(1*time.Hour)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			err := validatePayloadContents(tc.payload, username)
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, tc.errorText)
			}
		})
	}
}
