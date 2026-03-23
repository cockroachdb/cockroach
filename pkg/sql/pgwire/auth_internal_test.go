// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pgwire

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// mockAuthConn is a minimal mock implementation of AuthConn for testing.
// It tracks whether SetDbUser was called and what value was passed.
type mockAuthConn struct {
	setDbUserCalled   bool
	dbUserSet         username.SQLUsername
	systemIdentitySet string
}

var _ AuthConn = (*mockAuthConn)(nil)

func (m *mockAuthConn) SendAuthRequest(authType int32, data []byte) error { return nil }
func (m *mockAuthConn) GetPwdData() ([]byte, error)                       { return nil, nil }
func (m *mockAuthConn) AuthOK(ctx context.Context)                        {}
func (m *mockAuthConn) AuthFail(err error)                                {}
func (m *mockAuthConn) SetAuthMethod(method redact.SafeString)            {}
func (m *mockAuthConn) SetSystemIdentity(systemIdentity string) {
	m.systemIdentitySet = systemIdentity
}
func (m *mockAuthConn) SetDbUser(dbUser username.SQLUsername) {
	m.setDbUserCalled = true
	m.dbUserSet = dbUser
}
func (m *mockAuthConn) LogAuthInfof(ctx context.Context, msg redact.RedactableString) {}
func (m *mockAuthConn) LogAuthFailed(
	ctx context.Context, reason eventpb.AuthFailReason, err error,
) {
}
func (m *mockAuthConn) LogAuthOK(ctx context.Context)                        {}
func (m *mockAuthConn) LogSessionEnd(ctx context.Context, endTime time.Time) {}
func (m *mockAuthConn) GetTenantSpecificMetrics() *tenantSpecificMetrics {
	return nil
}

// TestCheckClientUsernameMatchesMapping tests the checkClientUsernameMatchesMapping
// function which verifies that client-provided usernames match identity mappings.
func TestCheckClientUsernameMatchesMapping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Helper to create a test conn with specified user
	makeTestConn := func(t *testing.T, requestedUser username.SQLUsername) *conn {
		s := newTestServer()
		c := s.newConn(
			ctx,
			func() {}, // cancel function
			&fakeConn{},
			sql.SessionArgs{
				User:                  requestedUser,
				ConnResultsBufferSize: 16 << 10,
			},
			timeutil.Now(),
		)
		return c
	}

	tests := []struct {
		name                    string
		requestedUser           string
		systemIdentity          string
		sanIdentities           []string
		mockMapRoleFunc         RoleMapper
		mockEnhancedMapRoleFunc EnhancedRoleMapper
		expectedIdentity        string
		expectedError           string
		expectSetDbUserCalled   bool
		expectedDbUser          string
	}{
		// SAN Identity Tests (Enhanced Mapping Path)
		{
			name:           "SAN path - successful match with single identity",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  []string{"SAN:DNS:example.com"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return []identmap.IdentityMapping{
					{
						SystemIdentity: "SAN:DNS:example.com",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("alice"),
					},
				}, nil
			},
			expectedIdentity:      "SAN:DNS:example.com",
			expectSetDbUserCalled: true,
			expectedDbUser:        "alice",
		},
		{
			name:           "SAN path - EnhancedMapRole returns error",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  []string{"SAN:DNS:example.com"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return nil, errors.New("mapping service unavailable")
			},
			expectedError:         "mapping service unavailable",
			expectSetDbUserCalled: false,
		},
		{
			name:           "SAN path - EnhancedMapRole returns empty mappings",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  []string{"SAN:DNS:example.com"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return []identmap.IdentityMapping{}, nil
			},
			expectedError:         `system identities ["SAN:DNS:example.com"] did not map to any database role`,
			expectSetDbUserCalled: false,
		},
		{
			name:           "SAN path - mappings returned but none match requested user",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  []string{"SAN:DNS:example.com"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return []identmap.IdentityMapping{
					{
						SystemIdentity: "SAN:DNS:example.com",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("bob"),
					},
				}, nil
			},
			expectedError:         `requested user identity "alice" does not correspond to any mapping for SAN identities ["SAN:DNS:example.com"]`,
			expectSetDbUserCalled: false,
		},
		{
			name:           "SAN path - multiple mappings, one matches",
			requestedUser:  "charlie",
			systemIdentity: "charlie@example.com",
			sanIdentities:  []string{"SAN:DNS:example.com", "SAN:URI:example.com", "SAN:IP:192.0.2.1"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return []identmap.IdentityMapping{
					{
						SystemIdentity: "SAN:DNS:example.com",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("alice"),
					},
					{
						SystemIdentity: "SAN:URI:example.com",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("bob"),
					},
					{
						SystemIdentity: "SAN:IP:192.0.2.1",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("charlie"),
					},
				}, nil
			},
			expectedIdentity:      "SAN:IP:192.0.2.1",
			expectSetDbUserCalled: true,
			expectedDbUser:        "charlie",
		},
		// Regular System Identity Tests (Standard Mapping Path)
		{
			name:           "Regular path - successful match",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{
					username.MakeSQLUsernameFromPreNormalizedString("alice"),
				}, nil
			},
			expectedIdentity:      "alice@example.com",
			expectSetDbUserCalled: true,
			expectedDbUser:        "alice",
		},
		{
			name:           "Regular path - MapRole returns error",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return nil, errors.New("LDAP server unreachable")
			},
			expectedError:         "LDAP server unreachable",
			expectSetDbUserCalled: false,
		},
		{
			name:           "Regular path - MapRole returns empty mappings",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{}, nil
			},
			expectedError:         `system identity "alice@example.com" did not map to a database role`,
			expectSetDbUserCalled: false,
		},
		{
			name:           "Regular path - mappings returned but none match requested user",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{
					username.MakeSQLUsernameFromPreNormalizedString("bob"),
					username.MakeSQLUsernameFromPreNormalizedString("charlie"),
				}, nil
			},
			expectedError:         `requested user identity "alice" does not correspond to any mapping for system identity "alice@example.com"`,
			expectSetDbUserCalled: false,
		},
		{
			name:           "Regular path - multiple mapped users, one matches",
			requestedUser:  "charlie",
			systemIdentity: "charlie@example.com",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{
					username.MakeSQLUsernameFromPreNormalizedString("alice"),
					username.MakeSQLUsernameFromPreNormalizedString("bob"),
					username.MakeSQLUsernameFromPreNormalizedString("charlie"),
				}, nil
			},
			expectedIdentity:      "charlie@example.com",
			expectSetDbUserCalled: true,
			expectedDbUser:        "charlie",
		},
		// Edge Cases
		{
			name:           "Regular path - empty systemIdentity string",
			requestedUser:  "alice",
			systemIdentity: "",
			sanIdentities:  nil,
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{
					username.MakeSQLUsernameFromPreNormalizedString("alice"),
				}, nil
			},
			expectedIdentity:      "",
			expectSetDbUserCalled: true,
			expectedDbUser:        "alice",
		},
		{
			name:           "SAN path - exactly one SAN identity",
			requestedUser:  "alice",
			systemIdentity: "ignored",
			sanIdentities:  []string{"SAN:DNS:alice.example.com"},
			mockEnhancedMapRoleFunc: func(ctx context.Context, ids []string) ([]identmap.IdentityMapping, error) {
				return []identmap.IdentityMapping{
					{
						SystemIdentity: "SAN:DNS:alice.example.com",
						MappedUser:     username.MakeSQLUsernameFromPreNormalizedString("alice"),
					},
				}, nil
			},
			expectedIdentity:      "SAN:DNS:alice.example.com",
			expectSetDbUserCalled: true,
			expectedDbUser:        "alice",
		},
		{
			name:           "SAN path - empty slice is treated as no SAN identities",
			requestedUser:  "alice",
			systemIdentity: "alice@example.com",
			sanIdentities:  []string{},
			mockMapRoleFunc: func(ctx context.Context, systemIdentity string) ([]username.SQLUsername, error) {
				return []username.SQLUsername{
					username.MakeSQLUsernameFromPreNormalizedString("alice"),
				}, nil
			},
			expectedIdentity:      "alice@example.com",
			expectSetDbUserCalled: true,
			expectedDbUser:        "alice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock AuthConn
			mockAC := &mockAuthConn{}

			// Create AuthBehaviors with appropriate mapper
			behaviors := &AuthBehaviors{}
			if tt.mockMapRoleFunc != nil {
				behaviors.SetRoleMapper(tt.mockMapRoleFunc)
			}
			if tt.mockEnhancedMapRoleFunc != nil {
				behaviors.SetEnhancedRoleMapper(tt.mockEnhancedMapRoleFunc)
			}
			behaviors.SetSANIdentities(tt.sanIdentities)

			// Create test conn with requested user
			requestedUser := username.MakeSQLUsernameFromPreNormalizedString(tt.requestedUser)
			c := makeTestConn(t, requestedUser)

			// Call the function under test
			matchedIdentity, err := c.checkClientUsernameMatchesMapping(
				ctx, mockAC, behaviors, tt.systemIdentity,
			)

			// Verify error expectations
			if tt.expectedError != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedError)
				require.Equal(t, "", matchedIdentity, "matchedIdentity should be empty on error")
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expectedIdentity, matchedIdentity)
			}

			// Verify SetDbUser was called appropriately
			require.Equal(t, tt.expectSetDbUserCalled, mockAC.setDbUserCalled,
				"SetDbUser called status mismatch")

			if tt.expectSetDbUserCalled {
				require.Equal(t, tt.expectedDbUser, mockAC.dbUserSet.Normalized(),
					"SetDbUser called with wrong username")
			}
		})
	}
}
