// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package security

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestValidateSANMatchForUser(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name           string
		user           string
		certSANs       []string
		rootSANsToSet  string
		nodeSANsToSet  string
		expectedResult bool
	}{
		// Non-root/node users (always return true)
		{
			name:           "regular user alice with DNS SAN",
			user:           "alice",
			certSANs:       []string{"SAN:DNS:example.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "regular user bob with URI SAN",
			user:           "bob",
			certSANs:       []string{"SAN:URI:spiffe://example.org/service"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "regular user charlie with empty SANs",
			user:           "charlie",
			certSANs:       []string{},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "regular user dave with nil SANs",
			user:           "dave",
			certSANs:       nil,
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "regular user eve with multiple SANs",
			user:           "eve",
			certSANs:       []string{"SAN:DNS:foo.com", "SAN:URI:bar", "SAN:IP:192.168.1.1"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		// Root user without configured root SANs
		{
			name:           "root user with empty cert SANs and no configured root SANs",
			user:           username.RootUser,
			certSANs:       []string{},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		{
			name:           "root user with DNS cert SAN but no configured root SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:example.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Root user with configured root SANs - exact match
		{
			name:           "root user exact match single DNS SAN",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:example.com"},
			rootSANsToSet:  "DNS=example.com",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		// Root user with configured root SANs - superset
		{
			name:           "root user cert has configured SAN plus additional DNS",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:example.com", "SAN:URI:spiffe://foo"},
			rootSANsToSet:  "DNS=example.com",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "root user cert has all configured SANs plus more",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:a.com", "SAN:DNS:b.com", "SAN:URI:c"},
			rootSANsToSet:  "DNS=a.com,DNS=b.com",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		// Root user with configured root SANs - subset (should fail)
		{
			name:           "root user cert missing some configured SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:example.com"},
			rootSANsToSet:  "DNS=example.com,URI=spiffe://foo",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Root user with configured root SANs - partial overlap
		{
			name:           "root user cert has partial overlap with configured SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:a.com", "SAN:DNS:b.com"},
			rootSANsToSet:  "DNS=b.com,DNS=c.com",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Root user with configured root SANs - completely different
		{
			name:           "root user cert completely different from configured SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:a.com"},
			rootSANsToSet:  "DNS=b.com",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Root user with configured root SANs - empty cert SANs
		{
			name:           "root user empty cert SANs with configured root SANs",
			user:           username.RootUser,
			certSANs:       []string{},
			rootSANsToSet:  "DNS=example.com",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Root user with configured root SANs - multiple types
		{
			name:           "root user cert has all configured SANs of different types",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:a.com", "SAN:URI:b", "SAN:IP:1.2.3.4", "SAN:IP:5.6.7.8"},
			rootSANsToSet:  "DNS=a.com,URI=b,IP=5.6.7.8,IP=1.2.3.4",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "root user cert missing IP SAN from configured SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:a.com", "SAN:URI:b"},
			rootSANsToSet:  "DNS=a.com,URI=b,IP=1.2.3.4",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Node user without configured node SANs
		{
			name:           "node user with empty cert SANs and no configured node SANs",
			user:           username.NodeUser,
			certSANs:       []string{},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		{
			name:           "node user with DNS cert SAN but no configured node SANs",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:node.example.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		// Node user with configured node SANs - exact match
		{
			name:           "node user exact match single DNS SAN",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:node.example.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=node.example.com",
			expectedResult: true,
		},
		// Node user with configured node SANs - superset
		{
			name:           "node user cert has configured SAN plus additional URI",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:node.example.com", "SAN:DNS:node.example_new.com", "SAN:URI:spiffe://node"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=node.example.com,DNS=node.example_new.com",
			expectedResult: true,
		},
		// Node user with configured node SANs - subset
		{
			name:           "node user cert missing some configured SANs",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:node.example.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=node.example.com,URI=spiffe://node",
			expectedResult: false,
		},
		// Node user with configured node SANs - completely different
		{
			name:           "node user cert completely different from configured SANs",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:a.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=b.com",
			expectedResult: false,
		},
		// Node user with configured node SANs - empty cert SANs
		{
			name:           "node user empty cert SANs with configured node SANs",
			user:           username.NodeUser,
			certSANs:       []string{},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=node.example.com",
			expectedResult: false,
		},
		// Both root and node SANs configured
		{
			name:           "root user with both root and node SANs configured matches root",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:root.com"},
			rootSANsToSet:  "DNS=root.com",
			nodeSANsToSet:  "DNS=node.com",
			expectedResult: true,
		},
		{
			name:           "root user with both root and node SANs configured matches node not root",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:node.com"},
			rootSANsToSet:  "DNS=root.com",
			nodeSANsToSet:  "DNS=node.com",
			expectedResult: false, // User-specific: root user must match root SANs
		},
		{
			name:           "node user with both root and node SANs configured matches node",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:node.com"},
			rootSANsToSet:  "DNS=root.com",
			nodeSANsToSet:  "DNS=node.com",
			expectedResult: true,
		},
		{
			name:           "node user with both root and node SANs configured matches root not node",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:root.com"},
			rootSANsToSet:  "DNS=root.com",
			nodeSANsToSet:  "DNS=node.com",
			expectedResult: false, // User-specific: node user must match node SANs
		},
		{
			name:           "root user cert matches both root and node SANs",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:shared.com"},
			rootSANsToSet:  "DNS=shared.com",
			nodeSANsToSet:  "DNS=shared.com",
			expectedResult: true,
		},
		{
			name:           "node user cert matches both root and node SANs",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:shared.com"},
			rootSANsToSet:  "DNS=shared.com",
			nodeSANsToSet:  "DNS=shared.com",
			expectedResult: true,
		},

		// Different SAN type combinations
		{
			name:           "root user with URI SPIFFE SAN exact match",
			user:           username.RootUser,
			certSANs:       []string{"SAN:URI:spiffe://example.org/root"},
			rootSANsToSet:  "URI=spiffe://example.org/root",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "node user with URI SPIFFE SAN exact match",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:URI:spiffe://example.org/node"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "URI=spiffe://example.org/node",
			expectedResult: true,
		},
		{
			name:           "root user with IP SAN exact match",
			user:           username.RootUser,
			certSANs:       []string{"SAN:IP:192.168.1.1"},
			rootSANsToSet:  "IP=192.168.1.1",
			nodeSANsToSet:  "",
			expectedResult: true,
		},
		{
			name:           "root user with IP SAN mismatch",
			user:           username.RootUser,
			certSANs:       []string{"SAN:IP:192.168.1.1"},
			rootSANsToSet:  "IP=192.168.1.2",
			nodeSANsToSet:  "",
			expectedResult: false,
		},
		{
			name:           "root user with mixed type SANs all present",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:example.com", "SAN:URI:spiffe://example.org", "SAN:IP:192.168.1.1"},
			rootSANsToSet:  "DNS=example.com,URI=spiffe://example.org",
			nodeSANsToSet:  "",
			expectedResult: true,
		},

		// Additional edge cases for user-specific validation
		{
			name:           "root user when only node SANs are configured",
			user:           username.RootUser,
			certSANs:       []string{"SAN:DNS:node.com"},
			rootSANsToSet:  "",
			nodeSANsToSet:  "DNS=node.com",
			expectedResult: false, // No root SANs configured
		},
		{
			name:           "node user when only root SANs are configured",
			user:           username.NodeUser,
			certSANs:       []string{"SAN:DNS:root.com"},
			rootSANsToSet:  "DNS=root.com",
			nodeSANsToSet:  "",
			expectedResult: false, // No node SANs configured
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				UnsetRootSAN()
				UnsetNodeSAN()
			}()

			if tt.rootSANsToSet != "" {
				err := SetRootSAN(tt.rootSANsToSet)
				require.NoError(t, err)
			}

			if tt.nodeSANsToSet != "" {
				err := SetNodeSAN(tt.nodeSANsToSet)
				require.NoError(t, err)
			}

			result := validateSANMatchForUser(tt.certSANs, tt.user)
			require.Equal(t, tt.expectedResult, result,
				"validateSANMatchForUser(%v, %q) = %v, want %v",
				tt.certSANs, tt.user, result, tt.expectedResult)
		})
	}
}
