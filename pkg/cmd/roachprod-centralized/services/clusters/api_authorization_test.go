// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"testing"

	pkgauth "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/auth"
	authmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/stretchr/testify/assert"
)

func TestCheckClusterAccessPermission(t *testing.T) {
	tests := []struct {
		name               string
		principal          *pkgauth.Principal
		cluster            *cloudcluster.Cluster
		requiredPermission string
		expected           bool
	}{
		{
			name: "user with all:view permission can access any cluster",
			principal: &pkgauth.Principal{
				User: &authmodels.User{Email: "other@example.com"},
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Provider:   "gcp",
						Account:    "project1",
						Permission: "clusters:view:all",
					},
				},
			},
			cluster: &cloudcluster.Cluster{
				User:           "test@example.com",
				CloudProviders: []string{"gcp-project1"},
			},
			requiredPermission: "clusters:view",
			expected:           true,
		},
		{
			name: "owner with own:view permission can access their cluster",
			principal: &pkgauth.Principal{
				User: &authmodels.User{Email: "test@example.com"},
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Provider:   "gcp",
						Account:    "project1",
						Permission: "clusters:view:own",
					},
				},
			},
			cluster: &cloudcluster.Cluster{
				User:           "test@example.com",
				CloudProviders: []string{"gcp-project1"},
			},
			requiredPermission: "clusters:view",
			expected:           true,
		},
		{
			name: "non-owner with only own:view permission cannot access",
			principal: &pkgauth.Principal{
				User: &authmodels.User{Email: "other@example.com"},
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Provider:   "gcp",
						Account:    "project1",
						Permission: "clusters:view:own",
					},
				},
			},
			cluster: &cloudcluster.Cluster{
				User:           "test@example.com",
				CloudProviders: []string{"gcp-project1"},
			},
			requiredPermission: "clusters:view",
			expected:           false,
		},
		{
			name: "user without permission for provider cannot access",
			principal: &pkgauth.Principal{
				User: &authmodels.User{Email: "test@example.com"},
				Permissions: []authmodels.Permission{
					&authmodels.UserPermission{
						Provider:   "aws",
						Account:    "account1",
						Permission: "clusters:view:all",
					},
				},
			},
			cluster: &cloudcluster.Cluster{
				User:           "test@example.com",
				CloudProviders: []string{"gcp-project1"},
			},
			requiredPermission: "clusters:view",
			expected:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := checkClusterAccessPermission(tt.principal, tt.cluster, tt.requiredPermission)
			assert.Equal(t, tt.expected, result)
		})
	}
}
