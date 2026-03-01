// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clusters

import (
	"fmt"
	"log/slog"
	"strings"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/aws"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/azure"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/ibm"
)

// validateProviderEnvironments checks that each real provider's identity
// string exists as a key in the providerEnvironments map. This catches
// config mismatches where the derived providerID (from config) does not
// match the actual provider.String() value. This is only meaningful in
// worker mode where real provider instances are available.
func validateProviderEnvironments(
	l *slog.Logger, providerEnvironments map[string]string, providers []vm.Provider,
) {
	for _, p := range providers {
		id := p.String()
		if _, ok := providerEnvironments[id]; !ok {
			l.Warn(
				"real provider identity does not match any derived environment mapping; "+
					"check that the cloud provider config matches the provider credentials",
				slog.String("providerID", id),
			)
		}
	}
}

// buildProviderEnvironments builds a mapping from providerID to environment
// name. The providerID is derived from the cloud provider config using
// deriveProviderID (e.g., "gce-my-project"), and the environment is the
// value configured in CloudProvider.Environment (e.g., "gcp-engineering").
// The environment name is used as the Scope value in the permission model;
// see checkClusterAccessPermission in api.go.
//
// In worker mode, callers should follow up with validateProviderEnvironments
// to verify that the derived IDs match the real provider instances.
func buildProviderEnvironments(
	l *slog.Logger, cloudProviders []configtypes.CloudProvider,
) map[string]string {
	result := make(map[string]string, len(cloudProviders))

	for i, cp := range cloudProviders {
		if cp.Environment == "" {
			l.Warn(
				"cloud provider has no environment configured; "+
					"clusters from this provider will not match any authorization scope",
				slog.String("type", cp.Type),
				slog.Int("index", i),
			)
			continue
		}

		providerID := deriveProviderID(cp)

		if providerID == "" {
			l.Warn(
				"unable to derive providerID for cloud provider; skipping environment mapping",
				slog.String("type", cp.Type),
				slog.Int("index", i),
			)
			continue
		}

		result[providerID] = cp.Environment
	}

	return result
}

// deriveProviderID replicates the vm.Provider.String() format for a
// CloudProvider config entry, allowing environment resolution without
// needing live cloud provider instances.
//
// The format for each provider type is:
//   - GCE:   "gce-<project>"
//   - AWS:   "aws-<accountID>"
//   - Azure: "azure-<subscriptionName>"
//   - IBM:   "ibm-<accountID>"
func deriveProviderID(cp configtypes.CloudProvider) string {
	switch strings.ToLower(cp.Type) {
	case gce.ProviderName:
		if cp.GCE.Project == "" {
			return ""
		}
		return fmt.Sprintf("%s-%s", gce.ProviderName, cp.GCE.Project)
	case aws.ProviderName:
		if cp.AWS.AccountID == "" {
			return ""
		}
		return fmt.Sprintf("%s-%s", aws.ProviderName, cp.AWS.AccountID)
	case azure.ProviderName:
		if cp.Azure.SubscriptionName == "" {
			return ""
		}
		return fmt.Sprintf("%s-%s", azure.ProviderName, cp.Azure.SubscriptionName)
	case ibm.ProviderName:
		if cp.IBM.AccountID == "" {
			return ""
		}
		return fmt.Sprintf("%s-%s", ibm.ProviderName, cp.IBM.AccountID)
	default:
		return ""
	}
}
