// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package configccl

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ValidateLeasePreferences ensures that lease placement preferences are
// allowed in the cluster and that they are well-formed.
func ValidateLeasePreferences(
	st *cluster.Settings, clusterID uuid.UUID, z config.ZoneConfig,
) error {
	if !st.Version.IsMinSupported(cluster.VersionPartitioning) {
		return errors.New(
			"cluster version does not support zone configs with lease placement preferences")
	}

	org := sql.ClusterOrganization.Get(&st.SV)
	if err := utilccl.CheckEnterpriseEnabled(st, clusterID, org, "lease preferences"); err != nil {
		return err
	}
	return validateLeasePreferencesImpl(z)
}

func validateLeasePreferencesImpl(z config.ZoneConfig) error {
	for _, s := range z.Subzones {
		if err := validateLeasePreferencesImpl(s.Config); err != nil {
			return err
		}
	}

	for _, leasePref := range z.LeasePreferences {
		if len(leasePref.Constraints) == 0 {
			return fmt.Errorf("every lease preference must include at least one constraint")
		}
		for _, constraint := range leasePref.Constraints {
			if constraint.Type == config.Constraint_DEPRECATED_POSITIVE {
				return fmt.Errorf("lease preference constraints must either be required " +
					"(prefixed with a '+') or prohibited (prefixed with a '-')")
			}
		}
	}

	return nil
}

func init() {
	sql.ValidateLeasePreferences = ValidateLeasePreferences
}
