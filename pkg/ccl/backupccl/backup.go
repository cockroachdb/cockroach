// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	descpb "github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
)

// GetTenants retrieves the tenant information from the manifest. It should be
// used instead of Tenants to support older versions of the manifest which used
// the deprecated field.
func (m *BackupManifest) GetTenants() []descpb.TenantInfoWithUsage {
	if len(m.Tenants) > 0 {
		return m.Tenants
	}
	if len(m.TenantsDeprecated) > 0 {
		res := make([]descpb.TenantInfoWithUsage, len(m.TenantsDeprecated))
		for i := range res {
			res[i].TenantInfo = m.TenantsDeprecated[i]
		}
		return res
	}
	return nil
}

// HasTenants returns true if the manifest contains (non-system) tenant data.
func (m *BackupManifest) HasTenants() bool {
	return len(m.Tenants) > 0 || len(m.TenantsDeprecated) > 0
}

func init() {
	protoreflect.RegisterShorthands((*BackupManifest)(nil), "backup", "backup_manifest")
}
