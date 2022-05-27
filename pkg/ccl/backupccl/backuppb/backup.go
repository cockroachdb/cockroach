// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backuppb

import (
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	_ "github.com/cockroachdb/cockroach/pkg/util/uuid" // required for backup.proto
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
)

// IsIncremental returns if the BackupManifest corresponds to an incremental
// backup.
func (m *BackupManifest) IsIncremental() bool {
	return !m.StartTime.IsEmpty()
}

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

// MarshalJSONPB implements jsonpb.JSONPBMarshaller to provide a custom Marshaller
// for jsonpb that redacts secrets in URI fields.
func (m ScheduledBackupExecutionArgs) MarshalJSONPB(marshaller *jsonpb.Marshaler) ([]byte, error) {
	if !protoreflect.ShouldRedact(marshaller) {
		return json.Marshal(m)
	}

	stmt, err := parser.ParseOne(m.BackupStatement)
	if err != nil {
		return nil, err
	}
	backup, ok := stmt.AST.(*tree.Backup)
	if !ok {
		return nil, errors.Errorf("unexpected %T statement in backup schedule: %v", backup, backup)
	}

	for i := range backup.To {
		raw, ok := backup.To[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.To[i] = tree.NewDString(clean)
	}

	// NB: this will never be non-nil with current schedule syntax but is here for
	// completeness.
	for i := range backup.IncrementalFrom {
		raw, ok := backup.IncrementalFrom[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.IncrementalFrom[i] = tree.NewDString(clean)
	}

	for i := range backup.Options.IncrementalStorage {
		raw, ok := backup.Options.IncrementalStorage[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.SanitizeExternalStorageURI(raw.RawString(), nil /* extraParams */)
		if err != nil {
			return nil, err
		}
		backup.Options.IncrementalStorage[i] = tree.NewDString(clean)
	}

	for i := range backup.Options.EncryptionKMSURI {
		raw, ok := backup.Options.EncryptionKMSURI[i].(*tree.StrVal)
		if !ok {
			return nil, errors.Errorf("unexpected %T arg in backup schedule: %v", raw, raw)
		}
		clean, err := cloud.RedactKMSURI(raw.RawString())
		if err != nil {
			return nil, err
		}
		backup.Options.EncryptionKMSURI[i] = tree.NewDString(clean)
	}

	if backup.Options.EncryptionPassphrase != nil {
		backup.Options.EncryptionPassphrase = tree.NewDString("redacted")
	}

	m.BackupStatement = backup.String()
	return json.Marshal(m)
}

func init() {
	protoreflect.RegisterShorthands((*BackupManifest)(nil), "backup", "backup_manifest")
}
