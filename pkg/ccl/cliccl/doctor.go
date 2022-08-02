// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backupinfo"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl/backuppb"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/doctor"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func init() {
	cli.DoctorFromBackupCommand = fromBackup
}

// fromBackup fetches descriptor information and synthesizes backup/restore
// information from a backup image.
func fromBackup(
	backupURI string, externalIODir string,
) (
	descTable doctor.DescriptorTable,
	namespaceTable doctor.NamespaceTable,
	jobsTable doctor.JobsTable,
	retErr error,
) {
	ctx := context.Background()
	debugBackupArgs.externalIODir = externalIODir
	manifest, err := getManifestFromURI(ctx, backupURI)
	if err != nil {
		return nil, nil, nil, err
	}

	descriptors, _ := backupinfo.LoadSQLDescsFromBackupsAtTime([]backuppb.BackupManifest{manifest}, hlc.Timestamp{})

	for _, desc := range descriptors {
		builder := desc.NewBuilder()
		mutDesc := builder.BuildCreatedMutable()
		bytes, err := protoutil.Marshal(mutDesc.DescriptorProto())
		if err != nil {
			return nil, nil, nil, err
		}
		descTable = append(descTable,
			doctor.DescriptorTableRow{
				ID:        int64(desc.GetID()),
				DescBytes: bytes,
				ModTime:   desc.GetModificationTime(),
			})
		namespaceTable = append(namespaceTable,
			doctor.NamespaceTableRow{
				ID: int64(desc.GetID()),
				NameInfo: descpb.NameInfo{
					Name:           desc.GetName(),
					ParentID:       desc.GetParentID(),
					ParentSchemaID: desc.GetParentSchemaID(),
				},
			})
	}
	return descTable, namespaceTable, nil, nil
}
