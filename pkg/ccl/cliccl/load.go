// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

func init() {
	loadShowCmd := &cobra.Command{
		Use:   "show <basepath>",
		Short: "show backups",
		Long:  "Shows information about a SQL backup.",
		RunE:  cli.MaybeDecorateGRPCError(runLoadShow),
	}

	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "loading commands",
		Long:  `Commands for bulk loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}
	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadShowCmd)
}

func runLoadShow(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("basepath argument is required")
	}

	ctx := context.Background()
	basepath := args[0]
	if !strings.Contains(basepath, "://") {
		basepath = cloudimpl.MakeLocalStorageURI(basepath)
	}

	externalStorageFromURI := func(ctx context.Context, uri string,
		user security.SQLUsername) (cloud.ExternalStorage, error) {
		return cloudimpl.ExternalStorageFromURI(ctx, uri, base.ExternalIODirConfig{},
			cluster.NoSettings, blobs.TestEmptyBlobClientFactory, user, nil, nil)
	}
	// This reads the raw backup descriptor (with table descriptors possibly not
	// upgraded from the old FK representation, or even older formats). If more
	// fields are added to the output, the table descriptors may need to be
	// upgraded.
	desc, err := backupccl.ReadBackupManifestFromURI(ctx, basepath, security.RootUserName(),
		externalStorageFromURI, nil)
	if err != nil {
		return errors.Wrapf(err, "fetching backup manifest")
	}

	var meta = backupMetaDisplayMsg(desc)
	jsonBytes, err := json.MarshalIndent(meta, "" /*prefix*/, "\t" /*indent*/)
	if err != nil {
		return errors.Wrapf(err, "marshall backup manifest")
	}
	s := string(jsonBytes)
	fmt.Println(s)
	return nil
}

type backupMetaDisplayMsg backupccl.BackupManifest
type backupFileDisplayMsg backupccl.BackupManifest_File

func (f backupFileDisplayMsg) MarshalJSON() ([]byte, error) {
	fileDisplayMsg := struct {
		Path         string
		Span         string
		DataSize     string
		IndexEntries int64
		Rows         int64
	}{
		Path:         f.Path,
		Span:         fmt.Sprint(f.Span),
		DataSize:     humanizeutil.IBytes(f.EntryCounts.DataSize),
		IndexEntries: f.EntryCounts.IndexEntries,
		Rows:         f.EntryCounts.Rows,
	}
	return json.Marshal(fileDisplayMsg)
}

func (b backupMetaDisplayMsg) MarshalJSON() ([]byte, error) {

	fileMsg := make([]backupFileDisplayMsg, len(b.Files))
	for i, file := range b.Files {
		fileMsg[i] = backupFileDisplayMsg(file)
	}

	displayMsg := struct {
		StartTime           string
		EndTime             string
		DataSize            string
		Rows                int64
		IndexEntries        int64
		FormatVersion       uint32
		ClusterID           uuid.UUID
		NodeID              roachpb.NodeID
		BuildInfo           string
		Files               []backupFileDisplayMsg
		Spans               string
		DatabaseDescriptors map[descpb.ID]string
		TableDescriptors    map[descpb.ID]string
		TypeDescriptors     map[descpb.ID]string
		SchemaDescriptors   map[descpb.ID]string
	}{
		StartTime:           timeutil.Unix(0, b.StartTime.WallTime).Format(time.RFC3339),
		EndTime:             timeutil.Unix(0, b.EndTime.WallTime).Format(time.RFC3339),
		DataSize:            humanizeutil.IBytes(b.EntryCounts.DataSize),
		Rows:                b.EntryCounts.Rows,
		IndexEntries:        b.EntryCounts.IndexEntries,
		FormatVersion:       b.FormatVersion,
		ClusterID:           b.ClusterID,
		NodeID:              b.NodeID,
		BuildInfo:           b.BuildInfo.Short(),
		Files:               fileMsg,
		Spans:               fmt.Sprint(b.Spans),
		DatabaseDescriptors: make(map[descpb.ID]string),
		TableDescriptors:    make(map[descpb.ID]string),
		TypeDescriptors:     make(map[descpb.ID]string),
		SchemaDescriptors:   make(map[descpb.ID]string),
	}

	dbIDToName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName[keys.PublicSchemaID] = sessiondata.PublicSchemaName
	typeIDToFullyQualifiedName := make(map[descpb.ID]string)
	tableIDToFullyQualifiedName := make(map[descpb.ID]string)

	for i := range b.Descriptors {
		d := &b.Descriptors[i]
		id := descpb.GetDescriptorID(d)
		tableDesc, databaseDesc, typeDesc, schemaDesc := descpb.FromDescriptor(d)
		if databaseDesc != nil {
			dbIDToName[id] = descpb.GetDescriptorName(d)
		} else if schemaDesc != nil {
			dbName := dbIDToName[schemaDesc.GetParentID()]
			schemaName := descpb.GetDescriptorName(d)
			schemaIDToFullyQualifiedName[id] = dbName + "." + schemaName
		} else if typeDesc != nil {
			parentSchema := schemaIDToFullyQualifiedName[typeDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[typeDesc.GetParentID()] + "." + parentSchema
			}
			typeName := descpb.GetDescriptorName(d)
			typeIDToFullyQualifiedName[id] = parentSchema + "." + typeName
		} else if tableDesc != nil {
			tbDesc := tabledesc.NewBuilder(tableDesc).BuildImmutable()
			parentSchema := schemaIDToFullyQualifiedName[tbDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[tableDesc.GetParentID()] + "." + parentSchema
			}
			tableName := descpb.GetDescriptorName(d)
			tableIDToFullyQualifiedName[id] = parentSchema + "." + tableName
		}
	}
	displayMsg.DatabaseDescriptors = dbIDToName
	displayMsg.TableDescriptors = tableIDToFullyQualifiedName
	displayMsg.SchemaDescriptors = schemaIDToFullyQualifiedName
	displayMsg.TypeDescriptors = typeIDToFullyQualifiedName

	return json.Marshal(displayMsg)
}
