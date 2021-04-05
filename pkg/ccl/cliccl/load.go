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
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
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

var externalIODir string

func init() {

	loadShowSummaryCmd := &cobra.Command{
		Use:   "summary <backup_path>",
		Short: "show backups summary",
		Long:  "Shows summary of meta information about a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowSummary),
	}

	loadShowBackupsCmd := &cobra.Command{
		Use:   "backups <backup_path>",
		Short: "show backups in collections",
		Long:  "Shows full backups in a backup collections.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowBackups),
	}

	loadShowIncrementalCmd := &cobra.Command{
		Use:   "incremental <backup_path>",
		Short: "show incremental backups",
		Long:  "Shows incremental chain of a SQL backup.",
		Args:  cobra.ExactArgs(1),
		RunE:  cli.MaybeDecorateGRPCError(runLoadShowIncremental),
	}

	loadShowCmds := &cobra.Command{
		Use:   "show [command]",
		Short: "show backups",
		Long:  "Shows information about a SQL backup.",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	loadCmds := &cobra.Command{
		Use:   "load [command]",
		Short: "load backup commands",
		Long:  `Commands for bulk loading external files.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Usage()
		},
	}

	loadFlags := loadCmds.Flags()
	loadFlags.StringVarP(
		&externalIODir,
		cliflags.ExternalIODir.Name,
		cliflags.ExternalIODir.Shorthand,
		"", /*value*/
		cliflags.ExternalIODir.Usage())

	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadShowCmds)
	loadShowCmds.AddCommand([]*cobra.Command{
		loadShowSummaryCmd,
		loadShowBackupsCmd,
		loadShowIncrementalCmd,
	}...)
	loadShowSummaryCmd.Flags().AddFlagSet(loadFlags)
	loadShowBackupsCmd.Flags().AddFlagSet(loadFlags)
	loadShowIncrementalCmd.Flags().AddFlagSet(loadFlags)
}

func newBlobFactory(ctx context.Context, dialing roachpb.NodeID) (blobs.BlobClient, error) {
	if dialing != 0 {
		return nil, errors.Errorf("accessing node %d during nodelocal access is unsupported for CLI inspection; only local access is supported with nodelocal://self", dialing)
	}
	if externalIODir == "" {
		externalIODir = filepath.Join(server.DefaultStorePath, "extern")
	}
	return blobs.NewLocalClient(externalIODir)
}

func runLoadShowSummary(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}

	ctx := context.Background()
	externalStorageFromURI := func(ctx context.Context, uri string,
		user security.SQLUsername) (cloud.ExternalStorage, error) {
		return cloudimpl.ExternalStorageFromURI(ctx, uri, base.ExternalIODirConfig{},
			cluster.NoSettings, newBlobFactory, user, nil /*Internal Executor*/, nil /*kvDB*/)
	}

	// This reads the raw backup descriptor (with table descriptors possibly not
	// upgraded from the old FK representation, or even older formats). If more
	// fields are added to the output, the table descriptors may need to be
	// upgraded.
	desc, err := backupccl.ReadBackupManifestFromURI(ctx, path, security.RootUserName(),
		externalStorageFromURI, nil)
	if err != nil {
		return err
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

func runLoadShowBackups(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}
	ctx := context.Background()
	store, err := cloudimpl.ExternalStorageFromURI(ctx, path, base.ExternalIODirConfig{},
		cluster.NoSettings, newBlobFactory, security.RootUserName(), nil /*Internal Executor*/, nil /*kvDB*/)
	if err != nil {
		return errors.Wrapf(err, "connect to external storage")
	}
	defer store.Close()

	backupPaths, err := backupccl.ListFullBackupsInCollection(ctx, store)
	if err != nil {
		return errors.Wrapf(err, "list full backups in collection")
	}

	if len(backupPaths) == 0 {
		fmt.Println("no backups found.")
	}

	for _, backupPath := range backupPaths {
		fmt.Println("./" + backupPath)
	}

	return nil
}

func runLoadShowIncremental(cmd *cobra.Command, args []string) error {

	path := args[0]
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}

	uri, err := url.Parse(path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store, err := cloudimpl.ExternalStorageFromURI(ctx, uri.String(), base.ExternalIODirConfig{},
		cluster.NoSettings, newBlobFactory, security.RootUserName(), nil /*Internal Executor*/, nil /*kvDB*/)
	if err != nil {
		return errors.Wrapf(err, "connect to external storage")
	}
	defer store.Close()

	incPaths, err := backupccl.FindPriorBackupLocations(ctx, store)
	if err != nil {
		return err
	}

	w := tabwriter.NewWriter(os.Stdout, 28 /*minwidth*/, 1 /*tabwidth*/, 2 /*padding*/, ' ' /*padchar*/, 0 /*flags*/)
	basepath := uri.Path
	manifestPaths := append([]string{""}, incPaths...)
	stores := make([]cloud.ExternalStorage, len(manifestPaths))
	stores[0] = store

	for i := range manifestPaths {

		if i > 0 {
			uri.Path = filepath.Join(basepath, manifestPaths[i])
			stores[i], err = cloudimpl.ExternalStorageFromURI(ctx, uri.String(), base.ExternalIODirConfig{},
				cluster.NoSettings, newBlobFactory, security.RootUserName(), nil /*Internal Executor*/, nil /*kvDB*/)
			if err != nil {
				return errors.Wrapf(err, "connect to external storage")
			}
			defer stores[i].Close()
		}

		manifest, err := backupccl.ReadBackupManifestFromStore(ctx, stores[i], nil)
		if err != nil {
			return err
		}
		startTime := manifest.StartTime.GoTime().Format(time.RFC3339)
		endTime := manifest.EndTime.GoTime().Format(time.RFC3339)
		if i == 0 {
			startTime = "-"
		}
		fmt.Fprintf(w, "%s	%s	%s\n", uri.Path, startTime, endTime)
	}

	if err := w.Flush(); err != nil {
		return err
	}
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
		DataBaseDescriptors map[descpb.ID]string
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
		DataBaseDescriptors: make(map[descpb.ID]string),
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

	displayMsg.DataBaseDescriptors = dbIDToName
	displayMsg.TableDescriptors = tableIDToFullyQualifiedName
	displayMsg.SchemaDescriptors = schemaIDToFullyQualifiedName
	displayMsg.TypeDescriptors = typeIDToFullyQualifiedName

	return json.Marshal(displayMsg)
}
