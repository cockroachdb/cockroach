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
	showMeta(desc)
	showSpans(desc)
	showFiles(desc)
	showDescriptors(desc)
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

	w := tabwriter.NewWriter(os.Stdout, 28, 1, 2, ' ', 0)
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

func showMeta(desc backupccl.BackupManifest) {
	start := timeutil.Unix(0, desc.StartTime.WallTime).Format(time.RFC3339Nano)
	end := timeutil.Unix(0, desc.EndTime.WallTime).Format(time.RFC3339Nano)
	fmt.Printf("StartTime: %s (%s)\n", start, desc.StartTime)
	fmt.Printf("EndTime: %s (%s)\n", end, desc.EndTime)
	fmt.Printf("DataSize: %d (%s)\n", desc.EntryCounts.DataSize, humanizeutil.IBytes(desc.EntryCounts.DataSize))
	fmt.Printf("Rows: %d\n", desc.EntryCounts.Rows)
	fmt.Printf("IndexEntries: %d\n", desc.EntryCounts.IndexEntries)
	fmt.Printf("FormatVersion: %d\n", desc.FormatVersion)
	fmt.Printf("ClusterID: %s\n", desc.ClusterID)
	fmt.Printf("NodeID: %s\n", desc.NodeID)
	fmt.Printf("BuildInfo: %s\n", desc.BuildInfo.Short())
}

func showSpans(desc backupccl.BackupManifest) {
	fmt.Printf("Spans:\n")
	if len(desc.Spans) == 0 {
		fmt.Printf("	(No spans included in the specified backup path.)\n")
	}
	for _, s := range desc.Spans {
		fmt.Printf("	%s\n", s)
	}
}

func showFiles(desc backupccl.BackupManifest) {
	fmt.Printf("Files:\n")
	if len(desc.Files) == 0 {
		fmt.Printf("	(No sst files included in the specified backup path.)\n")
	}
	for _, f := range desc.Files {
		fmt.Printf("	%s:\n", f.Path)
		fmt.Printf("		Span: %s\n", f.Span)
		fmt.Printf("		DataSize: %d (%s)\n", f.EntryCounts.DataSize, humanizeutil.IBytes(f.EntryCounts.DataSize))
		fmt.Printf("		Rows: %d\n", f.EntryCounts.Rows)
		fmt.Printf("		IndexEntries: %d\n", f.EntryCounts.IndexEntries)
	}
}

func showDescriptors(desc backupccl.BackupManifest) {
	// Note that these descriptors could be from any past version of the cluster,
	// in case more fields need to be added to the output.
	dbIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	dbIDToName := make(map[descpb.ID]string)
	schemaIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	schemaIDs = append(schemaIDs, keys.PublicSchemaID)
	schemaIDToFullyQualifiedName := make(map[descpb.ID]string)
	schemaIDToFullyQualifiedName[keys.PublicSchemaID] = sessiondata.PublicSchemaName
	typeIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	typeIDToFullyQualifiedName := make(map[descpb.ID]string)
	tableIDs := make([]descpb.ID, 0, len(desc.Descriptors))
	tableIDToFullyQualifiedName := make(map[descpb.ID]string)
	for i := range desc.Descriptors {
		d := &desc.Descriptors[i]
		id := descpb.GetDescriptorID(d)
		tableDesc, databaseDesc, typeDesc, schemaDesc := descpb.FromDescriptor(d)
		if databaseDesc != nil {
			dbIDToName[id] = descpb.GetDescriptorName(d)
			dbIDs = append(dbIDs, id)
		} else if schemaDesc != nil {
			dbName := dbIDToName[schemaDesc.GetParentID()]
			schemaName := descpb.GetDescriptorName(d)
			schemaIDToFullyQualifiedName[id] = dbName + "." + schemaName
			schemaIDs = append(schemaIDs, id)
		} else if typeDesc != nil {
			parentSchema := schemaIDToFullyQualifiedName[typeDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[typeDesc.GetParentID()] + "." + parentSchema
			}
			typeName := descpb.GetDescriptorName(d)
			typeIDToFullyQualifiedName[id] = parentSchema + "." + typeName
			typeIDs = append(typeIDs, id)
		} else if tableDesc != nil {
			tbDesc := tabledesc.NewBuilder(tableDesc).BuildImmutable()
			parentSchema := schemaIDToFullyQualifiedName[tbDesc.GetParentSchemaID()]
			if parentSchema == sessiondata.PublicSchemaName {
				parentSchema = dbIDToName[tableDesc.GetParentID()] + "." + parentSchema
			}
			tableName := descpb.GetDescriptorName(d)
			tableIDToFullyQualifiedName[id] = parentSchema + "." + tableName
			tableIDs = append(tableIDs, id)
		}
	}

	fmt.Printf("Databases:\n")
	for _, id := range dbIDs {
		fmt.Printf("	%d: %s\n",
			id, dbIDToName[id])
	}

	fmt.Printf("Schemas:\n")
	for _, id := range schemaIDs {
		fmt.Printf("	%d: %s\n",
			id, schemaIDToFullyQualifiedName[id])
	}

	fmt.Printf("Types:\n")
	if len(typeIDs) == 0 {
		fmt.Printf("	(No user-defined types included in the specified backup path.)\n")
	}
	for _, id := range typeIDs {
		fmt.Printf("	%d: %s\n",
			id, typeIDToFullyQualifiedName[id])
	}

	fmt.Printf("Tables:\n")
	if len(tableIDs) == 0 {
		fmt.Printf("	(No tables included in the specified backup path.)\n")
	}
	for _, id := range tableIDs {
		fmt.Printf("	%d: %s\n",
			id, tableIDToFullyQualifiedName[id])
	}
}
