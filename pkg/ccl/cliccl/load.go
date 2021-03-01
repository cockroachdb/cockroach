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
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/backupccl"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/cli/cliflags"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/storage/cloud"
	"github.com/cockroachdb/cockroach/pkg/storage/cloudimpl"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	descriptors = "descriptors"
	files       = "files"
	spans       = "spans"
	metadata    = "metadata"
)

var externalIODir string

func init() {
	loadShowCmd := &cobra.Command{
		Use:   "show [descriptors|files|spans|metadata] <backup_path>",
		Short: "show backups",
		Long:  "Shows subset(s) of meta information about a SQL backup.",
		Args:  cobra.MinimumNArgs(1),
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

	f := loadCmds.Flags()
	f.StringVarP(
		&externalIODir,
		cliflags.ExternalIODir.Name,
		cliflags.ExternalIODir.Shorthand,
		"", /*value*/
		cliflags.ExternalIODir.Usage())

	cli.AddCmd(loadCmds)
	loadCmds.AddCommand(loadShowCmd)
}

func newBlobFactory(ctx context.Context, dialing roachpb.NodeID) (blobs.BlobClient, error) {
	if dialing != 0 {
		return nil, errors.Errorf(`only support nodelocal (0/self) under offline inspection`)
	}
	if externalIODir == "" {
		externalIODir = filepath.Join(server.DefaultStorePath, "extern")
	}
	return blobs.NewLocalClient(externalIODir)
}

func parseShowArgs(args []string) (options map[string]bool, path string, err error) {
	options = make(map[string]bool)
	for _, arg := range args {
		switch strings.ToLower(arg) {
		case descriptors:
			options[descriptors] = true
		case files:
			options[files] = true
		case spans:
			options[spans] = true
		case metadata:
			options[metadata] = true
		default:
			if path != "" {
				return nil, "", errors.New("more than one path is specifiied")
			}
			path = arg
		}
	}

	if len(options) == 0 {
		options[descriptors] = true
		options[files] = true
		options[spans] = true
		options[metadata] = true
	}

	if len(args) == len(options) {
		return nil, "", errors.New("backup_path argument is required")
	}
	return options, path, nil
}

func runLoadShow(cmd *cobra.Command, args []string) error {

	var options map[string]bool
	var path string
	var err error
	if options, path, err = parseShowArgs(args); err != nil {
		return err
	}

	var showHeaders bool
	if len(options) > 1 {
		showHeaders = true
	}

	ctx := context.Background()
	if !strings.Contains(path, "://") {
		path = cloudimpl.MakeLocalStorageURI(path)
	}

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

	if _, ok := options[metadata]; ok {
		showMeta(desc)
	}

	if _, ok := options[spans]; ok {
		showSpans(desc, showHeaders)
	}

	if _, ok := options[files]; ok {
		showFiles(desc, showHeaders)
	}

	if _, ok := options[descriptors]; ok {
		err = showDescriptor(desc)
		if err != nil {
			return err
		}
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

func showSpans(desc backupccl.BackupManifest, showHeaders bool) {
	tabfmt := ""
	if showHeaders {
		fmt.Printf("Spans:\n")
		tabfmt = "\t"
	}
	for _, s := range desc.Spans {
		fmt.Printf("%s%s\n", tabfmt, s)
	}
}

func showFiles(desc backupccl.BackupManifest, showHeaders bool) {
	tabfmt := ""
	if showHeaders {
		fmt.Printf("Files:\n")
		tabfmt = "\t"
	}
	for _, f := range desc.Files {
		fmt.Printf("%s%s:\n", tabfmt, f.Path)
		fmt.Printf("	Span: %s\n", f.Span)
		fmt.Printf("	Sha512: %0128x\n", f.Sha512)
		fmt.Printf("	DataSize: %d (%s)\n", f.EntryCounts.DataSize, humanizeutil.IBytes(f.EntryCounts.DataSize))
		fmt.Printf("	Rows: %d\n", f.EntryCounts.Rows)
		fmt.Printf("	IndexEntries: %d\n", f.EntryCounts.IndexEntries)
	}
}

func showDescriptor(desc backupccl.BackupManifest) error {
	// Note that these descriptors could be from any past version of the cluster,
	// in case more fields need to be added to the output.
	dbIDToName := make(map[descpb.ID]string)
	for i := range desc.Descriptors {
		d := &desc.Descriptors[i]
		id := descpb.GetDescriptorID(d)
		if d.GetDatabase() != nil {
			dbIDToName[id] = descpb.GetDescriptorName(d)
		}
	}

	fmt.Printf("Databases:\n")
	for i := range dbIDToName {
		fmt.Printf("	%s\n",
			dbIDToName[i])
	}

	fmt.Printf("Tables:\n")
	for i := range desc.Descriptors {
		d := &desc.Descriptors[i]
		if descpb.TableFromDescriptor(d, hlc.Timestamp{}) != nil {
			tbDesc := tabledesc.NewImmutable(*descpb.TableFromDescriptor(d, hlc.Timestamp{}))
			dbName := dbIDToName[tbDesc.GetParentID()]
			fmt.Printf("	%s (%s) \n", descpb.GetDescriptorName(d), dbName)
			err := showCreate(tbDesc, desc.Descriptors, dbName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func showCreate(desc catalog.TableDescriptor, allDescs []descpb.Descriptor, dbName string) error {

	fakeNodeInfo := sql.NodeInfo{
		AdminURL:  func() *url.URL { return nil },
		PGURL:     func(*url.Userinfo) (*url.URL, error) { return nil, nil },
		ClusterID: func() uuid.UUID { return uuid.UUID{} },
		NodeID:    nil,
	}

	p, _ := sql.NewInternalPlanner(
		"showCreateInCli",
		nil, /*kv.Txn*/
		security.RootUserName(),
		&sql.MemoryMetrics{},
		&sql.ExecutorConfig{
			RPCContext: nil,
			NodeInfo:   fakeNodeInfo,
		},
		sessiondatapb.SessionData{},
	)

	displayOptions := sql.ShowCreateDisplayOptions{
		FKDisplayMode:  sql.OmitMissingFKClausesFromCreate,
		IgnoreComments: true,
	}

	createStmt, err := p.(sql.PlanHookState).ShowCreate(
		context.Background(),
		dbName,
		allDescs,
		desc,
		displayOptions)
	if err != nil {
		return err
	}

	createStmt = "\t\t" + createStmt
	createStmt = strings.ReplaceAll(createStmt, "\n", "\n\t\t")
	fmt.Println(createStmt)
	return nil
}
