// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cluster

import (
	"context"
	gosql "database/sql"
	"os"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// Cluster is the interface through which a given roachtest interacts with the
// provisioned cloud hardware for a given test.
//
// This interface is currently crufty and unprincipled as it was extracted from
// code in which tests had direct access to the `cluster` type.
type Cluster interface {
	// Selecting nodes.

	All() option.NodeListOption
	Range(begin, end int) option.NodeListOption
	Nodes(ns ...int) option.NodeListOption
	Node(i int) option.NodeListOption

	// Uploading and downloading from/to nodes.

	Get(ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option) error
	Put(ctx context.Context, src, dest string, opts ...option.Option)
	PutE(ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option) error
	PutLibraries(ctx context.Context, libraryDir string, libraries []string) error
	Stage(
		ctx context.Context, l *logger.Logger, application, versionOrSHA, dir string, opts ...option.Option,
	) error
	PutString(
		ctx context.Context, content, dest string, mode os.FileMode, opts ...option.Option,
	) error

	// Starting and stopping CockroachDB.

	StartE(ctx context.Context, l *logger.Logger, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option) error
	Start(ctx context.Context, l *logger.Logger, startOpts option.StartOpts, settings install.ClusterSettings, opts ...option.Option)
	StopE(ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option) error
	Stop(ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option)
	SignalE(ctx context.Context, l *logger.Logger, sig int, opts ...option.Option) error
	Signal(ctx context.Context, l *logger.Logger, sig int, opts ...option.Option)
	StopCockroachGracefullyOnNode(ctx context.Context, l *logger.Logger, node int) error
	NewMonitor(context.Context, ...option.Option) Monitor

	// Hostnames and IP addresses of the nodes.

	InternalAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)
	InternalIP(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)
	ExternalAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)
	ExternalIP(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)

	// SQL connection strings.

	InternalPGUrl(ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string) ([]string, error)
	ExternalPGUrl(ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string) ([]string, error)

	// SQL clients to nodes.
	Conn(ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption)) *gosql.DB
	ConnE(ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption)) (*gosql.DB, error)

	// URLs for the Admin UI.

	InternalAdminUIAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)
	ExternalAdminUIAddr(ctx context.Context, l *logger.Logger, node option.NodeListOption) ([]string, error)

	// Running commands on nodes.

	// RunWithDetails runs a command on the specified nodes and returns results details and an error.
	// The returned error is only for a major failure in roachprod run command so the caller needs
	// to check for individual node errors in `[]install.RunResultDetails`.
	// Use it when you need output details such as stdout or stderr, or remote exit status.
	RunWithDetails(ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string) ([]install.RunResultDetails, error)

	// Run is fatal on errors.
	// Use it when an error means the test should fail.
	Run(ctx context.Context, node option.NodeListOption, args ...string)

	// RunE runs a command on the specified nodes and returns an error.
	// Use it when you need to run a command and only care if it ran successfully or not.
	RunE(ctx context.Context, node option.NodeListOption, args ...string) error

	// RunWithDetailsSingleNode is just like RunWithDetails but used when 1) operating
	// on a single node AND 2) an error from roachprod itself would be treated the same way
	// you treat an error from the command. This makes error checking easier / friendlier
	// and helps us avoid code replication.
	RunWithDetailsSingleNode(ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string) (install.RunResultDetails, error)

	// Metadata about the provisioned nodes.

	Spec() spec.ClusterSpec
	Name() string
	IsLocal() bool
	// IsSecure returns true iff the cluster uses TLS.
	IsSecure() bool
	// Returns CPU architecture of the nodes.
	Architecture() vm.CPUArch

	// Deleting CockroachDB data and logs on nodes.

	WipeE(ctx context.Context, l *logger.Logger, preserveCerts bool, opts ...option.Option) error
	Wipe(ctx context.Context, preserveCerts bool, opts ...option.Option)

	// Internal niche tools.

	Reformat(ctx context.Context, l *logger.Logger, node option.NodeListOption, filesystem string) error
	Install(
		ctx context.Context, l *logger.Logger, nodes option.NodeListOption, software ...string,
	) error

	// Methods whose inclusion on this interface is purely historical.
	// These should be removed over time.

	MakeNodes(opts ...option.Option) string
	GitClone(
		ctx context.Context, l *logger.Logger, src, dest, branch string, node option.NodeListOption,
	) error

	FetchTimeseriesData(ctx context.Context, l *logger.Logger) error
	FetchDebugZip(ctx context.Context, l *logger.Logger, dest string) error
	RefetchCertsFromNode(ctx context.Context, node int) error

	StartGrafana(ctx context.Context, l *logger.Logger, promCfg *prometheus.Config) error
	StopGrafana(ctx context.Context, l *logger.Logger, dumpDir string) error

	// Volume snapshot related APIs.
	//
	// NB: The --local case for these snapshot APIs are that they all no-op. But
	// it should be transparent to roachtests. The assumption this interface
	// makes is that the calling roachtest that needed to CreateSnapshot will
	// proceed with then using the already populated disks. Disks that it
	// populated having not found any existing snapshots. So --local runs don't
	// rely on the remaining snapshot methods to actually do anything.

	// CreateSnapshot creates volume snapshots of the cluster using the given
	// prefix. These snapshots can later be retrieved, deleted or applied to
	// already instantiated clusters.
	//
	CreateSnapshot(ctx context.Context, snapshotPrefix string) ([]vm.VolumeSnapshot, error)
	// ListSnapshots lists the individual volume snapshots that satisfy the
	// search criteria.
	ListSnapshots(ctx context.Context, vslo vm.VolumeSnapshotListOpts) ([]vm.VolumeSnapshot, error)
	// DeleteSnapshots permanently deletes the given snapshots.
	DeleteSnapshots(ctx context.Context, snapshots ...vm.VolumeSnapshot) error
	// ApplySnapshots applies the given volume snapshots to the underlying
	// cluster. This is a destructive operation as far as existing state is
	// concerned - all already-attached volumes are detached and deleted to make
	// room for new snapshot-derived volumes. The new volumes are created using
	// the same specs (size, disk type, etc.) as the original cluster.
	//
	// TODO(irfansharif): The implementation tacitly assumes one volume
	// per-node, but this could be changed. Another assumption is that all
	// volumes are created identically.
	ApplySnapshots(ctx context.Context, snapshots []vm.VolumeSnapshot) error
}
