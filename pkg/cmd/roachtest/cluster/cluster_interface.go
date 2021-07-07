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

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/logger"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
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
	PutLibraries(ctx context.Context, libraryDir string) error
	Stage(
		ctx context.Context, l *logger.Logger, application, versionOrSHA, dir string, opts ...option.Option,
	) error
	PutString(
		ctx context.Context, content, dest string, mode os.FileMode, opts ...option.Option,
	) error

	// Starting and stopping CockroachDB.

	StartE(ctx context.Context, opts ...option.Option) error
	Start(ctx context.Context, opts ...option.Option)
	StopE(ctx context.Context, opts ...option.Option) error
	Stop(ctx context.Context, opts ...option.Option)
	StopCockroachGracefullyOnNode(ctx context.Context, node int) error
	NewMonitor(context.Context, ...option.Option) Monitor

	// Hostnames and IP addresses of the nodes.

	InternalAddr(ctx context.Context, node option.NodeListOption) ([]string, error)
	InternalIP(ctx context.Context, node option.NodeListOption) ([]string, error)
	ExternalAddr(ctx context.Context, node option.NodeListOption) ([]string, error)
	ExternalIP(ctx context.Context, node option.NodeListOption) ([]string, error)

	// SQL connection strings.

	InternalPGUrl(ctx context.Context, node option.NodeListOption) ([]string, error)
	ExternalPGUrl(ctx context.Context, node option.NodeListOption) ([]string, error)
	ExternalPGUrlSecure(
		ctx context.Context, node option.NodeListOption, user string, certsDir string, port int,
	) ([]string, error)

	// SQL clients to nodes.

	Conn(ctx context.Context, node int) *gosql.DB
	ConnE(ctx context.Context, node int) (*gosql.DB, error)
	ConnSecure(
		ctx context.Context, node int, user string, certsDir string, port int,
	) (*gosql.DB, error)

	// URLs for the Admin UI.

	InternalAdminUIAddr(ctx context.Context, node option.NodeListOption) ([]string, error)
	ExternalAdminUIAddr(ctx context.Context, node option.NodeListOption) ([]string, error)

	// Running commands on nodes.

	Run(ctx context.Context, node option.NodeListOption, args ...string)
	RunE(ctx context.Context, node option.NodeListOption, args ...string) error
	RunL(ctx context.Context, l *logger.Logger, node option.NodeListOption, args ...string) error
	RunWithBuffer(
		ctx context.Context, l *logger.Logger, node option.NodeListOption, args ...string,
	) ([]byte, error)

	// Metadata about the provisioned nodes.

	Spec() spec.ClusterSpec
	Name() string
	IsLocal() bool

	// Deleting CockroachDB data and logs on nodes.

	WipeE(ctx context.Context, l *logger.Logger, opts ...option.Option) error
	Wipe(ctx context.Context, opts ...option.Option)

	// Toggling encryption-at-rest settings for starting CockroachDB.

	EncryptAtRandom(on bool)
	EncryptDefault(on bool)

	// Internal niche tools.

	Reset(ctx context.Context) error
	Reformat(ctx context.Context, node option.NodeListOption, args ...string)
	Install(
		ctx context.Context, node option.NodeListOption, args ...string,
	) error

	// Methods whose inclusion on this interface is purely historical.
	// These should be removed over time.

	MakeNodes(opts ...option.Option) string
	CheckReplicaDivergenceOnDB(context.Context, *logger.Logger, *gosql.DB) error
	GitClone(
		ctx context.Context, l *logger.Logger, src, dest, branch string, node option.NodeListOption,
	) error
}
