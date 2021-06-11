// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package main

import (
	"context"
	gosql "database/sql"
	"os"
)

// clusterI is the interface through which a given roachtest interacts with the
// provisioned cloud hardware for a given test.
//
// This interface is currently crufty and unprincipled as it was extracted from
// code in which tests had direct access to the `cluster` type.
type clusterI interface {
	// Selecting nodes.

	All() nodeListOption
	Range(begin, end int) nodeListOption
	Nodes(ns ...int) nodeListOption
	Node(i int) nodeListOption

	// Uploading and downloading from/to nodes.

	Get(ctx context.Context, l *logger, src, dest string, opts ...option) error
	Put(ctx context.Context, src, dest string, opts ...option)
	PutE(ctx context.Context, l *logger, src, dest string, opts ...option) error
	PutLibraries(ctx context.Context, libraryDir string) error
	Stage(
		ctx context.Context, l *logger, application, versionOrSHA, dir string, opts ...option,
	) error
	PutString(
		ctx context.Context, content, dest string, mode os.FileMode, opts ...option,
	) error

	// Starting and stopping CockroachDB.

	StartE(ctx context.Context, opts ...option) error
	Start(ctx context.Context, t *test, opts ...option)
	StopE(ctx context.Context, opts ...option) error
	Stop(ctx context.Context, opts ...option)
	StopCockroachGracefullyOnNode(ctx context.Context, node int) error

	// Hostnames and IP addresses of the nodes.

	InternalAddr(ctx context.Context, node nodeListOption) ([]string, error)
	InternalIP(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalAddr(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalIP(ctx context.Context, node nodeListOption) ([]string, error)

	// SQL connection strings.

	InternalPGUrl(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalPGUrl(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalPGUrlSecure(
		ctx context.Context, node nodeListOption, user string, certsDir string, port int,
	) ([]string, error)

	// SQL clients to nodes.

	Conn(ctx context.Context, node int) *gosql.DB
	ConnE(ctx context.Context, node int) (*gosql.DB, error)
	ConnSecure(
		ctx context.Context, node int, user string, certsDir string, port int,
	) (*gosql.DB, error)

	// URLs for the Admin UI.

	InternalAdminUIAddr(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalAdminUIAddr(ctx context.Context, node nodeListOption) ([]string, error)

	// Running commands on nodes.

	Run(ctx context.Context, node nodeListOption, args ...string)
	RunE(ctx context.Context, node nodeListOption, args ...string) error
	RunL(ctx context.Context, l *logger, node nodeListOption, args ...string) error
	RunWithBuffer(
		ctx context.Context, l *logger, node nodeListOption, args ...string,
	) ([]byte, error)

	// Metadata about the provisioned nodes.

	Spec() clusterSpec
	Name() string
	isLocal() bool

	// Deleting CockroachDB data and logs on nodes.

	WipeE(ctx context.Context, l *logger, opts ...option) error
	Wipe(ctx context.Context, opts ...option)

	// Toggling encryption-at-rest settings for starting CockroachDB.

	EncryptAtRandom(on bool)
	EncryptDefault(on bool)

	// Internal niche tools.

	Reset(ctx context.Context) error
	Reformat(ctx context.Context, node nodeListOption, args ...string)
	Install(
		ctx context.Context, l *logger, node nodeListOption, args ...string,
	) error

	// Methods whose inclusion on this interface is purely historical.
	// These should be removed over time.

	makeNodes(opts ...option) string
	CheckReplicaDivergenceOnDB(context.Context, *test, *gosql.DB) error
	FetchDiskUsage(ctx context.Context, t *test) error
	GitClone(
		ctx context.Context, l *logger, src, dest, branch string, node nodeListOption,
	) error
}
