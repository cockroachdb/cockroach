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

type clusterI interface {
	isLocal() bool
	EncryptAtRandom(on bool)
	Spec() clusterSpec
	StopCockroachGracefullyOnNode(ctx context.Context, node int) error
	All() nodeListOption
	Range(begin, end int) nodeListOption
	Nodes(ns ...int) nodeListOption
	Node(i int) nodeListOption
	FetchDiskUsage(ctx context.Context) error
	Put(ctx context.Context, src, dest string, opts ...option)
	PutE(ctx context.Context, l *logger, src, dest string, opts ...option) error
	PutLibraries(ctx context.Context, libraryDir string) error
	Stage(
		ctx context.Context, l *logger, application, versionOrSHA, dir string, opts ...option,
	) error
	Get(ctx context.Context, l *logger, src, dest string, opts ...option) error
	PutString(
		ctx context.Context, content, dest string, mode os.FileMode, opts ...option,
	) error
	GitClone(
		ctx context.Context, l *logger, src, dest, branch string, node nodeListOption,
	) error
	StartE(ctx context.Context, opts ...option) error
	Start(ctx context.Context, t *test, opts ...option)
	StopE(ctx context.Context, opts ...option) error
	Stop(ctx context.Context, opts ...option)
	Reset(ctx context.Context) error
	WipeE(ctx context.Context, l *logger, opts ...option) error
	Wipe(ctx context.Context, opts ...option)
	Run(ctx context.Context, node nodeListOption, args ...string)
	Reformat(ctx context.Context, node nodeListOption, args ...string)
	Install(
		ctx context.Context, l *logger, node nodeListOption, args ...string,
	) error
	RunE(ctx context.Context, node nodeListOption, args ...string) error
	RunL(ctx context.Context, l *logger, node nodeListOption, args ...string) error
	RunWithBuffer(
		ctx context.Context, l *logger, node nodeListOption, args ...string,
	) ([]byte, error)
	InternalPGUrl(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalPGUrl(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalPGUrlSecure(
		ctx context.Context, node nodeListOption, user string, certsDir string, port int,
	) ([]string, error)
	InternalAdminUIAddr(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalAdminUIAddr(ctx context.Context, node nodeListOption) ([]string, error)
	InternalAddr(ctx context.Context, node nodeListOption) ([]string, error)
	InternalIP(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalAddr(ctx context.Context, node nodeListOption) ([]string, error)
	ExternalIP(ctx context.Context, node nodeListOption) ([]string, error)
	Conn(ctx context.Context, node int) *gosql.DB
	ConnE(ctx context.Context, node int) (*gosql.DB, error)
	ConnSecure(
		ctx context.Context, node int, user string, certsDir string, port int,
	) (*gosql.DB, error)
}
