// Copyright 2023 The Cockroach Authors.
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
	gosql "database/sql"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/prometheus"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/errors"
	"golang.org/x/net/context"
)

func nilLogger() *logger.Logger {
	lcfg := logger.Config{
		Stdout: io.Discard,
		Stderr: io.Discard,
	}
	l, err := lcfg.NewLogger("" /* path */)
	if err != nil {
		panic(err)
	}
	return l
}

func alwaysFailingClusterAllocator(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, error) {
	return nil, errors.New("cluster creation failed")
}

func alwaysSucceedingClusterAllocator(
	ctx context.Context,
	t registry.TestSpec,
	alloc *quotapool.IntAlloc,
	artifactsDir string,
	wStatus *workerStatus,
) (*clusterImpl, error) {
	res := clusterImpl{l: nilLogger(), expiration: (&spec.ClusterSpec{}).Expiration(), r: &clusterRegistry{}}
	return &res, nil
}

type mockCluster struct {
}

func (m mockCluster) All() option.NodeListOption {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Range(begin, end int) option.NodeListOption {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Nodes(ns ...int) option.NodeListOption {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Node(i int) option.NodeListOption {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Get(
	ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Put(ctx context.Context, src, dest string, opts ...option.Option) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) PutE(
	ctx context.Context, l *logger.Logger, src, dest string, opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) PutLibraries(
	ctx context.Context, libraryDir string, libraries []string,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Stage(
	ctx context.Context,
	l *logger.Logger,
	application, versionOrSHA, dir string,
	opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) PutString(
	ctx context.Context, content, dest string, mode os.FileMode, opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) StartE(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
	opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Start(
	ctx context.Context,
	l *logger.Logger,
	startOpts option.StartOpts,
	settings install.ClusterSettings,
	opts ...option.Option,
) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) StopE(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Stop(
	ctx context.Context, l *logger.Logger, stopOpts option.StopOpts, opts ...option.Option,
) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) StopCockroachGracefullyOnNode(
	ctx context.Context, l *logger.Logger, node int,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) NewMonitor(ctx context.Context, option ...option.Option) cluster.Monitor {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Extend(ctx context.Context, d time.Duration, l *logger.Logger) error {
	return nil
}

func (m mockCluster) InternalAddr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) InternalIP(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ExternalAddr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ExternalIP(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) InternalPGUrl(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ExternalPGUrl(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, tenant string,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Conn(
	ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption),
) *gosql.DB {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ConnE(
	ctx context.Context, l *logger.Logger, node int, opts ...func(*option.ConnOption),
) (*gosql.DB, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) InternalAdminUIAddr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) ExternalAdminUIAddr(
	ctx context.Context, l *logger.Logger, node option.NodeListOption,
) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) RunWithDetails(
	ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string,
) ([]install.RunResultDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Run(ctx context.Context, node option.NodeListOption, args ...string) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) RunE(ctx context.Context, node option.NodeListOption, args ...string) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) RunWithDetailsSingleNode(
	ctx context.Context, testLogger *logger.Logger, nodes option.NodeListOption, args ...string,
) (install.RunResultDetails, error) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Spec() spec.ClusterSpec {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Name() string {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) IsLocal() bool {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) IsSecure() bool {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) WipeE(ctx context.Context, l *logger.Logger, opts ...option.Option) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Wipe(ctx context.Context, opts ...option.Option) {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Reformat(
	ctx context.Context, l *logger.Logger, node option.NodeListOption, filesystem string,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) Install(
	ctx context.Context, l *logger.Logger, nodes option.NodeListOption, software ...string,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) MakeNodes(opts ...option.Option) string {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) GitClone(
	ctx context.Context, l *logger.Logger, src, dest, branch string, node option.NodeListOption,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) FetchTimeseriesData(ctx context.Context, l *logger.Logger) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) RefetchCertsFromNode(ctx context.Context, node int) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) StartGrafana(
	ctx context.Context, l *logger.Logger, promCfg *prometheus.Config,
) error {
	//TODO implement me
	panic("implement me")
}

func (m mockCluster) StopGrafana(ctx context.Context, l *logger.Logger, dumpDir string) error {
	//TODO implement me
	panic("implement me")
}
