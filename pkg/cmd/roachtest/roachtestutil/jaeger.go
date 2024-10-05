// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
//

package roachtestutil

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
)

// InstallLaunchAndConfigureJaegerAllInOne sets up jaeger on n1 and changes the cluster settings
// so that tracing is globally on and directed at the jaeger collector. The UI can be accessed
// on n1's port 16686. The jaeger data will be stored under n1's logs, so it will be collected
// as part of the artifacts should the build fail, and jaeger-all-in-one can be used to look
// at the collected data.
//
// Tracing is expensive, this command downloads a large archive from github, and
// the collector and UI both use CPU and RAM. In other words, don't use this in
// tests that have no headroom available. As a general rule, this should also
// only be used in manual testing (i.e. calls to this method should not be
// checked in). As a warning, large searches can crash jaeger which will then
// fail the running test - so tread carefully. Selecting a single service should
// work, though.
//
// The jaeger datasets are large (10+GiB per hour) and are kept on CockroachDB's
// data partition. However, they are symlinked into the logs directory so that
// they will be retrieved for failed tests.
func InstallLaunchAndConfigureJaegerAllInOne(
	ctx context.Context, l *logger.Logger, c cluster.Cluster, m cluster.Monitor,
) error {
	if c.IsLocal() {
		// Don't bother getting this right locally; most of us are on darwin.
		// It can be done if someone feels strongly.
		return nil
	}
	// Don't install under `m.Go`, we want this method to return only when the collector is
	// almost (and ideally fully, but that's harder) ready.
	if err := c.RunE(ctx, option.WithNodes(c.Node(1)), `[ -f jaeger-all-in-one ] ||
curl -sSL https://github.com/jaegertracing/jaeger/releases/download/v1.22.0/jaeger-1.22.0-linux-amd64.tar.gz |
tar --strip-components=1 -xvzf -`); err != nil {
		return err
	}
	m.Go(func(ctx context.Context) error {
		err := c.RunE(ctx, option.WithNodes(c.Node(1)), `SPAN_STORAGE_TYPE=badger BADGER_EPHEMERAL=false \
BADGER_DIRECTORY_VALUE=/mnt/data1/jaeger.badger/data BADGER_DIRECTORY_KEY=/mnt/data1/jaeger.badger/key \
./jaeger-all-in-one --collector.zipkin.host-port=:9411`)
		if ctx.Err() != nil {
			err = nil
		}
		return err
	})
	hps, err := c.InternalIP(ctx, l, c.Node(1))
	if err != nil {
		return err
	}
	if _, err := c.Conn(ctx, l, 1).Exec(`SET CLUSTER SETTING trace.zipkin.collector = $1`, hps[0]+":9411"); err != nil {
		return err
	}
	time.Sleep(time.Second) // give jaeger a moment to start
	hps, err = c.ExternalIP(ctx, l, c.Node(1))
	if err != nil {
		return err
	}
	c.Run(ctx, option.WithNodes(c.Node(1)), "ln -s /mnt/data1/jaeger.badger logs/jaeger.badger")
	l.Printf("jaeger UI should now be running at http://%s:16686", hps[0])
	return nil
}

var _ = InstallLaunchAndConfigureJaegerAllInOne // defeat unused lint
