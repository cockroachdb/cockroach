// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package ingest

import (
	"context"
	gosql "database/sql"
	"encoding/json"
	"net"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/obs"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obslib/obsutil"
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	logspb "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/collector/logs/v1"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// Test an end-to-end integration between the ObsService and a CRDB cluster:
// verify that events get exported from CRDB and imported in the Obs Service.
func TestEventIngestionIntegration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "embed", func(t *testing.T, embed bool) {
		var obsAddr string

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		testConsumer := obsutil.NewTestCaptureConsumer()
		if !embed {
			// Allocate a port for the ingestion service to work around a circular
			// dependency: CRDB needs to be told what the port is, but we can only create
			// the event ingester after having started CRDB (because the ingester wants a
			// reference to CRDB).
			otlpListener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			defer func() {
				_ = otlpListener.Close()
			}()
			obsAddr = otlpListener.Addr().String()
			s, sqlDB, _ = serverutils.StartServer(t,
				base.TestServerArgs{
					ObsServiceAddr: obsAddr,
					Knobs: base.TestingKnobs{
						EventExporter: &obs.EventExporterTestingKnobs{
							// Flush every message.
							FlushTriggerByteSize: 1,
						},
					},
				},
			)
			defer s.Stopper().Stop(ctx)

			// Start the ingestion in the background.
			obsStop := stop.NewStopper()
			defer obsStop.Stop(ctx)
			e := MakeEventIngester(ctx, testConsumer, nil)
			opts := rpc.DefaultContextOptions()
			opts.NodeID = &base.NodeIDContainer{}
			opts.Insecure = true
			opts.Stopper = obsStop
			opts.Settings = cluster.MakeTestingClusterSettings()
			rpcContext := rpc.NewContext(ctx, opts)
			grpcServer, err := rpc.NewServer(ctx, rpcContext)
			require.NoError(t, err)
			defer grpcServer.Stop()
			logspb.RegisterLogsServiceServer(grpcServer, e)
			go func() {
				_ = grpcServer.Serve(otlpListener)
			}()
		} else {
			s, sqlDB, _ = serverutils.StartServer(t,
				base.TestServerArgs{
					ObsServiceAddr: base.ObsServiceEmbedFlagValue,
					Knobs: base.TestingKnobs{
						EventExporter: &obs.EventExporterTestingKnobs{
							// Flush every message.
							FlushTriggerByteSize: 1,
							TestConsumer:         testConsumer,
						},
					},
				},
			)
			defer s.Stopper().Stop(ctx)
		}

		// Perform a schema change and check that we get an event.
		_, err := sqlDB.Exec("create table t()")
		require.NoError(t, err)
		testutils.SucceedsSoon(t, func() error {
			foundEvent := testConsumer.Contains(func(event *obspb.Event) bool {
				type eventLogType struct {
					EventType string `json:"EventType"`
					Statement string `json:"Statement"`
				}

				var ev eventLogType
				if err := json.Unmarshal([]byte(event.LogRecord.Body.GetStringValue()), &ev); err != nil {
					t.Fatalf("failed to deserialize event: %v", event)
				}
				// Look for our specific create table statement.
				if ev.EventType == "create_table" &&
					strings.Contains(ev.Statement, "CREATE TABLE defaultdb.public.t") {
					return true
				}
				return false
			})
			if !foundEvent {
				return errors.Newf("no event found yet")
			}
			return nil
		})
	})
}
