// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn"
	"github.com/cockroachdb/cockroach/pkg/cloud/externalconn/connectionpb"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

func TestStripEmptyTopicNameFromChangefeeds_Jobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_ChangefeedsRejectEmptyTopicName)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	registry := s.JobRegistry().(*jobs.Registry)
	db := s.InternalDB().(isql.DB)

	// The CREATE CHANGEFEED statement handler is registered by the changefeed
	// CCL package, which isn't linked in this test binary. We create
	// changefeed jobs directly instead. The migration operates on the
	// persisted payload, which is identical regardless of how the job was
	// created.
	testCases := map[string]struct {
		sinkURI     string
		wantSinkURI string
	}{
		"empty topic_name stripped": {
			sinkURI:     "kafka://localhost:9092?topic_name=",
			wantSinkURI: "kafka://localhost:9092",
		},
		"empty topic_name stripped, other params preserved": {
			sinkURI:     "kafka://localhost:9092?topic_name=&other=val",
			wantSinkURI: "kafka://localhost:9092?other=val",
		},
		"non-empty topic_name unchanged": {
			sinkURI:     "kafka://localhost:9092?topic_name=foo",
			wantSinkURI: "kafka://localhost:9092?topic_name=foo",
		},
		"no topic_name unchanged": {
			sinkURI:     "kafka://localhost:9092?other=val",
			wantSinkURI: "kafka://localhost:9092?other=val",
		},
	}

	// Create a changefeed job for each test case.
	jobIDs := make(map[string]jobspb.JobID, len(testCases))
	for name, tc := range testCases {
		jobID := registry.MakeJobID()
		jobIDs[name] = jobID
		record := jobs.Record{
			Description: name,
			Username:    username.RootUserName(),
			Details: jobspb.ChangefeedDetails{
				SinkURI: tc.sinkURI,
			},
			Progress: jobspb.ChangefeedProgress{},
		}
		_, err := registry.CreateAdoptableJobWithTxn(ctx, record, jobID, nil /* txn */)
		require.NoError(t, err)
	}

	readSinkURI := func(t *testing.T, jobID jobspb.JobID) string {
		var sinkURI string
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			payloadBytes, exists, err := infoStorage.GetLegacyPayload(
				ctx, "test-read-payload")
			if err != nil {
				return err
			}
			require.True(t, exists, "payload should exist for job %d", jobID)
			var payload jobspb.Payload
			if err := protoutil.Unmarshal(payloadBytes, &payload); err != nil {
				return err
			}
			sinkURI = payload.GetChangefeed().SinkURI
			return nil
		})
		require.NoError(t, err)
		return sinkURI
	}

	// Verify SinkURIs are persisted as-is before the migration.
	for name, tc := range testCases {
		require.Equal(t, tc.sinkURI, readSinkURI(t, jobIDs[name]),
			"pre-migration SinkURI mismatch for %q", name)
	}

	// Run the upgrade.
	upgrades.Upgrade(t, s.SQLConn(t),
		clusterversion.V26_2_ChangefeedsRejectEmptyTopicName, nil, false)

	// Verify the SinkURIs after the migration.
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.wantSinkURI, readSinkURI(t, jobIDs[name]))
		})
	}
}

func TestStripEmptyTopicNameFromChangefeeds_ExternalConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t,
		clusterversion.V26_2_ChangefeedsRejectEmptyTopicName)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	db := s.InternalDB().(isql.DB)

	// The kafka and pubsub external connection schemes are registered by the
	// changefeed CCL package, which isn't linked in this test binary. We
	// create external connections directly instead. The migration operates
	// on the persisted connection details, which is identical regardless of
	// how the connection was created.
	testCases := map[string]struct {
		uri      string
		provider connectionpb.ConnectionProvider
		wantURI  string
	}{
		"kafka empty topic_name stripped": {
			uri:      "kafka://broker:9092?topic_name=",
			provider: connectionpb.ConnectionProvider_kafka,
			wantURI:  "kafka://broker:9092",
		},
		"kafka non-empty topic_name unchanged": {
			uri:      "kafka://broker:9092?topic_name=foo",
			provider: connectionpb.ConnectionProvider_kafka,
			wantURI:  "kafka://broker:9092?topic_name=foo",
		},
		"pubsub empty topic_name stripped": {
			uri:      "gcpubsub://project?topic_name=",
			provider: connectionpb.ConnectionProvider_gcpubsub,
			wantURI:  "gcpubsub://project",
		},
		"s3 not touched": {
			uri:      "s3://bucket?topic_name=",
			provider: connectionpb.ConnectionProvider_s3,
			wantURI:  "s3://bucket?topic_name=",
		},
	}

	// Create an external connection for each test case.
	for name, tc := range testCases {
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			ec := externalconn.NewMutableExternalConnection()
			ec.SetConnectionName(name)
			ec.SetConnectionType(connectionpb.TypeStorage)
			ec.SetConnectionDetails(connectionpb.ConnectionDetails{
				Provider: tc.provider,
				Details: &connectionpb.ConnectionDetails_SimpleURI{
					SimpleURI: &connectionpb.SimpleURI{URI: tc.uri},
				},
			})
			ec.SetOwner(username.RootUserName())
			ec.SetOwnerID(1)
			return ec.Create(ctx, txn)
		})
		require.NoError(t, err)
	}

	readURI := func(t *testing.T, name string) string {
		var uri string
		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			ec, err := externalconn.LoadExternalConnection(ctx, name, txn)
			if err != nil {
				return err
			}
			d, ok := ec.ConnectionProto().Details.(*connectionpb.ConnectionDetails_SimpleURI)
			require.True(t, ok)
			uri = d.SimpleURI.URI
			return nil
		})
		require.NoError(t, err)
		return uri
	}

	// Verify URIs are persisted as-is before the migration.
	for name, tc := range testCases {
		require.Equal(t, tc.uri, readURI(t, name),
			"pre-migration URI mismatch for %q", name)
	}

	// Run the upgrade.
	upgrades.Upgrade(t, s.SQLConn(t),
		clusterversion.V26_2_ChangefeedsRejectEmptyTopicName, nil, false)

	// Verify the URIs after the migration.
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.wantURI, readURI(t, name))
		})
	}
}
