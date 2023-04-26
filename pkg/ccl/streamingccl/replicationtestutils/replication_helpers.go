// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package replicationtestutils

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/contextutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// FeedEventPredicate allows tests to search a ReplicationFeed.
type FeedEventPredicate func(message streamingccl.Event) bool

// FeedErrorPredicate allows tests to match an error from ReplicationFeed.
type FeedErrorPredicate func(err error) bool

// KeyMatches makes a FeedEventPredicate that matches a given key.
func KeyMatches(key roachpb.Key) FeedEventPredicate {
	return func(msg streamingccl.Event) bool {
		if msg.Type() != streamingccl.KVEvent {
			return false
		}
		return bytes.Equal(key, msg.GetKV().Key)
	}
}

func minResolvedTimestamp(resolvedSpans []jobspb.ResolvedSpan) hlc.Timestamp {
	minTimestamp := hlc.MaxTimestamp
	for _, rs := range resolvedSpans {
		if rs.Timestamp.Less(minTimestamp) {
			minTimestamp = rs.Timestamp
		}
	}
	return minTimestamp
}

// ResolvedAtLeast makes a FeedEventPredicate that matches when a timestamp has been
// reached.
func ResolvedAtLeast(lo hlc.Timestamp) FeedEventPredicate {
	return func(msg streamingccl.Event) bool {
		if msg.Type() != streamingccl.CheckpointEvent {
			return false
		}
		return lo.LessEq(minResolvedTimestamp(msg.GetResolvedSpans()))
	}
}

// FeedSource is a source of events for a ReplicationFeed.
type FeedSource interface {
	// Next returns the next event, and a flag indicating if there are more events
	// to consume.
	Next() (streamingccl.Event, bool)

	// Error returns the error encountered in the feed. If present, it
	// is set after Next() indicates there is no more event to consume.
	Error() error

	// Close shuts down the source.
	Close(ctx context.Context)
}

// ReplicationFeed allows tests to search for events on a feed.
type ReplicationFeed struct {
	t   *testing.T
	f   FeedSource
	msg streamingccl.Event
}

// MakeReplicationFeed creates a ReplicationFeed based on a given FeedSource.
func MakeReplicationFeed(t *testing.T, f FeedSource) *ReplicationFeed {
	return &ReplicationFeed{
		t: t,
		f: f,
	}
}

// ObserveKey consumes the feed until requested key has been seen (or deadline expired).
// Note: we don't do any buffering here.  Therefore, it is required that the key
// we want to observe will arrive at some point in the future.
func (rf *ReplicationFeed) ObserveKey(ctx context.Context, key roachpb.Key) roachpb.KeyValue {
	rf.consumeUntil(ctx, KeyMatches(key), func(err error) bool {
		return false
	})
	return *rf.msg.GetKV()
}

// ObserveResolved consumes the feed until we received resolved timestamp that's at least
// as high as the specified low watermark.  Returns observed resolved timestamp.
func (rf *ReplicationFeed) ObserveResolved(ctx context.Context, lo hlc.Timestamp) hlc.Timestamp {
	rf.consumeUntil(ctx, ResolvedAtLeast(lo), func(err error) bool {
		return false
	})
	return minResolvedTimestamp(rf.msg.GetResolvedSpans())
}

// ObserveError consumes the feed until the feed is exhausted, and the final error should
// match 'errPred'.
func (rf *ReplicationFeed) ObserveError(ctx context.Context, errPred FeedErrorPredicate) {
	rf.consumeUntil(ctx, func(message streamingccl.Event) bool {
		return false
	}, errPred)
}

// Close cleans up any resources.
func (rf *ReplicationFeed) Close(ctx context.Context) {
	rf.f.Close(ctx)
}

func (rf *ReplicationFeed) consumeUntil(
	ctx context.Context, pred FeedEventPredicate, errPred FeedErrorPredicate,
) {
	require.NoError(rf.t, contextutil.RunWithTimeout(ctx, "consume", 2*time.Minute,
		func(ctx context.Context) error {
			rowCount := 0
			for {
				msg, haveMoreRows := rf.f.Next()
				if !haveMoreRows {
					if err := rf.f.Error(); err != nil {
						if errPred(err) {
							return nil
						}
						return err
					}
					return errors.Newf("ran out of rows after processing %d rows", rowCount)
				}
				rowCount++
				if msg == nil {
					return errors.New("consumed empty msg")
				}
				if pred(msg) {
					rf.msg = msg
					return nil
				}
			}
		}),
	)

}

// TenantState maintains test state related to tenant.
type TenantState struct {
	// Name is the name of the tenant.
	Name roachpb.TenantName
	// ID is the ID of the tenant.
	ID roachpb.TenantID
	// Codec is the Codec of the tenant.
	Codec keys.SQLCodec
	// SQL is a sql connection to the tenant.
	SQL *sqlutils.SQLRunner
}

// ReplicationHelper wraps a test server configured to be run in streaming
// replication tests. It exposes easy access to a tenant in the server, as well
// as a PGUrl to the underlying server.
type ReplicationHelper struct {
	// SysServer is the backing server.
	SysServer serverutils.TestServerInterface
	// SysSQL is a sql connection to the system tenant.
	SysSQL *sqlutils.SQLRunner
	// PGUrl is the pgurl of this server.
	PGUrl url.URL
}

// NewReplicationHelper starts test server with the required cluster settings for streming
func NewReplicationHelper(
	t *testing.T, serverArgs base.TestServerArgs,
) (*ReplicationHelper, func()) {
	ctx := context.Background()

	// Start server
	s, db, _ := serverutils.StartServer(t, serverArgs)

	// Make changefeeds run faster.
	resetFreq := changefeedbase.TestingSetDefaultMinCheckpointFrequency(50 * time.Millisecond)

	// Set required cluster settings.
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t, strings.Split(`
SET CLUSTER SETTING kv.rangefeed.enabled = true;
SET CLUSTER SETTING kv.closed_timestamp.target_duration = '1s';
SET CLUSTER SETTING changefeed.experimental_poll_interval = '10ms';
SET CLUSTER SETTING cross_cluster_replication.enabled = true;
`, `;`)...)

	// Sink to read data from.
	sink, cleanupSink := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(username.RootUser))

	h := &ReplicationHelper{
		SysServer: s,
		SysSQL:    sqlutils.MakeSQLRunner(db),
		PGUrl:     sink,
	}

	return h, func() {
		cleanupSink()
		resetFreq()
		s.Stopper().Stop(ctx)
	}
}

// CreateTenant creates a tenant under the replication helper's server.
func (rh *ReplicationHelper) CreateTenant(
	t *testing.T, tenantID roachpb.TenantID, tenantName roachpb.TenantName,
) (TenantState, func()) {
	_, tenantConn := serverutils.StartTenant(t, rh.SysServer, base.TestTenantArgs{
		TenantID:   tenantID,
		TenantName: tenantName,
	})
	return TenantState{
			Name:  tenantName,
			ID:    tenantID,
			Codec: keys.MakeSQLCodec(tenantID),
			SQL:   sqlutils.MakeSQLRunner(tenantConn),
		}, func() {
			require.NoError(t, tenantConn.Close())
		}
}

// TableSpan returns primary index span for a table.
func (rh *ReplicationHelper) TableSpan(codec keys.SQLCodec, table string) roachpb.Span {
	desc := desctestutils.TestingGetPublicTableDescriptor(
		rh.SysServer.DB(), codec, "d", table)
	return desc.PrimaryIndexSpan(codec)
}

// StartReplicationStream reaches out to the system tenant to start the
// replication stream from the source tenant.
func (rh *ReplicationHelper) StartReplicationStream(
	t *testing.T, sourceTenantName roachpb.TenantName,
) streampb.ReplicationProducerSpec {
	var rawReplicationProducerSpec []byte
	row := rh.SysSQL.QueryRow(t, `SELECT crdb_internal.start_replication_stream($1)`, sourceTenantName)
	row.Scan(&rawReplicationProducerSpec)
	var replicationProducerSpec streampb.ReplicationProducerSpec
	err := protoutil.Unmarshal(rawReplicationProducerSpec, &replicationProducerSpec)
	require.NoError(t, err)
	return replicationProducerSpec
}
