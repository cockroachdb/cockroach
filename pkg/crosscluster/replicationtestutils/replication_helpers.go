// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"bytes"
	"context"
	"math/rand"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/streamclient"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/pgurlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// FeedEventPredicate allows tests to search a ReplicationFeed.
type FeedEventPredicate func(message crosscluster.Event) bool

// FeedErrorPredicate allows tests to match an error from ReplicationFeed.
type FeedErrorPredicate func(err error) bool

// KeyMatches makes a FeedEventPredicate that matches a given key in a kv batch.
func KeyMatches(key roachpb.Key) FeedEventPredicate {
	return func(msg crosscluster.Event) bool {
		if msg.Type() != crosscluster.KVEvent {
			return false
		}
		for _, kv := range msg.GetKVs() {
			if bytes.Equal(key, kv.KeyValue.Key) {
				return true
			}
		}
		return false
	}
}

func AnySpanConfigMatches() FeedEventPredicate {
	return func(msg crosscluster.Event) bool {
		return msg.Type() == crosscluster.SpanConfigEvent
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
	return func(msg crosscluster.Event) bool {
		if msg.Type() != crosscluster.CheckpointEvent {
			return false
		}
		return lo.LessEq(minResolvedTimestamp(msg.GetCheckpoint().ResolvedSpans))
	}
}

// ContainsRangeStats makes a FeedEventPredicate that matches when the range
// contains a checkpoint.
func ContainsRangeStats() FeedEventPredicate {
	return func(msg crosscluster.Event) bool {
		if msg.Type() != crosscluster.CheckpointEvent {
			return false
		}
		return msg.GetCheckpoint().RangeStats != nil
	}
}

// FeedSource is a source of events for a ReplicationFeed.
type FeedSource interface {
	// Next returns the next event, and a flag indicating if there are more events
	// to consume.
	Next() (crosscluster.Event, bool)

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
	msg crosscluster.Event
}

// MakeReplicationFeed creates a ReplicationFeed based on a given FeedSource.
func MakeReplicationFeed(t *testing.T, f FeedSource) *ReplicationFeed {
	return &ReplicationFeed{
		t: t,
		f: f,
	}
}

// ObserveKey consumes the feed until requested key has been seen (or deadline
// expired) in a batch (all of which, including subsequent keys, is consumed).
// Note: we don't do any buffering here.  Therefore, it is required that the key
// we want to observe will arrive at some point in the future.
func (rf *ReplicationFeed) ObserveKey(ctx context.Context, key roachpb.Key) roachpb.KeyValue {
	rf.consumeUntil(ctx, KeyMatches(key), func(err error) bool {
		return false
	})
	return rf.msg.GetKVs()[0].KeyValue
}

// ObserveAnySpanConfigRecord consumes the feed until any span config record is observed.
// Note: we don't do any buffering here.  Therefore, it is required that the key
// we want to observe will arrive at some point in the future.
func (rf *ReplicationFeed) ObserveAnySpanConfigRecord(
	ctx context.Context,
) streampb.StreamedSpanConfigEntry {
	rf.consumeUntil(ctx, AnySpanConfigMatches(), func(err error) bool {
		return false
	})
	return *rf.msg.GetSpanConfigEvent()
}

// ObserveResolved consumes the feed until we received resolved timestamp that's at least
// as high as the specified low watermark.  Returns observed resolved timestamp.
func (rf *ReplicationFeed) ObserveResolved(ctx context.Context, lo hlc.Timestamp) hlc.Timestamp {
	rf.consumeUntil(ctx, ResolvedAtLeast(lo), func(err error) bool {
		return false
	})
	return minResolvedTimestamp(rf.msg.GetCheckpoint().ResolvedSpans)
}

// ObserveRangeStats consumes the feed until we recieve a checkpoint that
// contains range stats. Returns the stats from the checkpoint.
func (rf *ReplicationFeed) ObserveRangeStats(ctx context.Context) streampb.StreamEvent_RangeStats {
	if !ContainsRangeStats()(rf.msg) {
		rf.consumeUntil(ctx, ContainsRangeStats(), func(err error) bool {
			return false
		})
	}
	return *rf.msg.GetCheckpoint().RangeStats
}

// ObserveError consumes the feed until the feed is exhausted, and the final error should
// match 'errPred'.
func (rf *ReplicationFeed) ObserveError(ctx context.Context, errPred FeedErrorPredicate) {
	rf.consumeUntil(ctx, func(message crosscluster.Event) bool {
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
	require.NoError(rf.t, timeutil.RunWithTimeout(ctx, "consume", 2*time.Minute,
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
	SysServer  serverutils.ApplicationLayerInterface
	TestServer serverutils.TestServerInterface
	// SysSQL is a sql connection to the system tenant.
	SysSQL *sqlutils.SQLRunner
	// PGUrl is the pgurl of this server.
	PGUrl url.URL

	rng *rand.Rand
}

// NewReplicationHelper starts test server with the required cluster settings for streming
func NewReplicationHelper(
	t *testing.T, serverArgs base.TestServerArgs,
) (*ReplicationHelper, func()) {
	ctx := context.Background()

	// Start server
	srv, db, _ := serverutils.StartServer(t, serverArgs)
	s := srv.SystemLayer()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.ExecMultiple(t,
		// Required for replication stremas to work.
		`SET CLUSTER SETTING kv.rangefeed.enabled = true`,

		// Speeds up the tests a bit.
		`SET CLUSTER SETTING kv.rangefeed.closed_timestamp_refresh_interval = '200ms'`,
		`SET CLUSTER SETTING kv.closed_timestamp.target_duration = '50ms'`,
		`SET CLUSTER SETTING kv.closed_timestamp.side_transport_interval = '10ms'`,
		`SET CLUSTER SETTING stream_replication.min_checkpoint_frequency = '10ms'`)

	// Sink to read data from.
	sink, cleanupSink := pgurlutils.PGUrl(t, s.AdvSQLAddr(), t.Name(), url.User(username.RootUser))

	rng, seed := randutil.NewPseudoRand()
	t.Logf("Replication helper seed %d", seed)

	h := &ReplicationHelper{
		SysServer:  s,
		TestServer: srv,
		SysSQL:     sqlutils.MakeSQLRunner(db),
		PGUrl:      sink,
		rng:        rng,
	}

	return h, func() {
		cleanupSink()
		srv.Stopper().Stop(ctx)
	}
}

// CreateTenant creates a tenant under the replication helper's server.
func (rh *ReplicationHelper) CreateTenant(
	t *testing.T, tenantID roachpb.TenantID, tenantName roachpb.TenantName,
) (TenantState, func()) {
	_, tenantConn := serverutils.StartTenant(t, rh.TestServer, base.TestTenantArgs{
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

func (rh *ReplicationHelper) MaybeGenerateInlineURL(t *testing.T) streamclient.ClusterUri {
	if rh.rng.Float64() > 0.5 {
		return streamclient.MakeTestClusterUri(rh.PGUrl)
	}

	t.Log("using inline certificates")
	ret := rh.PGUrl
	v := ret.Query()
	for _, opt := range []string{"sslcert", "sslkey", "sslrootcert"} {
		path := v.Get(opt)
		content, err := os.ReadFile(path)
		require.NoError(t, err)
		v.Set(opt, string(content))

	}
	v.Set("sslinline", "true")
	ret.RawQuery = v.Encode()
	return streamclient.MakeTestClusterUri(ret)
}
