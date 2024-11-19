// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package replicationtestutils

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvaccessor"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/stretchr/testify/require"
)

// MakeSpanConfigRecord makes a dummy span config record with the given ttl. If the ttl is 0,  no span config is added,
// and the record is treated as a delete record.
func MakeSpanConfigRecord(t *testing.T, targetSpan roachpb.Span, ttl int) spanconfig.Record {
	target := spanconfig.MakeTargetFromSpan(targetSpan)
	var spanConfig roachpb.SpanConfig
	if ttl > 0 {
		spanConfig = roachpb.SpanConfig{
			GCPolicy: roachpb.GCPolicy{
				TTLSeconds: int32(ttl),
			},
		}
	}
	// check that all orderedUpdates are observed.
	record, err := spanconfig.MakeRecord(target, spanConfig)
	require.NoError(t, err)
	return record
}

func RecordToEntry(record spanconfig.Record) roachpb.SpanConfigEntry {
	t := record.GetTarget().ToProto()
	c := record.GetConfig()
	return roachpb.SpanConfigEntry{
		Target: t,
		Config: c,
	}
}

// PrettyRecords pretty prints the span config target and config ttl.
func PrettyRecords(records []spanconfig.Record) string {
	var b strings.Builder
	for _, update := range records {
		b.WriteString(fmt.Sprintf(" %s: ttl %d,", update.GetTarget().GetSpan(), update.GetConfig().GCPolicy.TTLSeconds))
	}
	return b.String()
}

// NewReplicationHelperWithDummySpanConfigTable creates a new ReplicationHelper,
// a tenant, and a mock span config table that a spanConfigStreamClient can
// listen for updates on. To mimic tenant creation, this helper writes a
// spanConfig with the target [tenantPrefix,tenantPrefix.Next()). During tenant
// creation, this span config induces a range split on the tenant's start key.
func NewReplicationHelperWithDummySpanConfigTable(
	ctx context.Context, t *testing.T, streamingTestKnobs *sql.StreamingTestingKnobs,
) (*ReplicationHelper, *spanconfigkvaccessor.KVAccessor, TenantState, func()) {
	const dummySpanConfigurationsName = "dummy_span_configurations"
	dummyFQN := tree.NewTableNameWithSchema("d", catconstants.PublicSchemaName, dummySpanConfigurationsName)

	streamingTestKnobs.MockSpanConfigTableName = dummyFQN
	h, cleanup := NewReplicationHelper(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
		Knobs: base.TestingKnobs{
			Streaming: streamingTestKnobs,
		},
	})

	h.SysSQL.Exec(t, `
CREATE DATABASE d;
USE d;`)
	h.SysSQL.Exec(t, fmt.Sprintf("CREATE TABLE %s (LIKE system.span_configurations INCLUDING ALL)", dummyFQN))

	sourceAccessor := spanconfigkvaccessor.New(
		h.SysServer.DB(),
		h.SysServer.InternalExecutor().(isql.Executor),
		h.SysServer.ClusterSettings(),
		h.SysServer.Clock(),
		dummyFQN.String(),
		nil, /* knobs */
	)

	sourceTenantID := roachpb.MustMakeTenantID(uint64(10))
	sourceTenant, tenantCleanup := h.CreateTenant(t, sourceTenantID, "app")

	// To mimic tenant creation, write the source tenant split key to the dummy
	// span config table. For more info on this split key, read up on
	// https://github.com/cockroachdb/cockroach/pull/104920
	tenantPrefix := keys.MakeTenantPrefix(sourceTenantID)
	tenantSplitSpan := roachpb.Span{Key: tenantPrefix, EndKey: tenantPrefix.Next()}

	require.NoError(t, sourceAccessor.UpdateSpanConfigRecords(
		ctx,
		[]spanconfig.Target{},
		[]spanconfig.Record{MakeSpanConfigRecord(t, tenantSplitSpan, 14400)},
		hlc.MinTimestamp,
		hlc.MaxTimestamp))

	return h, sourceAccessor, sourceTenant, func() {
		tenantCleanup()
		cleanup()
	}
}
