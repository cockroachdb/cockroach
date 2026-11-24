// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/metamorphic"
)

var useSwapMutations = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"logical_replication.consumer.use_swap_mutations.enabled",
	"determines whether the consumer uses swap mutations for update and delete operations",
	metamorphic.ConstantWithTestBool("logical_replication.consumer.use_swap_mutations.enabled", true),
)

func NewInternalSession(
	ctx context.Context, db isql.DB, sd *sessiondata.SessionData, settings *cluster.Settings,
) (isql.Session, error) {
	sd = sd.Clone()
	sd.PlanCacheMode = sessiondatapb.PlanCacheModeForceGeneric
	sd.VectorizeMode = sessiondatapb.VectorizeOff
	sd.UseSwapMutations = useSwapMutations.Get(&settings.SV)
	sd.BufferedWritesEnabled = false
	sd.OriginIDForLogicalDataReplication = 1
	return db.Session(ctx, "logical-data-replication", isql.WithSessionData(sd))
}
