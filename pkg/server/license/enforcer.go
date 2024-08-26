// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package license

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Enforcer is responsible for enforcing license policies.
type Enforcer struct {
	// DB is used to access into the KV. This is set via InitDB. It must have
	// access to the system tenant since it read/writes KV keys in the system
	// keyspace.
	db descs.DB

	// TestingKnobs are used to control the behavior of the enforcer for testing.
	TestingKnobs *TestingKnobs

	// clusterInitTS is the timestamp of when the cluster was initialized. This is
	// read/written to the KV.
	clusterInitTS atomic.Int64

	// startTime is the time when the enforcer was created. This is used to seed
	// the cluster init timestamp if it's not set in the KV.
	startTime time.Time
}

type TestingKnobs struct {
	// OverrideStartTime if set, overrides the time that's used to seed the
	// cluster init timestamp.
	OverrideStartTime *time.Time
}

var instance *Enforcer
var once sync.Once

// GetEnforcerInstance returns the singleton instance of the Enforcer. The
// Enforcer is responsible for license enforcement policies.
func GetEnforcerInstance() *Enforcer {
	if instance == nil {
		once.Do(
			func() {
				instance = newEnforcer()
			})
	}
	return instance
}

// newEnforcer creates a new Enforcer object.
func newEnforcer() *Enforcer {
	return &Enforcer{
		startTime: timeutil.Now(),
	}
}

// Start will load the necessary metadata for the enforcer. It reads from the
// KV license metadata and will populate any missing data as needed. The DB
// passed in must have access to the system tenant.
func (e *Enforcer) Start(ctx context.Context, db descs.DB) error {
	e.db = db
	return e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// We could use a conditional put for this logic. However, we want to read
		// and cache the value, and the common case is that the value will be read.
		// Only during the initialization of the first node in the cluster will we
		// need to write a new timestamp. So, we optimize for the case where the
		// timestamp already exists.
		val, err := txn.KV().Get(ctx, keys.ClusterInitTimestamp)
		if err != nil {
			return err
		}
		if val.Value == nil {
			log.Infof(ctx, "Generated new cluster init time: %s", e.startTime.UTC().String())
			e.clusterInitTS.Store(timeutil.ToUnixMicros(e.getStartTime()))
			return txn.KV().Put(ctx, keys.ClusterInitTimestamp, e.clusterInitTS.Load())
		}
		e.clusterInitTS.Store(val.ValueInt())
		log.Infof(ctx, "Fetched existing cluster init time: %s", e.GetClusterInitTS().String())
		return nil
	})
}

// GetClusterInitTS will return the timestamp of when the cluster was initialized.
func (e *Enforcer) GetClusterInitTS() time.Time {
	// In the rare case that the cluster init timestamp has not been cached yet,
	// we will return an approximate value, the start time of the server. This
	// should only happen if we are in the process of caching the cluster init
	// timestamp, or we failed to cache it. This is preferable to returning an
	// error or a zero value.
	if e.clusterInitTS.Load() == 0 {
		return e.getStartTime()
	}
	return timeutil.FromUnixMicros(e.clusterInitTS.Load())
}

// getStartTime returns the time when the enforcer was created. This accounts
// for testing knobs that may override the time.
func (e *Enforcer) getStartTime() time.Time {
	if e.TestingKnobs != nil && e.TestingKnobs.OverrideStartTime != nil {
		return *e.TestingKnobs.OverrideStartTime
	}
	return e.startTime
}
