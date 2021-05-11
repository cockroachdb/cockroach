// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ts

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var (
	resolution1nsDefaultRollupThreshold = time.Second
	// The deprecated prune threshold for the 10s resolution was created before
	// time series rollups were enabled. It is still used in the transition period
	// during an upgrade before the cluster version is finalized. After the
	// version upgrade, the rollup threshold is used instead.
	deprecatedResolution10sDefaultPruneThreshold = 30 * 24 * time.Hour
	resolution10sDefaultRollupThreshold          = 10 * 24 * time.Hour
	resolution30mDefaultPruneThreshold           = 90 * 24 * time.Hour
	resolution50nsDefaultPruneThreshold          = 1 * time.Millisecond
)

// TimeseriesStorageEnabled controls whether to store timeseries data to disk.
var TimeseriesStorageEnabled = settings.RegisterBoolSetting(
	"timeseries.storage.enabled",
	"if set, periodic timeseries data is stored within the cluster; disabling is not recommended "+
		"unless you are storing the data elsewhere",
	true,
).WithPublic()

// Resolution10sStorageTTL defines the maximum age of data that will be retained
// at he 10 second resolution. Data older than this is subject to being "rolled
// up" into the 30 minute resolution and then deleted.
var Resolution10sStorageTTL = settings.RegisterDurationSetting(
	"timeseries.storage.resolution_10s.ttl",
	"the maximum age of time series data stored at the 10 second resolution. Data older than this "+
		"is subject to rollup and deletion.",
	resolution10sDefaultRollupThreshold,
).WithPublic()

// deprecatedResolution30StoreDuration is retained for backward compatibility during a version upgrade.
var deprecatedResolution30StoreDuration = func() *settings.DurationSetting {
	s := settings.RegisterDurationSetting(
		"timeseries.storage.30m_resolution_ttl", "replaced by timeseries.storage.resolution_30m.ttl",
		resolution30mDefaultPruneThreshold,
	)
	s.SetRetired()
	return s
}()

func init() {
	// The setting is not used any more, but we need to keep its
	// definition for backward compatibility until the next release
	// cycle.
	_ = deprecatedResolution30StoreDuration
}

// Resolution30mStorageTTL defines the maximum age of data that will be
// retained at he 30 minute resolution. Data older than this is subject to
// deletion.
var Resolution30mStorageTTL = settings.RegisterDurationSetting(
	"timeseries.storage.resolution_30m.ttl",
	"the maximum age of time series data stored at the 30 minute resolution. Data older than this "+
		"is subject to deletion.",
	resolution30mDefaultPruneThreshold,
).WithPublic()

// DB provides Cockroach's Time Series API.
type DB struct {
	db      *kv.DB
	st      *cluster.Settings
	metrics *TimeSeriesMetrics

	// pruneAgeByResolution maintains a suggested maximum age per resolution; data
	// which is older than the given threshold for a resolution is considered
	// eligible for deletion. Thresholds are specified in nanoseconds.
	pruneThresholdByResolution map[Resolution]func() int64

	// forceRowFormat is set to true if the database should write in the old row
	// format, regardless of the current cluster setting. Currently only set to
	// true in tests to verify backwards compatibility.
	forceRowFormat bool
}

// NewDB creates a new DB instance.
func NewDB(db *kv.DB, settings *cluster.Settings) *DB {
	pruneThresholdByResolution := map[Resolution]func() int64{
		Resolution10s: func() int64 {
			return Resolution10sStorageTTL.Get(&settings.SV).Nanoseconds()
		},
		Resolution30m:  func() int64 { return Resolution30mStorageTTL.Get(&settings.SV).Nanoseconds() },
		resolution1ns:  func() int64 { return resolution1nsDefaultRollupThreshold.Nanoseconds() },
		resolution50ns: func() int64 { return resolution50nsDefaultPruneThreshold.Nanoseconds() },
	}
	return &DB{
		db:                         db,
		st:                         settings,
		metrics:                    NewTimeSeriesMetrics(),
		pruneThresholdByResolution: pruneThresholdByResolution,
	}
}

// A DataSource can be queryied for a slice of time series data.
type DataSource interface {
	GetTimeSeriesData() []tspb.TimeSeriesData
}

// poller maintains information for a polling process started by PollSource().
type poller struct {
	log.AmbientContext
	db        *DB
	source    DataSource
	frequency time.Duration
	r         Resolution
	stopper   *stop.Stopper
}

// PollSource begins a Goroutine which periodically queries the supplied
// DataSource for time series data, storing the returned data in the server.
// Stored data will be sampled using the provided Resolution. The polling
// process will continue until the provided stop.Stopper is stopped.
func (db *DB) PollSource(
	ambient log.AmbientContext,
	source DataSource,
	frequency time.Duration,
	r Resolution,
	stopper *stop.Stopper,
) {
	ambient.AddLogTag("ts-poll", nil)
	p := &poller{
		AmbientContext: ambient,
		db:             db,
		source:         source,
		frequency:      frequency,
		r:              r,
		stopper:        stopper,
	}
	p.start()
}

// start begins the goroutine for this poller, which will periodically request
// time series data from the DataSource and store it.
func (p *poller) start() {
	// Poll once immediately and synchronously.
	p.poll()
	_ = p.stopper.RunAsyncTask(context.TODO(), "ts-poller", func(context.Context) {
		ticker := time.NewTicker(p.frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopper.ShouldQuiesce():
				return
			}
		}
	})
}

// poll retrieves data from the underlying DataSource a single time, storing any
// returned time series data on the server.
func (p *poller) poll() {
	if !TimeseriesStorageEnabled.Get(&p.db.st.SV) {
		return
	}

	bgCtx := p.AnnotateCtx(context.Background())
	if err := p.stopper.RunTask(bgCtx, "ts.poller: poll", func(bgCtx context.Context) {
		data := p.source.GetTimeSeriesData()
		if len(data) == 0 {
			return
		}

		ctx, span := p.AnnotateCtxWithSpan(bgCtx, "ts-poll")
		defer span.Finish()

		if err := p.db.StoreData(ctx, p.r, data); err != nil {
			log.Warningf(ctx, "error writing time series data: %s", err)
		}
	}); err != nil {
		log.Warningf(bgCtx, "%v", err)
	}
}

// StoreData writes the supplied time series data to the cockroach server.
// Stored data will be sampled at the supplied resolution.
func (db *DB) StoreData(ctx context.Context, r Resolution, data []tspb.TimeSeriesData) error {
	if r.IsRollup() {
		return fmt.Errorf(
			"invalid attempt to store time series data in rollup resolution %s", r.String(),
		)
	}
	if TimeseriesStorageEnabled.Get(&db.st.SV) {
		if err := db.tryStoreData(ctx, r, data); err != nil {
			db.metrics.WriteErrors.Inc(1)
			return err
		}
	}
	return nil
}

func (db *DB) tryStoreData(ctx context.Context, r Resolution, data []tspb.TimeSeriesData) error {
	var kvs []roachpb.KeyValue
	var totalSizeOfKvs int64
	var totalSamples int64

	// Process data collection: data is converted to internal format, and a key
	// is generated for each internal message.
	for _, d := range data {
		idatas, err := d.ToInternal(r.SlabDuration(), r.SampleDuration(), db.WriteColumnar())
		if err != nil {
			return err
		}
		for _, idata := range idatas {
			var value roachpb.Value
			if err := value.SetProto(&idata); err != nil {
				return err
			}
			key := MakeDataKey(d.Name, d.Source, r, idata.StartTimestampNanos)
			kvs = append(kvs, roachpb.KeyValue{
				Key:   key,
				Value: value,
			})
			totalSamples += int64(idata.SampleCount())
			totalSizeOfKvs += int64(len(value.RawBytes)+len(key)) + sizeOfTimestamp
		}
	}

	if err := db.storeKvs(ctx, kvs); err != nil {
		return err
	}

	db.metrics.WriteSamples.Inc(totalSamples)
	db.metrics.WriteBytes.Inc(totalSizeOfKvs)
	return nil
}

// storeRollup writes the supplied time series rollup data to the cockroach
// server.
func (db *DB) storeRollup(ctx context.Context, r Resolution, data []rollupData) error {
	if !r.IsRollup() {
		return fmt.Errorf(
			"invalid attempt to store rollup data in non-rollup resolution %s", r.String(),
		)
	}
	if TimeseriesStorageEnabled.Get(&db.st.SV) {
		if err := db.tryStoreRollup(ctx, r, data); err != nil {
			db.metrics.WriteErrors.Inc(1)
			return err
		}
	}
	return nil
}

func (db *DB) tryStoreRollup(ctx context.Context, r Resolution, data []rollupData) error {
	var kvs []roachpb.KeyValue

	for _, d := range data {
		idatas, err := d.toInternal(r.SlabDuration(), r.SampleDuration())
		if err != nil {
			return err
		}
		for _, idata := range idatas {
			var value roachpb.Value
			if err := value.SetProto(&idata); err != nil {
				return err
			}
			key := MakeDataKey(d.name, d.source, r, idata.StartTimestampNanos)
			kvs = append(kvs, roachpb.KeyValue{
				Key:   key,
				Value: value,
			})
		}
	}

	return db.storeKvs(ctx, kvs)
	// TODO(mrtracy): metrics for rollups stored
}

func (db *DB) storeKvs(ctx context.Context, kvs []roachpb.KeyValue) error {
	b := &kv.Batch{}
	for _, kv := range kvs {
		b.AddRawRequest(&roachpb.MergeRequest{
			RequestHeader: roachpb.RequestHeader{
				Key: kv.Key,
			},
			Value: kv.Value,
		})
	}

	return db.db.Run(ctx, b)
}

// computeThresholds returns a map of timestamps for each resolution supported
// by the system. Data at a resolution which is older than the threshold
// timestamp for that resolution is considered eligible for deletion.
func (db *DB) computeThresholds(timestamp int64) map[Resolution]int64 {
	result := make(map[Resolution]int64, len(db.pruneThresholdByResolution))
	for k, v := range db.pruneThresholdByResolution {
		result[k] = timestamp - v()
	}
	return result
}

// PruneThreshold returns the pruning threshold duration for this resolution,
// expressed in nanoseconds. This duration determines how old time series data
// must be before it is eligible for pruning.
func (db *DB) PruneThreshold(r Resolution) int64 {
	threshold, ok := db.pruneThresholdByResolution[r]
	if !ok {
		panic(fmt.Sprintf("no prune threshold found for resolution value %v", r))
	}
	return threshold()
}

// Metrics gets the TimeSeriesMetrics structure used by this DB instance.
func (db *DB) Metrics() *TimeSeriesMetrics {
	return db.metrics
}

// WriteColumnar returns true if this DB should write data in the newer columnar
// format.
func (db *DB) WriteColumnar() bool {
	return !db.forceRowFormat
}

// WriteRollups returns true if this DB should write rollups for resolutions
// targeted for a rollup resolution.
func (db *DB) WriteRollups() bool {
	return !db.forceRowFormat
}
