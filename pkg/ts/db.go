// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package ts

import (
	"context"
	"fmt"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

var (
	resolution1nsDefaultPruneThreshold = time.Second
	resolution10sDefaultPruneThreshold = 30 * 24 * time.Hour
)

// TimeseriesStorageEnabled controls whether to store timeseries data to disk.
var TimeseriesStorageEnabled = settings.RegisterBoolSetting(
	"timeseries.storage.enabled",
	"if set, periodic timeseries data is stored within the cluster; disabling is not recommended "+
		"unless you are storing the data elsewhere",
	true,
)

// Resolution10StoreDuration defines the amount of time to store internal metrics
var Resolution10StoreDuration = settings.RegisterDurationSetting(
	"timeseries.resolution_10s.storage_duration",
	"the amount of time to store timeseries data",
	resolution10sDefaultPruneThreshold,
)

// DB provides Cockroach's Time Series API.
type DB struct {
	db      *client.DB
	st      *cluster.Settings
	metrics *TimeSeriesMetrics

	// pruneAgeByResolution maintains a suggested maximum age per resolution; data
	// which is older than the given threshold for a resolution is considered
	// eligible for deletion. Thresholds are specified in nanoseconds.
	pruneThresholdByResolution map[Resolution]func() int64
}

// NewDB creates a new DB instance.
func NewDB(db *client.DB, settings *cluster.Settings) *DB {
	pruneThresholdByResolution := map[Resolution]func() int64{
		Resolution10s: func() int64 { return Resolution10StoreDuration.Get(&settings.SV).Nanoseconds() },
		resolution1ns: func() int64 { return resolution1nsDefaultPruneThreshold.Nanoseconds() },
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
	p.stopper.RunWorker(context.TODO(), func(context.Context) {
		// Poll once immediately.
		p.poll()
		ticker := time.NewTicker(p.frequency)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				p.poll()
			case <-p.stopper.ShouldStop():
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
		log.Warning(bgCtx, err)
	}
}

// StoreData writes the supplied time series data to the cockroach server.
// Stored data will be sampled at the supplied resolution.
func (db *DB) StoreData(ctx context.Context, r Resolution, data []tspb.TimeSeriesData) error {
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
	sizeOfTimestamp := int64(unsafe.Sizeof(hlc.Timestamp{}))

	// Process data collection: data is converted to internal format, and a key
	// is generated for each internal message.
	for _, d := range data {
		idatas, err := d.ToInternal(r.SlabDuration(), r.SampleDuration())
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
			totalSamples += int64(len(idata.Samples))
			totalSizeOfKvs += int64(len(value.RawBytes)+len(key)) + sizeOfTimestamp
		}
	}

	// Send the individual internal merge requests.
	b := &client.Batch{}
	for _, kv := range kvs {
		b.AddRawRequest(&roachpb.MergeRequest{
			Span: roachpb.Span{
				Key: kv.Key,
			},
			Value: kv.Value,
		})
	}

	if err := db.db.Run(ctx, b); err != nil {
		return err
	}

	db.metrics.WriteSamples.Inc(totalSamples)
	db.metrics.WriteBytes.Inc(totalSizeOfKvs)
	return nil
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
