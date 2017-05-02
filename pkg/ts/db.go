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
//
// Author: Matt Tracy (matt@cockroachlabs.com)

package ts

import (
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// DB provides Cockroach's Time Series API.
type DB struct {
	db *client.DB
}

// NewDB creates a new DB instance.
func NewDB(db *client.DB) *DB {
	return &DB{
		db: db,
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
	bgCtx := p.AnnotateCtx(context.Background())
	if err := p.stopper.RunTask(bgCtx, func(bgCtx context.Context) {
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
	var kvs []roachpb.KeyValue

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
			kvs = append(kvs, roachpb.KeyValue{
				Key:   MakeDataKey(d.Name, d.Source, r, idata.StartTimestampNanos),
				Value: value,
			})
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

	return db.db.Run(ctx, b)
}
