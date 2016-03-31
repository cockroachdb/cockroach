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

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
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

// PollSource begins a Goroutine which periodically queries the supplied
// DataSource for time series data, storing the returned data in the server.
// Stored data will be sampled using the provided Resolution. The polling
// process will continue until the provided stop.Stopper is stopped.
func (db *DB) PollSource(source DataSource, frequency time.Duration, r Resolution, stopper *stop.Stopper) {
	p := &poller{
		db:        db,
		source:    source,
		frequency: frequency,
		r:         r,
		stopper:   stopper,
	}
	p.start()
}

// A DataSource can be queryied for a slice of time series data.
type DataSource interface {
	GetTimeSeriesData() []TimeSeriesData
}

// poller maintains information for a polling process started by PollSource().
type poller struct {
	db        *DB
	source    DataSource
	frequency time.Duration
	r         Resolution
	stopper   *stop.Stopper
}

// start begins the goroutine for this poller, which will periodically request
// time series data from the DataSource and store it.
func (p *poller) start() {
	p.stopper.RunWorker(func() {
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
	p.stopper.RunTask(func() {
		data := p.source.GetTimeSeriesData()
		if len(data) == 0 {
			return
		}

		if err := p.db.StoreData(p.r, data); err != nil {
			log.Warningf("error writing time series data: %s", err)
		}
	})
}

// StoreData writes the supplied time series data to the cockroach server.
// Stored data will be sampled at the supplied resolution.
func (db *DB) StoreData(r Resolution, data []TimeSeriesData) error {
	var kvs []roachpb.KeyValue

	// Process data collection: data is converted to internal format, and a key
	// is generated for each internal message.
	for _, d := range data {
		idatas, err := d.ToInternal(r.KeyDuration(), r.SampleDuration())
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
	b := client.Batch{}
	for _, kv := range kvs {
		b.InternalAddRequest(&roachpb.MergeRequest{
			Span: roachpb.Span{
				Key: kv.Key,
			},
			Value: kv.Value,
		})
	}

	return db.db.Run(&b).GoError()
}
