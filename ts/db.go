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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Matt Tracy (matt.r.tracy@gmail.com)

package ts

import (
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// DB provides Cockroach's Time Series API.
type DB struct {
	kv *client.KV
}

// NewDB creates a new DB instance.
func NewDB(kv *client.KV) *DB {
	return &DB{
		kv: kv,
	}
}

// PollSource begins a Goroutine which periodically queries the supplied
// DataSource for time series data, storing the returned data in the server.
// Stored data will be sampled using the provided Resolution. The polling
// process will continue until the provided util.Stopper is stopped.
func (db *DB) PollSource(source DataSource, frequency time.Duration, r Resolution, stopper *util.Stopper) {
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
	GetTimeSeriesData() []proto.TimeSeriesData
}

// poller maintains information for a polling process started by PollSource().
type poller struct {
	db        *DB
	source    DataSource
	frequency time.Duration
	r         Resolution
	stopper   *util.Stopper
}

// start begins the goroutine for this poller, which will periodically request
// time series data from the DataSource and store it.
func (p *poller) start() {
	p.stopper.RunWorker(func() {
		ticker := time.NewTicker(p.frequency)
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
	if !p.stopper.StartTask() {
		return
	}
	defer p.stopper.FinishTask()

	data := p.source.GetTimeSeriesData()
	if len(data) == 0 {
		return
	}

	if err := p.db.storeData(p.r, data); err != nil {
		log.Warningf("error writing time series data: %s", err.Error())
	}
}

// storeData writes the supplied time series data to the cockroach server.
// Stored data will be sampled at the supplied resolution.
func (db *DB) storeData(r Resolution, data []proto.TimeSeriesData) error {
	var kvs []proto.KeyValue

	// Process data collection: data is converted to internal format, and a key
	// is generated for each internal message.
	for _, d := range data {
		idatas, err := d.ToInternal(r.KeyDuration(), r.SampleDuration())
		if err != nil {
			return err
		}
		for _, idata := range idatas {
			value, err := idata.ToValue()
			if err != nil {
				return err
			}
			kvs = append(kvs, proto.KeyValue{
				Key:   MakeDataKey(d.Name, d.Source, r, idata.StartTimestampNanos),
				Value: *value,
			})
		}
	}

	// Send the individual internal merge requests.
	// TODO(mrtracy): In the likely event that there are multiple values to
	// merge, they should be batched together instead of being called
	// individually. However, InternalBatchRequest currently does not support
	// InternalMergeRequest, probably because it cannot be part of a
	// transaction. Look into batching this.
	for _, kv := range kvs {
		if err := db.kv.Run(client.Call{
			Args: &proto.InternalMergeRequest{
				RequestHeader: proto.RequestHeader{
					Key: kv.Key,
				},
				Value: kv.Value,
			},
			Reply: &proto.InternalMergeResponse{}}); err != nil {
			return err
		}
	}

	return nil
}
