// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/errors"
	"github.com/codahale/hdrhistogram"
)

// SpecWarehouseFactor is the default maximum per-warehouse newOrder
// throughput per second used to compute the maximum throughput value for a
// given warehouse count. This value is provided in the TPC-C spec.
const SpecWarehouseFactor = 12.86

// DeckWarehouseFactor is the warehouse factor to be used with the workload deck
// implemented in this package. This value differs from the spec's provided
// value as the deck mechanism used by workload differs from the default
// workload but is an acceptable alternative. This is the default value.
//
// The 12.605 is computed from the operation mix and the number of secs
// it takes to cycle through a deck:
//
//   10*(18+12) + 10*(3+12) + 1*(2+10) + 1*(2+5) + 1*(2+5) = 476
//
// 10 workers per warehouse times 10 newOrder ops per deck results in:
//
//   (10*10)/(476/60) = 12.605...
//
const DeckWarehouseFactor = 12.605

// PassingEfficiency is a percentage of the theoretical maximum tpmC required
// to pass TPC-C.
const PassingEfficiency = 85.0

// These values represent the maximum allowable 90th-%ile latency for the
// given queries as specified in section 5.2.5.7 of the TPC-C spec.
var passing90ThPercentile = map[string]time.Duration{
	"newOrder":    5 * time.Second,
	"payment":     5 * time.Second,
	"orderStatus": 5 * time.Second,
	"delivery":    5 * time.Second,
	"stockLevel":  20 * time.Second,
}

// Result represents the outcome of a TPCC run.
type Result struct {
	// ActiveWarehouses is the number of warehouses used in the TPC-C run.
	ActiveWarehouses int

	// Cumulative maps from query name to the cumulative response time
	// histogram for the TPC-C run.
	Cumulative map[string]*hdrhistogram.Histogram

	// Elapsed is the amount of time captured by the Cumulative.
	Elapsed time.Duration

	// WarehouseFactor is the maximal number of newOrder transactions per second
	// per Warehouse. If zero it defaults to DeckWarehouseFactor which is derived
	// from this workload. The value is used to compute the efficiency of the run.
	WarehouseFactor float64
}

// MergeResults returns a result value constructed by merging the arguments.
// It should only be used if the number of ActiveWarehouses matches and will
// panic if they differ. Elapsed is computed as the max of all elapsed values
// and makes the assumption that the distributions roughly overlap in time.
// This should be used when merging from multiple load generators during the
// same test run.
func MergeResults(results ...*Result) *Result {
	if len(results) == 0 {
		return nil
	}
	base := &Result{
		ActiveWarehouses: results[0].ActiveWarehouses,
		Cumulative: make(map[string]*hdrhistogram.Histogram,
			len(results[0].Cumulative)),
		WarehouseFactor: results[0].WarehouseFactor,
	}
	for _, r := range results {
		if r.Elapsed > base.Elapsed {
			base.Elapsed = r.Elapsed
		}
		if r.ActiveWarehouses != base.ActiveWarehouses {
			panic(errors.Errorf("cannot merge histograms with different "+
				"ActiveWarehouses values: got both %v and %v",
				r.ActiveWarehouses, base.ActiveWarehouses))
		}
		for q, h := range r.Cumulative {
			if cur, exists := base.Cumulative[q]; exists {
				cur.Merge(h)
			} else {
				base.Cumulative[q] = histogram.Copy(h)
			}
		}
	}
	return base
}

// NewResult constructs a new Result.
func NewResult(
	activeWarehouses int,
	warehouseFactor float64,
	elapsed time.Duration,
	cumulative map[string]*hdrhistogram.Histogram,
) *Result {
	return &Result{
		ActiveWarehouses: activeWarehouses,
		WarehouseFactor:  warehouseFactor,
		Elapsed:          elapsed,
		Cumulative:       cumulative,
	}
}

// NewResultWithSnapshots creates a new result from a deserialized set of
// histogram snapshots.
func NewResultWithSnapshots(
	activeWarehouses int, warehouseFactor float64, snapshots map[string][]histogram.SnapshotTick,
) *Result {
	var start time.Time
	var end time.Time
	ret := make(map[string]*hdrhistogram.Histogram, len(snapshots))
	for n, snaps := range snapshots {
		var cur *hdrhistogram.Histogram
		for _, s := range snaps {
			h := hdrhistogram.Import(s.Hist)
			if cur == nil {
				cur = h
			} else {
				cur.Merge(h)
			}
			if start.IsZero() || s.Now.Before(start) {
				start = s.Now
			}
			if sEnd := s.Now.Add(s.Elapsed); end.IsZero() || sEnd.After(end) {
				end = sEnd
			}
		}
		ret[n] = cur
	}
	return NewResult(activeWarehouses, warehouseFactor, end.Sub(start), ret)
}

// TpmC returns a tpmC value with a warehouse factor of 12.86.
// TpmC will panic if r does not contain a "newOrder" histogram in Cumulative.
func (r *Result) TpmC() float64 {
	no := r.Cumulative["newOrder"]
	if no == nil {
		return 0
	}
	return float64(no.TotalCount()) / (r.Elapsed.Seconds() / 60)
}

// Efficiency returns the efficiency of a TPC-C run.
// It relies on the WarehouseFactor which defaults to DeckWarehouseFactor.
func (r *Result) Efficiency() float64 {
	tpmC := r.TpmC()
	warehouseFactor := r.WarehouseFactor
	if warehouseFactor == 0 {
		warehouseFactor = DeckWarehouseFactor
	}
	return (100 * tpmC) / (warehouseFactor * float64(r.ActiveWarehouses))
}

// FailureError returns nil if the Result is passing or an error describing
// the failure if the result is failing.
func (r *Result) FailureError() error {
	if _, newOrderExists := r.Cumulative["newOrder"]; !newOrderExists {
		return errors.Errorf("no newOrder data exists")
	}

	// Collect all failing criteria errors into errs so that the returned error
	// contains information about all of the failures.
	var err error
	if eff := r.Efficiency(); eff < PassingEfficiency {
		err = errors.CombineErrors(err,
			errors.Errorf("efficiency value of %v is below passing threshold of %v",
				eff, PassingEfficiency))
	}
	for query, max90th := range passing90ThPercentile {
		h, exists := r.Cumulative[query]
		if !exists {
			return errors.Errorf("no %v data exists", query)
		}
		if v := time.Duration(h.ValueAtQuantile(.9)); v > max90th {
			err = errors.CombineErrors(err,
				errors.Errorf("90th percentile latency for %v at %v exceeds passing threshold of %v",
					query, v, max90th))
		}
	}
	return err
}
