// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package coldata

import (
	"time"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
)

// Bools is a slice of bool.
type Bools []bool

// Int16s is a slice of int16.
type Int16s []int16

// Int32s is a slice of int32.
type Int32s []int32

// Int64s is a slice of int64.
type Int64s []int64

// Float64s is a slice of float64.
type Float64s []float64

// Decimals is a slice of apd.Decimal.
type Decimals []apd.Decimal

// Times is a slice of time.Time.
type Times []time.Time

// Durations is a slice of duration.Duration.
type Durations []duration.Duration

// Get returns the element at index idx of the vector.
func (c Bools) Get(idx int) bool { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Bools) Set(idx int, v bool) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Int16s) Get(idx int) int16 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int16s) Set(idx int, v int16) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Int32s) Get(idx int) int32 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int32s) Set(idx int, v int32) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Int64s) Get(idx int) int64 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Int64s) Set(idx int, v int64) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Float64s) Get(idx int) float64 { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Float64s) Set(idx int, v float64) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Decimals) Get(idx int) apd.Decimal { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Decimals) Set(idx int, v apd.Decimal) { c[idx].Set(&v) }

// Get returns the element at index idx of the vector.
func (c Times) Get(idx int) time.Time { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Times) Set(idx int, v time.Time) { c[idx] = v }

// Get returns the element at index idx of the vector.
func (c Durations) Get(idx int) duration.Duration { return c[idx] }

// Set sets the element at index idx of the vector.
func (c Durations) Set(idx int, v duration.Duration) { c[idx] = v }
