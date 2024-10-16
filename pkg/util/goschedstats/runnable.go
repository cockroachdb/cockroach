// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goschedstats

import "time"

// RunnableCountCallback is provided the current value of runnable goroutines,
// GOMAXPROCS, and the current sampling period.
type RunnableCountCallback func(numRunnable int, numProcs int, samplePeriod time.Duration)
