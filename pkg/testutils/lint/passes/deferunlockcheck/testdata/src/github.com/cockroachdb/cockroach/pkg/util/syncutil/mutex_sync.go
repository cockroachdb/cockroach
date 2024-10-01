// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syncutil

import "sync"

type Mutex struct {
	sync.Mutex
}

type RWMutex struct {
	sync.RWMutex
}
