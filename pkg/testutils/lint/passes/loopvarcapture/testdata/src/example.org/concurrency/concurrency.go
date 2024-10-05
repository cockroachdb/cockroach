// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency

import (
	"fmt"
	"math/rand"
)

type Group struct{}

func (g Group) Go(f func()) {
	go f()
}

func Go(f func()) {
	go f()
}

func GoWithError(f func()) error {
	if rand.Float64() < 0.5 {
		return fmt.Errorf("random error")
	}

	go f()
	return nil
}

func SafeFunction(f func()) {
	f()
}
