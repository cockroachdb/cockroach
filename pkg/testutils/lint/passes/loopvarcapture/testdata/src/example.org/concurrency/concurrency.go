// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
