// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storelivenesspb

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another. A store can unilaterally
// increment the epoch for which it requests support from another store (e.g.
// after a restart).
type Epoch int64

// SafeValue implements the redact.SafeValue interface.
func (e Epoch) SafeValue() {}
