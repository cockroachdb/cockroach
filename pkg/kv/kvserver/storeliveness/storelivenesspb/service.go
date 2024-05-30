// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storelivenesspb

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// Epoch is an epoch in the Store Liveness fabric, referencing an uninterrupted
// period of support from one store to another.
type Epoch int64

// Expiration is a timestamp indicating the extent of support from one store to
// another in the Store Liveness fabric within a given epoch. This expiration
// may be extended through the provision of additional support.
type Expiration hlc.Timestamp
