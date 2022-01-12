// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package contention

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// TxnIDResolutionInterval is the cluster setting that controls how often the
// Transaction ID Resolution is performed.
var TxnIDResolutionInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"sql.contention.event_store.resolution_interval",
	"the interval at which transaction fingerprint ID resolution is "+
		"performed (set to 0 to disable)",
	time.Second*30,
)

// StoreCapacity is the cluster setting that controls the
// maximum size of the contention event store.
var StoreCapacity = settings.RegisterByteSizeSetting(
	settings.TenantWritable,
	"sql.contention.event_store.capacity",
	"the in-memory storage capacity per-node of contention event store",
	64*1024*1024, // 64 MB per node.
).WithPublic()
