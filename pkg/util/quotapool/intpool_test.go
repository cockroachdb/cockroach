// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package quotapool_test

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
)

// logSlowAcquisition is an Option to log slow acquisitions.
var logSlowAcquisition = quotapool.OnSlowAcquisition(2*time.Second, quotapool.LogSlowAcquisition)
