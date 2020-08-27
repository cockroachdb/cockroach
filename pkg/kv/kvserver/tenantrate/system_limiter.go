// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantrate

import "context"

// systemLimiter implements Limiter for the use of tracking metrics for the
// system tenant. It does not actually perform any rate-limiting.
type systemLimiter struct {
	tenantMetrics
}

func (s systemLimiter) Wait(ctx context.Context, isWrite bool, writeBytes int64) error {
	if isWrite {
		s.writeRequestsAdmitted.Inc(1)
	} else {
		s.readRequestsAdmitted.Inc(1)
	}
	s.writeBytesAdmitted.Inc(writeBytes)
	return nil
}

func (s systemLimiter) RecordRead(ctx context.Context, readBytes int64) {
	s.readBytesAdmitted.Inc(readBytes)
}

var _ Limiter = (*systemLimiter)(nil)
