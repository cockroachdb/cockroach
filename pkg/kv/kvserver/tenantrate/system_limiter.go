// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tenantrate

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

// systemLimiter implements Limiter for the use of tracking metrics for the
// system tenant. It does not actually perform any rate-limiting.
type systemLimiter struct {
	tenantMetrics
}

func (s systemLimiter) Wait(ctx context.Context, reqInfo tenantcostmodel.BatchInfo) error {
	if reqInfo.WriteCount > 0 {
		s.writeBatchesAdmitted.Inc(1)
		s.writeRequestsAdmitted.Inc(reqInfo.WriteCount)
		s.writeBytesAdmitted.Inc(reqInfo.WriteBytes)
	}
	return nil
}

func (s systemLimiter) RecordRead(ctx context.Context, respInfo tenantcostmodel.BatchInfo) {
	if respInfo.ReadCount > 0 {
		s.readBatchesAdmitted.Inc(1)
		s.readRequestsAdmitted.Inc(respInfo.ReadCount)
		s.readBytesAdmitted.Inc(respInfo.ReadBytes)
	}
}

var _ Limiter = (*systemLimiter)(nil)
