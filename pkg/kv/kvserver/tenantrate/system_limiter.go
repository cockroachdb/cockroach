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

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcostmodel"
)

// systemLimiter implements Limiter for the use of tracking metrics for the
// system tenant. It does not actually perform any rate-limiting.
type systemLimiter struct {
	tenantMetrics
}

func (s systemLimiter) Wait(ctx context.Context, reqInfo tenantcostmodel.RequestInfo) error {
	if reqInfo.IsWrite() {
		s.writeBatchesAdmitted.Inc(1)
		s.writeRequestsAdmitted.Inc(reqInfo.WriteCount())
		s.writeBytesAdmitted.Inc(reqInfo.WriteBytes())
	}
	return nil
}

func (s systemLimiter) RecordRead(ctx context.Context, respInfo tenantcostmodel.ResponseInfo) {
	if respInfo.IsRead() {
		s.readBatchesAdmitted.Inc(1)
		s.readRequestsAdmitted.Inc(respInfo.ReadCount())
		s.readBytesAdmitted.Inc(respInfo.ReadBytes())
	}
}

var _ Limiter = (*systemLimiter)(nil)
