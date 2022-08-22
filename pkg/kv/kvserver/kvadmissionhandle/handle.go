// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvadmissionhandle

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
)

// TODO(irfansharif): Consider moving KVAdmissionController and adjacent types
// into this package (and renaming it to just kvadmission).

// Handle groups data around some piece admitted work. Depending on the type of
// work, it holds (a) references to specific work queues, (b) state needed to
// inform said work queues of what work was done after the fact, and (c)
// information around how much work a request is allowed to do (used for
// cooperative scheduling with elastic CPU granters).
type Handle struct {
	TenantID                           roachpb.TenantID
	StoreAdmissionQ                    *admission.StoreWorkQueue
	StoreWorkHandle                    admission.StoreWorkHandle
	ElasticCPUWorkHandle               admission.ElasticCPUWorkHandle
	CallAdmittedWorkDoneOnKVAdmissionQ bool
}

// OverElasticCPULimit is used to check we're over the allotted elastic CPU
// tokens. Integrated callers are expected to invoke this in tight loops (we
// assume most callers are CPU-intensive and thus have tight loops somewhere)
// and bail once done.
//
// TODO(irfansharif): Could this be made smarter/structured as an iterator?
// Perhaps auto-estimating the per-loop-iteration time and only retrieving the
// running time only after the estimated "iters until over limit" has passed. Do
// only if we're sensitive to per-iteration check overhead.
func (h *Handle) OverElasticCPULimit() bool {
	overLimit, _ := h.ElasticCPUWorkHandle.OverLimit()
	return overLimit
}

type handleKey struct{}

// ContextWithHandle returns a Context wrapping the supplied kvadmission handle.
func ContextWithHandle(ctx context.Context, h Handle) context.Context {
	return context.WithValue(ctx, handleKey{}, h)
}

// HandleFromContext returns the kvadmission handle contained in the Context, if
// any.
func HandleFromContext(ctx context.Context) Handle {
	val := ctx.Value(handleKey{})
	h, ok := val.(Handle)
	if !ok {
		return Handle{}
	}
	return h
}
