// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package logstream

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// controller is the singleton streamController used by the exported interface
// of the logstream package.
var controller = newStreamController()

// streamController provides the primary functionality of the logstream package, which is built
// to route structured events logged via log.Structured to one or more processors, registered via
// RegisterProcessor. This is done using tenant separation.
type streamController struct {
	rmu struct {
		syncutil.RWMutex
		tenantProcessors map[roachpb.TenantID]*processorBuffer
	}
}

var _ log.StructuredLogProcessor = (*streamController)(nil)

func newStreamController() *streamController {
	sc := &streamController{}
	sc.rmu.tenantProcessors = make(map[roachpb.TenantID]*processorBuffer)
	return sc
}

// Inject our controller to pkg/util/log as a dependency.
func init() {
	log.SetStructuredLogProcessor(controller)
}

// RegisterProcessor registers the given Processor to consume all log stream events of
// the given EventType, logged via log.Structured.
//
// The processing is done asynchronously from the log.Structured call, which simply buffers the event.
// Each tenant has their own processor(s) and async buffer, meaning each tenant is responsible for
// making their own calls to RegisterProcessor.
func RegisterProcessor(ctx context.Context, eventType log.EventType, processor Processor) {
	if controller == nil {
		panic(errors.AssertionFailedf("attempted to registry logstream processor before controller was initialized"))
	}
	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tID = roachpb.SystemTenantID
	}
	controller.rmu.Lock()
	defer controller.rmu.Unlock()
	tenantProcessor, ok := controller.rmu.tenantProcessors[tID]
	if !ok {
		// TODO(abarganier): config knobs around flush trigger criteria.
		tenantProcessor = newProcessorBuffer(
			newLogTypeEventRouter(),
			10*time.Second,
			100,  /* triggerLen */
			1000, /* maxLen */
		)
		controller.rmu.tenantProcessors[tID] = tenantProcessor
		tenantProcessor.Start(ctx)
	}
	child, ok := tenantProcessor.child.(*LogTypeEventRouter)
	if !ok {
		panic(errors.AssertionFailedf("unexpected child type for structuredProcessorBuffer"))
	}
	child.register(ctx, eventType, processor)
}

// Process implements the log.StructuredLogProcessor interface.
func (l *streamController) Process(ctx context.Context, eventType log.EventType, e any) {
	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tID = roachpb.SystemTenantID
	}
	l.rmu.RLock()
	defer l.rmu.RUnlock()
	processor, ok := l.rmu.tenantProcessors[tID]
	if !ok {
		panic(errors.AssertionFailedf("processor not found for tenant ID: %v", tID))
	}
	if err := processor.Process(ctx, &TypedEvent{
		eventType: eventType,
		event:     e,
	}); err != nil {
		log.Errorf(ctx, "error consuming exhaust: %v", err)
	}
}
