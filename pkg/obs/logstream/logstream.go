// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstream

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
		tenantRouters map[roachpb.TenantID]*asyncProcessorRouter
	}
}

var _ log.StructuredLogProcessor = (*streamController)(nil)

func newStreamController() *streamController {
	sc := &streamController{}
	sc.rmu.tenantRouters = make(map[roachpb.TenantID]*asyncProcessorRouter)
	return sc
}

// Inject our controller to pkg/util/log as a dependency. This allows us to avoid cumbersome circular dependency
// issues by enabling us to develop outside of pkg/util/log, which is pretty much imported by every package
// in CRDB.
func init() {
	log.SetStructuredLogProcessor(controller)
}

// Processor defines internally how a component can process streams of structured log events as they're
// passed to log.Structured.
//
// Implementations are responsible for making type assertions, while the logstream package makes the guarantee
// that only events of a single type will be routed to them.
//
// Once implemented, register your Processor implementation via RegisterProcessor. Each tenant is responsible
// for registering their own Processor instance.
type Processor interface {
	// Process processes a single log stream event. All incoming events are guaranteed to match the
	// type indicated by the log.StructuredLogMeta when registering via RegisterProcessor.
	Process(context.Context, any) error
}

// RegisterProcessor registers the given Processor to consume all log stream events of the given EventType,
// logged via log.Structured.
//
// The processing is done asynchronously from the log.Structured call, which simply buffers the event.
// Each tenant has their own processor(s) and async buffer, meaning each tenant is responsible for
// making their own calls to RegisterProcessor.
func RegisterProcessor(
	ctx context.Context, stopper *stop.Stopper, logMeta log.StructuredLogMeta, processor Processor,
) {
	if controller == nil {
		panic(errors.AssertionFailedf("attempted to registry logstream processor before controller was initialized"))
	}
	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tID = roachpb.SystemTenantID
	}
	controller.rmu.Lock()
	defer controller.rmu.Unlock()
	tenantRouter, ok := controller.rmu.tenantRouters[tID]
	if !ok {
		// TODO(abarganier): config knobs around flush trigger criteria.
		tenantRouter = newAsyncProcessorRouter(
			10*time.Second,
			100,  /* triggerLen */
			1000, /* maxLen */
		)
		controller.rmu.tenantRouters[tID] = tenantRouter
		if err := tenantRouter.Start(ctx, stopper); err != nil {
			panic(errors.AssertionFailedf(
				"failed to start asyncProcessorRouter for tenant ID %d, event type %s", tID, logMeta))
		}
	}
	tenantRouter.register(logMeta.EventType, processor)
}

// Process implements the log.StructuredLogProcessor interface.
//
// logstream.Process enforces tenant-separation for buffering & processing of events. The provided
// context.Context is used to determine the appropriate tenant.
func (l *streamController) Process(ctx context.Context, eventType log.EventType, e any) {
	tID, ok := roachpb.ClientTenantFromContext(ctx)
	if !ok {
		tID = roachpb.SystemTenantID
	}
	l.rmu.RLock()
	defer l.rmu.RUnlock()
	processor, ok := l.rmu.tenantRouters[tID]
	if !ok {
		// We don't mandate that a processor has been registered.
		return
	}
	if err := processor.Process(ctx, &typedEvent{
		eventType: eventType,
		event:     e,
	}); err != nil {
		log.Errorf(ctx, "error consuming exhaust: %v", err)
	}
}
