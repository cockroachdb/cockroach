// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// Label values for the "phase" tag on Datadog events.
const (
	phaseRun             = "run"
	phaseCleanup         = "cleanup"
	phaseDependencyCheck = "dependency-check"
)

// operationEventEmitter encapsulates Datadog event emission for a single
// operation run. It captures invariant parameters at construction time so
// call sites stay concise. Events carry phase, result, owner, and worker
// tags for dashboard filtering and counting.
type operationEventEmitter struct {
	ctx         context.Context
	client      *datadogV1.EventsApi
	opSpec      *registry.OperationSpec
	clusterName string
	operationID uint64
	owner       string
	workerLabel string
	tags        []string
}

func newOperationEventEmitter(
	ctx context.Context,
	client *datadogV1.EventsApi,
	opSpec *registry.OperationSpec,
	clusterName string,
	operationID uint64,
	owner string,
	workerLabel string,
	tags []string,
) *operationEventEmitter {
	return &operationEventEmitter{
		ctx:         ctx,
		client:      client,
		opSpec:      opSpec,
		clusterName: clusterName,
		operationID: operationID,
		owner:       owner,
		workerLabel: workerLabel,
		tags:        tags,
	}
}

// EmitStarted emits an info event when the run phase begins.
func (e *operationEventEmitter) EmitStarted() {
	e.emit(eventOpStarted, "started", nil)
}

// EmitCompleted emits an event when the run phase ends. result should be
// one of "success", "failed", or "panicked".
func (e *operationEventEmitter) EmitCompleted(result string, failures []error) {
	e.emit(eventOpCompleted, result, failures)
}

// EmitCleanupCompleted emits an event when cleanup ends. result should be
// one of "success", "failed", or "skipped".
func (e *operationEventEmitter) EmitCleanupCompleted(result string, failures []error) {
	e.emit(eventCleanupCompleted, result, failures)
}

// EmitDepCheckFailed emits a warning event when a dependency check fails
// or errors. result should be "failed" or "error".
func (e *operationEventEmitter) EmitDepCheckFailed(result string, failures []error) {
	e.emit(eventDepCheckFailed, result, failures)
}

func (e *operationEventEmitter) emit(eventType ddEventType, result string, failures []error) {
	// The context must be configured to communicate with Datadog.
	_, hasAPIKeys := e.ctx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	_, hasServerVariables := e.ctx.Value(datadog.ContextServerVariables).(map[string]string)
	if !hasAPIKeys || !hasServerVariables {
		return
	}

	var phase string
	alertType := datadogV1.EVENTALERTTYPE_INFO

	switch eventType {
	case eventOpStarted:
		phase = phaseRun
		alertType = datadogV1.EVENTALERTTYPE_INFO
	case eventOpCompleted:
		phase = phaseRun
		if result == resultSuccess {
			alertType = datadogV1.EVENTALERTTYPE_SUCCESS
		} else {
			alertType = datadogV1.EVENTALERTTYPE_ERROR
		}
	case eventCleanupCompleted:
		phase = phaseCleanup
		if result == cleanupResultFailed {
			alertType = datadogV1.EVENTALERTTYPE_ERROR
		}
	case eventDepCheckFailed:
		phase = phaseDependencyCheck
		alertType = datadogV1.EVENTALERTTYPE_WARNING
	}

	title := fmt.Sprintf("op %s phase:%s result:%s", e.opSpec.Name, phase, result)
	hostname, _ := os.Hostname()

	text := fmt.Sprintf(
		"cluster: %s\noperation: %s\nowner: %s\nworker: %s\nresult: %s",
		e.clusterName, e.opSpec.Name, e.owner, e.workerLabel, result,
	)
	if errorDetails := formatFailures(failures); errorDetails != "" {
		text += fmt.Sprintf("\n\nerror details:\n%s", errorDetails)
	}

	tags := append(e.tags,
		fmt.Sprintf("operation-name:%s", e.opSpec.Name),
		fmt.Sprintf("phase:%s", phase),
		fmt.Sprintf("result:%s", result),
		fmt.Sprintf("owner:%s", e.owner),
		fmt.Sprintf("worker:%s", e.workerLabel),
	)

	// Best effort — ignore return values.
	_, _, _ = e.client.CreateEvent(e.ctx, datadogV1.EventCreateRequest{
		AggregationKey: datadog.PtrString(fmt.Sprintf("operation-%d", e.operationID)),
		AlertType:      &alertType,
		DateHappened:   datadog.PtrInt64(timeutil.Now().Unix()),
		Host:           &hostname,
		SourceTypeName: datadog.PtrString("roachtest"),
		Tags:           tags,
		Text:           text,
		Title:          title,
	})
}

// formatFailures returns a human-readable summary of operation failures.
func formatFailures(failures []error) string {
	if len(failures) == 0 {
		return ""
	}
	var sb strings.Builder
	for i, f := range failures {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(fmt.Sprintf("failure #%d: %+v", i+1, f))
	}
	return sb.String()
}
