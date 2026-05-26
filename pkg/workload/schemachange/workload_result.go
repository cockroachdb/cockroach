// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachange

import (
	"context"
	"fmt"
	"sync/atomic"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	trace2 "go.opentelemetry.io/otel/trace"
)

const (
	numSchemaOps               = "numSchemaOps"
	numSchemaOpsSucceeded      = "numSchemaOpsSucceeded"
	numSchemaOpsExpectedFailed = "numSchemaOpsExpectedToFail"
	txnCommitted               = "txnCommitted"
	schemaWorkerSpanName       = "schemachange.worker"
)

// schemaWorkloadResultAnnotator is a SpanProcessor that adds workload attributes
// to a span. The workload attributes are used to summarize various operations
// executed by the schema workload and are output to the log file. These attributes
// can be reviewed to get a summary of the type of operations executed by the random
// schema workload.
type schemaWorkloadResultAnnotator struct {
	// numSchemaOps indicates the total number of schema operations (aka statements) run.
	numSchemaOps atomic.Int64
	// numSchemaOpsSucceeded indicates the number of schema operations that succeeded. A value of 0
	// would indicate the workload was unable to generate any random operation that could succeed.
	numSchemaOpsSucceeded atomic.Int64
	// numSchemaOpsExpectedFailed indicates the number of schema operations that were expected to fail.
	// Ideally, we should have a healthy balance between numSchemaOpsSucceeded and numSchemaOpsExpectedFailed
	// in a run of the random schema workload.
	numSchemaOpsExpectedFailed atomic.Int64
	// numTxnsCommitted indicates the number of transactions that committed successfully in this workload run.
	numTxnsCommitted atomic.Int64
	// numTxnRollbacks indicates the number of transactions that were rolled back in this workload run.
	numTxnRollbacks atomic.Int64
	// numTxns indicates the total number of transactions that ran in this workload.
	// This value should be greater than zero.
	numTxns atomic.Int64

	// To provide more stats about the workload, add fields below. Any new field should also be added
	// to (schemaWorkloadResultAnnotator).OnEnd, (schemaWorkloadResultAnnotator).initWorkloadMetrics
	// and (schemaWorkloadResultAnnotator).logWorkloadStats to ensure it is printed in the workload logs.
}

// OnStart implements the SpanProcessor interface.
func (s *schemaWorkloadResultAnnotator) OnStart(_ context.Context, _ trace.ReadWriteSpan) {}

// Shutdown implements the SpanProcessor interface.
func (s *schemaWorkloadResultAnnotator) Shutdown(_ context.Context) error {
	return nil
}

// ForceFlush implements the SpanProcessor interface.
func (s *schemaWorkloadResultAnnotator) ForceFlush(_ context.Context) error {
	return nil
}

// OnEnd implements the SpanProcessor interface. It aggregates and logs metrics
// for the schema change workload. These metrics provide an overview of the work
// done by the random schema change workload run. These metrics can be viewed
// in the output log file to get an idea of the number and variety of operations
// performed by the workload. These can give some insight into how valuable the particular
// run of the schema change workload was. For example, if the workload did not execute a
// large number of schema change operations, that could be an indication that the workload
// is not producing meaningful runs even if the test is succeeding. These metrics can be
// used as a sanity check to ensure the workload is generating and running meaningful tests.
func (s *schemaWorkloadResultAnnotator) OnEnd(sp trace.ReadOnlySpan) {
	if sp.Name() == schemaWorkerSpanName {
		for _, attr := range sp.Attributes() {
			switch attr.Key {
			case numSchemaOps:
				s.numSchemaOps.Add(attr.Value.AsInt64())
			case numSchemaOpsSucceeded:
				s.numSchemaOpsSucceeded.Add(attr.Value.AsInt64())
			case numSchemaOpsExpectedFailed:
				s.numSchemaOpsExpectedFailed.Add(attr.Value.AsInt64())
			case txnCommitted:
				if attr.Value.AsBool() {
					s.numTxnsCommitted.Add(1)
				} else {
					s.numTxnRollbacks.Add(1)
				}
				s.numTxns.Add(1)
			}
		}
	}
}

// logWorkloadStats aggregates and logs metrics across all schema change workers.
// This should be called once at the end of a schemachange workload run.
func (s *schemaWorkloadResultAnnotator) logWorkloadStats(log *atomicLog) {
	log.printLn("Schema Workload Stats")
	log.printLn(fmt.Sprintf("Total Schema Statements Executed = %d", s.numSchemaOps.Load()))
	log.printLn(fmt.Sprintf("Total Schema Statements Succeeded = %d", s.numSchemaOpsSucceeded.Load()))
	log.printLn(fmt.Sprintf("Total Schema Statement Expected Failures = %d", s.numSchemaOpsExpectedFailed.Load()))
	log.printLn(fmt.Sprintf("Total Transactions Committed = %d", s.numTxnsCommitted.Load()))
	log.printLn(fmt.Sprintf("Total Transactions Rolled Back = %d", s.numTxnRollbacks.Load()))
	log.printLn(fmt.Sprintf("Total Transactions Executed = %d", s.numTxns.Load()))
}

// initWorkloadMetrics initializes metrics being captured for a schema change run.
func initWorkloadMetrics() map[string]attribute.Value {
	return map[string]attribute.Value{
		numSchemaOps:               attribute.Int64Value(0),
		numSchemaOpsSucceeded:      attribute.Int64Value(0),
		numSchemaOpsExpectedFailed: attribute.Int64Value(0),
		txnCommitted:               attribute.BoolValue(false),
	}
}

// incWorkloadMetric is a helper function to increment the value of a metric by one.
func incWorkloadMetric(metricName string, workloadMetrics map[string]attribute.Value) {
	metricValue := workloadMetrics[metricName].AsInt64() + 1
	workloadMetrics[metricName] = attribute.Int64Value(metricValue)
}

// endSchemaWorkerSpan is a helper function to attach workload metrics as attributes to a span.
func endSchemaWorkerSpan(span trace2.Span, workloadMetrics map[string]attribute.Value) {
	for name, value := range workloadMetrics {
		span.SetAttributes(attribute.KeyValue{Key: attribute.Key(name), Value: value})
	}
	span.End()
}
