// Copyright (c) 2019 The Jaeger Authors.
// Copyright (c) 2017 Uber Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json

// ReferenceType is the reference type of one span to another
type ReferenceType string

// TraceID is the shared trace ID of all spans in the trace.
type TraceID string

// SpanID is the id of a span
type SpanID string

// ProcessID is a hashed value of the Process struct that is unique within the trace.
type ProcessID string

// ValueType is the type of a value stored in KeyValue struct.
type ValueType string

const (
	// ChildOf means a span is the child of another span
	ChildOf ReferenceType = "CHILD_OF"
	// FollowsFrom means a span follows from another span
	FollowsFrom ReferenceType = "FOLLOWS_FROM"

	// StringType indicates a string value stored in KeyValue
	StringType ValueType = "string"
	// BoolType indicates a Boolean value stored in KeyValue
	BoolType ValueType = "bool"
	// Int64Type indicates a 64bit signed integer value stored in KeyValue
	Int64Type ValueType = "int64"
	// Float64Type indicates a 64bit float value stored in KeyValue
	Float64Type ValueType = "float64"
	// BinaryType indicates an arbitrary byte array stored in KeyValue
	BinaryType ValueType = "binary"
)

// Trace is a list of spans
type Trace struct {
	TraceID   TraceID               `json:"traceID"`
	Spans     []Span                `json:"spans"`
	Processes map[ProcessID]Process `json:"processes"`
	Warnings  []string              `json:"warnings"`
}

// Span is a span denoting a piece of work in some infrastructure
// When converting to UI model, ParentSpanID and Process should be dereferenced into
// References and ProcessID, respectively.
// When converting to ES model, ProcessID and Warnings should be omitted. Even if
// included, ES with dynamic settings off will automatically ignore unneeded fields.
type Span struct {
	TraceID       TraceID     `json:"traceID"`
	SpanID        SpanID      `json:"spanID"`
	ParentSpanID  SpanID      `json:"parentSpanID,omitempty"` // deprecated
	Flags         uint32      `json:"flags,omitempty"`
	OperationName string      `json:"operationName"`
	References    []Reference `json:"references"`
	StartTime     uint64      `json:"startTime"` // microseconds since Unix epoch
	Duration      uint64      `json:"duration"`  // microseconds
	Tags          []KeyValue  `json:"tags"`
	Logs          []Log       `json:"logs"`
	ProcessID     ProcessID   `json:"processID,omitempty"`
	Process       *Process    `json:"process,omitempty"`
	Warnings      []string    `json:"warnings"`
}

// Reference is a reference from one span to another
type Reference struct {
	RefType ReferenceType `json:"refType"`
	TraceID TraceID       `json:"traceID"`
	SpanID  SpanID        `json:"spanID"`
}

// Process is the process emitting a set of spans
type Process struct {
	ServiceName string     `json:"serviceName"`
	Tags        []KeyValue `json:"tags"`
}

// Log is a log emitted in a span
type Log struct {
	Timestamp uint64     `json:"timestamp"`
	Fields    []KeyValue `json:"fields"`
}

// KeyValue is a key-value pair with typed value.
type KeyValue struct {
	Key   string      `json:"key"`
	Type  ValueType   `json:"type,omitempty"`
	Value interface{} `json:"value"`
}

// DependencyLink shows dependencies between services
type DependencyLink struct {
	Parent    string `json:"parent"`
	Child     string `json:"child"`
	CallCount uint64 `json:"callCount"`
}

// Operation defines the data in the operation response when query operation by service and span kind
type Operation struct {
	Name     string `json:"name"`
	SpanKind string `json:"spanKind"`
}
