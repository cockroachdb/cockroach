// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package transform

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	commonv1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/common/v1"
	logsv1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/logs/v1"
	resourcev1 "github.com/cockroachdb/cockroach/pkg/obsservice/obspb/opentelemetry-proto/resource/v1"
)

// LogRecordToEvent transforms a given LogRecord, with an accompanying
// Resource and Scope, into an internal Event message for further
// processing.
func LogRecordToEvent(
	ingestTime time.Time,
	resource *resourcev1.Resource,
	scope *commonv1.InstrumentationScope,
	logRecord logsv1.LogRecord,
) *obspb.Event {
	logRecord.ObservedTimeUnixNano = uint64(ingestTime.UnixNano())
	return &obspb.Event{
		Resource:  resource,
		Scope:     scope,
		LogRecord: logRecord,
	}
}
