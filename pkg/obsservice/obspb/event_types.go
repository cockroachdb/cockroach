// Copyright 2022 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obspb

// EventType identifies a type of event that the Obs Service can ingest.
type EventType string

const (
	// EventlogEvent represents general events about the cluster that historically
	// have been persisted inside CRDB in the system.eventlog table.
	EventlogEvent EventType = "eventlog"
)

// EventlogEventTypeAttribute represents the key of the attribute containing
// the event type of an EventlogEvent.
const EventlogEventTypeAttribute = "event_type"
