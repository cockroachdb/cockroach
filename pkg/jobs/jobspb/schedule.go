// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobspb

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"

// ScheduleID is the ID of a job schedule.
type ScheduleID = catpb.ScheduleID

// InvalidScheduleID is a constant indicating the schedule ID is not valid.
const InvalidScheduleID = catpb.InvalidScheduleID
