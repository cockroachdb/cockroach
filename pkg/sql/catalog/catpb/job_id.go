// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

import "strconv"

// JobID is the ID of a job. It is defined here and imported in jobspb because
// jobs are referenced in descriptors and also jobs reference parts of
// descriptors. This avoids any dependency cycles.
type JobID int64

// InvalidJobID is the zero value for JobID corresponding to no job.
const InvalidJobID JobID = 0

// SafeValue implements the redact.SafeValue interface.
func (j JobID) SafeValue() {}

// String implements the fmt.Stringer interface.
func (j JobID) String() string {
	return strconv.Itoa(int(j))
}

// ScheduleID is the ID of a job schedule. It is defined here and imported in
// jobspb for the same reasons as JobID. Additionally, it's helpful to keep the
// two type definitions close together.
type ScheduleID int64

// InvalidScheduleID is a constant indicating the schedule ID is not valid.
const InvalidScheduleID ScheduleID = 0

// SafeValue implements the redact.SafeValue interface.
func (s ScheduleID) SafeValue() {}

// String implements the fmt.Stringer interface.
func (s ScheduleID) String() string {
	return strconv.Itoa(int(s))
}
