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
