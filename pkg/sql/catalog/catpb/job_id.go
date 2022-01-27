// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

// JobID is the ID of a job. It is defined here and imported in jobspb because
// jobs are referenced in descriptors and also jobs reference parts of
// descriptors. This avoids any dependency cycles.
type JobID int64

// InvalidJobID is the zero value for JobID corresponding to no job.
const InvalidJobID JobID = 0

// SafeValue implements the redact.SafeValue interface.
func (j JobID) SafeValue() {}
