// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobspb

import "github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"

// ScheduleID is the ID of a job schedule.
type ScheduleID = catpb.ScheduleID

// InvalidScheduleID is a constant indicating the schedule ID is not valid.
const InvalidScheduleID = catpb.InvalidScheduleID
