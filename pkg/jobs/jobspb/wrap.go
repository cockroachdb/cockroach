// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package jobspb

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Details is a marker interface for job details proto structs.
type Details interface{}

var _ Details = BackupDetails{}
var _ Details = RestoreDetails{}
var _ Details = SchemaChangeDetails{}
var _ Details = ChangefeedDetails{}

// ProgressDetails is a marker interface for job progress details proto structs.
type ProgressDetails interface{}

var _ ProgressDetails = BackupProgress{}
var _ ProgressDetails = RestoreProgress{}
var _ ProgressDetails = SchemaChangeProgress{}
var _ ProgressDetails = ChangefeedProgress{}

// Type returns the payload's job type.
func (p *Payload) Type() Type {
	return DetailsType(p.Details)
}

// DetailsType returns the type for a payload detail.
func DetailsType(d isPayload_Details) Type {
	switch d.(type) {
	case *Payload_Backup:
		return TypeBackup
	case *Payload_Restore:
		return TypeRestore
	case *Payload_SchemaChange:
		return TypeSchemaChange
	case *Payload_Import:
		return TypeImport
	case *Payload_Changefeed:
		return TypeChangefeed
	default:
		panic(fmt.Sprintf("Payload.Type called on a payload with an unknown details type: %T", d))
	}
}

// WrapProgressDetails wraps a ProgressDetails object in the protobuf wrapper
// struct necessary to make it usable as the Details field of a Progress.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapProgressDetails(details ProgressDetails) interface {
	isProgress_Details
} {
	switch d := details.(type) {
	case BackupProgress:
		return &Progress_Backup{Backup: &d}
	case RestoreProgress:
		return &Progress_Restore{Restore: &d}
	case SchemaChangeProgress:
		return &Progress_SchemaChange{SchemaChange: &d}
	case ImportProgress:
		return &Progress_Import{Import: &d}
	case ChangefeedProgress:
		return &Progress_Changefeed{Changefeed: &d}
	default:
		panic(fmt.Sprintf("WrapProgressDetails: unknown details type %T", d))
	}
}

// UnwrapDetails returns the details object stored within the payload's Details
// field, discarding the protobuf wrapper struct.
func (p *Payload) UnwrapDetails() Details {
	switch d := p.Details.(type) {
	case *Payload_Backup:
		return *d.Backup
	case *Payload_Restore:
		return *d.Restore
	case *Payload_SchemaChange:
		return *d.SchemaChange
	case *Payload_Import:
		return *d.Import
	case *Payload_Changefeed:
		return *d.Changefeed
	default:
		return nil
	}
}

// UnwrapDetails returns the details object stored within the progress' Details
// field, discarding the protobuf wrapper struct.
func (p *Progress) UnwrapDetails() ProgressDetails {
	switch d := p.Details.(type) {
	case *Progress_Backup:
		return *d.Backup
	case *Progress_Restore:
		return *d.Restore
	case *Progress_SchemaChange:
		return *d.SchemaChange
	case *Progress_Import:
		return *d.Import
	case *Progress_Changefeed:
		return *d.Changefeed
	default:
		return nil
	}
}

func (t Type) String() string {
	// Protobufs, by convention, use CAPITAL_SNAKE_CASE for enum identifiers.
	// Since Type's string representation is used as a SHOW JOBS output column, we
	// simply swap underscores for spaces in the identifier for very SQL-esque
	// names, like "BACKUP" and "SCHEMA CHANGE".
	return strings.Replace(Type_name[int32(t)], "_", " ", -1)
}

// WrapPayloadDetails wraps a Details object in the protobuf wrapper struct
// necessary to make it usable as the Details field of a Payload.
//
// Providing an unknown details type indicates programmer error and so causes a
// panic.
func WrapPayloadDetails(details Details) interface {
	isPayload_Details
} {
	switch d := details.(type) {
	case BackupDetails:
		return &Payload_Backup{Backup: &d}
	case RestoreDetails:
		return &Payload_Restore{Restore: &d}
	case SchemaChangeDetails:
		return &Payload_SchemaChange{SchemaChange: &d}
	case ImportDetails:
		return &Payload_Import{Import: &d}
	case ChangefeedDetails:
		return &Payload_Changefeed{Changefeed: &d}
	default:
		panic(fmt.Sprintf("jobs.WrapPayloadDetails: unknown details type %T", d))
	}
}

// Completed returns the total complete percent of processing this table. There
// are two phases: sampling, SST writing. The SST phase is divided into two
// stages: read CSV, write SST. Thus, the entire progress can be measured
// by (sampling + read csv + write sst) progress. Since there are multiple
// distSQL processors running these stages, we assign slots to each one, and
// they are in charge of updating their portion of the progress. Since we read
// over CSV files twice (once for sampling, once for SST writing), we must
// indicate which phase we are in. This is done using the SamplingProgress
// slice, which is empty if we are in the second stage (and can thus be
// implied as complete).
func (d ImportProgress) Completed() float32 {
	const (
		// These ratios are approximate after running simple benchmarks.
		samplingPhaseContribution = 0.1
		readStageContribution     = 0.65
		writeStageContribution    = 0.25
	)

	sum := func(fs []float32) float32 {
		var total float32
		for _, f := range fs {
			total += f
		}
		return total
	}
	sampling := sum(d.SamplingProgress) * samplingPhaseContribution
	if len(d.SamplingProgress) == 0 {
		// SamplingProgress is empty iff we are in the second phase. If so, the
		// first phase is implied as fully complete.
		sampling = samplingPhaseContribution
	}
	read := sum(d.ReadProgress) * readStageContribution
	write := sum(d.WriteProgress) * writeStageContribution
	completed := sampling + read + write
	// Float addition can round such that the sum is > 1.
	if completed > 1 {
		completed = 1
	}
	return completed
}

// ChangefeedTargets is a set of id targets with metadata.
type ChangefeedTargets map[sqlbase.ID]ChangefeedTarget
