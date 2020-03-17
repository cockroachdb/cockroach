// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobspb

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
)

// Details is a marker interface for job details proto structs.
type Details interface{}

var _ Details = BackupDetails{}
var _ Details = RestoreDetails{}
var _ Details = SchemaChangeDetails{}
var _ Details = ChangefeedDetails{}
var _ Details = CreateStatsDetails{}
var _ Details = SchemaChangeGCDetails{}

// ProgressDetails is a marker interface for job progress details proto structs.
type ProgressDetails interface{}

var _ ProgressDetails = BackupProgress{}
var _ ProgressDetails = RestoreProgress{}
var _ ProgressDetails = SchemaChangeProgress{}
var _ ProgressDetails = ChangefeedProgress{}
var _ ProgressDetails = CreateStatsProgress{}
var _ ProgressDetails = SchemaChangeGCProgress{}

// Type returns the payload's job type.
func (p *Payload) Type() Type {
	return DetailsType(p.Details)
}

// DetailsType returns the type for a payload detail.
func DetailsType(d isPayload_Details) Type {
	switch d := d.(type) {
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
	case *Payload_CreateStats:
		createStatsName := d.CreateStats.Name
		if createStatsName == stats.AutoStatsName {
			return TypeAutoCreateStats
		}
		return TypeCreateStats
	case *Payload_SchemaChangeGC:
		return TypeSchemaChangeGC
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
	case CreateStatsProgress:
		return &Progress_CreateStats{CreateStats: &d}
	case SchemaChangeGCProgress:
		return &Progress_SchemaChangeGC{SchemaChangeGC: &d}
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
	case *Payload_CreateStats:
		return *d.CreateStats
	case *Payload_SchemaChangeGC:
		return *d.SchemaChangeGC
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
	case *Progress_CreateStats:
		return *d.CreateStats
	case *Progress_SchemaChangeGC:
		return *d.SchemaChangeGC
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
	case CreateStatsDetails:
		return &Payload_CreateStats{CreateStats: &d}
	case SchemaChangeGCDetails:
		return &Payload_SchemaChangeGC{SchemaChangeGC: &d}
	default:
		panic(fmt.Sprintf("jobs.WrapPayloadDetails: unknown details type %T", d))
	}
}

// ChangefeedTargets is a set of id targets with metadata.
type ChangefeedTargets map[sqlbase.ID]ChangefeedTarget

// SchemaChangeDetailsFormatVersion is the format version for
// SchemaChangeDetails.
type SchemaChangeDetailsFormatVersion uint32

const (
	// BaseFormatVersion corresponds to the initial version of
	// SchemaChangeDetails, intended for the original version of schema change
	// jobs which were meant to be updated by a SchemaChanger instead of being run
	// as jobs by the job registry.
	BaseFormatVersion SchemaChangeDetailsFormatVersion = iota
	// JobResumerFormatVersion corresponds to the introduction of the schema
	// change job resumer. This version introduces the TableID and MutationID
	// fields, and, more generally, flags the job as being suitable for the job
	// registry to adopt.
	JobResumerFormatVersion

	// Silence unused warning.
	_ = BaseFormatVersion
)
