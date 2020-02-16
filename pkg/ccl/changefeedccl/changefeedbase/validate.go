// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedbase

import (
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// ValidateTable validates that a table descriptor can be watched by a CHANGEFEED.
func ValidateTable(targets jobspb.ChangefeedTargets, tableDesc *sqlbase.TableDescriptor) error {
	t, ok := targets[tableDesc.ID]
	if !ok {
		return errors.Errorf(`unwatched table: %s`, tableDesc.Name)
	}

	// Technically, the only non-user table known not to work is system.jobs
	// (which creates a cycle since the resolved timestamp high-water mark is
	// saved in it), but there are subtle differences in the way many of them
	// work and this will be under-tested, so disallow them all until demand
	// dictates.
	if tableDesc.ID < keys.MinUserDescID {
		return errors.Errorf(`CHANGEFEEDs are not supported on system tables`)
	}
	if tableDesc.IsView() {
		return errors.Errorf(`CHANGEFEED cannot target views: %s`, tableDesc.Name)
	}
	if tableDesc.IsVirtualTable() {
		return errors.Errorf(`CHANGEFEED cannot target virtual tables: %s`, tableDesc.Name)
	}
	if tableDesc.IsSequence() {
		return errors.Errorf(`CHANGEFEED cannot target sequences: %s`, tableDesc.Name)
	}
	if len(tableDesc.Families) != 1 {
		return errors.Errorf(
			`CHANGEFEEDs are currently supported on tables with exactly 1 column family: %s has %d`,
			tableDesc.Name, len(tableDesc.Families))
	}

	if tableDesc.State == sqlbase.TableDescriptor_DROP {
		return errors.Errorf(`"%s" was dropped or truncated`, t.StatementTimeName)
	}
	if tableDesc.Name != t.StatementTimeName {
		return errors.Errorf(`"%s" was renamed to "%s"`, t.StatementTimeName, tableDesc.Name)
	}

	// TODO(mrtracy): re-enable this when allow-backfill option is added.
	// if tableDesc.HasColumnBackfillMutation() {
	// 	return errors.Errorf(`CHANGEFEEDs cannot operate on tables being backfilled`)
	// }

	return nil
}
