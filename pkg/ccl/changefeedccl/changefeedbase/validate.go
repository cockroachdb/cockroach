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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
)

// ValidateTable validates that a table descriptor can be watched by a CHANGEFEED.
func ValidateTable(
	targets []jobspb.ChangefeedTargetSpecification,
	tableDesc catalog.TableDescriptor,
	opts map[string]string,
) error {
	var found bool
	for _, cts := range targets {
		var t jobspb.ChangefeedTargetSpecification
		if cts.TableID == tableDesc.GetID() {
			t = cts
			found = true
		} else {
			continue
		}

		// Technically, the only non-user table known not to work is system.jobs
		// (which creates a cycle since the resolved timestamp high-water mark is
		// saved in it), but there are subtle differences in the way many of them
		// work and this will be under-tested, so disallow them all until demand
		// dictates.
		if catalog.IsSystemDescriptor(tableDesc) {
			return errors.Errorf(`CHANGEFEEDs are not supported on system tables`)
		}
		if tableDesc.IsView() {
			return errors.Errorf(`CHANGEFEED cannot target views: %s`, tableDesc.GetName())
		}
		if tableDesc.IsVirtualTable() {
			return errors.Errorf(`CHANGEFEED cannot target virtual tables: %s`, tableDesc.GetName())
		}
		if tableDesc.IsSequence() {
			return errors.Errorf(`CHANGEFEED cannot target sequences: %s`, tableDesc.GetName())
		}
		switch t.Type {
		case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
			if len(tableDesc.GetFamilies()) != 1 {
				return errors.Errorf(
					`CHANGEFEED created on a table with a single column family (%s) cannot now target a table with %d families. targets: %+v, tableDesc: %+v`,
					tableDesc.GetName(), len(tableDesc.GetFamilies()), targets, tableDesc)
			}
		case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
			_, columnFamiliesOpt := opts[OptSplitColumnFamilies]
			if !columnFamiliesOpt && len(tableDesc.GetFamilies()) != 1 {
				return errors.Errorf(
					`CHANGEFEED targeting a table (%s) with multiple column families requires WITH %s and will emit multiple events per row.`,
					tableDesc.GetName(), OptSplitColumnFamilies)
			}
		case jobspb.ChangefeedTargetSpecification_COLUMN_FAMILY:
			cols := 0
			for _, family := range tableDesc.GetFamilies() {
				if family.Name == t.FamilyName {
					cols = len(family.ColumnIDs)
					break
				}
			}
			if cols == 0 {
				return errors.Errorf("CHANGEFEED targeting nonexistent or removed column family %s of table %s", t.FamilyName, tableDesc.GetName())
			}
		}

		if tableDesc.Dropped() {
			return errors.Errorf(`"%s" was dropped`, t.StatementTimeName)
		}

		if tableDesc.Offline() {
			return errors.Errorf("CHANGEFEED cannot target offline table: %s (offline reason: %q)", tableDesc.GetName(), tableDesc.GetOfflineReason())
		}
	}
	if !found {
		return errors.Errorf(`unwatched table: %s`, tableDesc.GetName())
	}

	return nil
}

// WarningsForTable returns any known nonfatal issues with running a changefeed on this kind of table.
func WarningsForTable(
	targets jobspb.ChangefeedTargets, tableDesc catalog.TableDescriptor, opts map[string]string,
) []error {
	warnings := []error{}
	if _, ok := opts[OptVirtualColumns]; !ok {
		for _, col := range tableDesc.AccessibleColumns() {
			if col.IsVirtual() {
				warnings = append(warnings,
					errors.Errorf("Changefeeds will filter out values for virtual column %s in table %s", col.ColName(), tableDesc.GetName()),
				)
			}
		}
	}
	if tableDesc.NumFamilies() > 1 {
		warnings = append(warnings,
			errors.Errorf("Table %s has %d underlying column families. Messages will be emitted separately for each family specified, or each family if none specified.",
				tableDesc.GetName(), tableDesc.NumFamilies(),
			))
	}
	return warnings
}
