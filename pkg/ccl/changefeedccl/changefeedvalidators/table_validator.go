// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedvalidators

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/errors"
)

// ValidateTable validates that a table descriptor can be watched by a CHANGEFEED.
func ValidateTable(
	targets changefeedbase.Targets,
	tableDesc catalog.TableDescriptor,
	canHandle changefeedbase.CanHandle,
) error {
	if err := validateTable(targets, tableDesc, canHandle); err != nil {
		return changefeedbase.WithTerminalError(err)
	}
	return nil
}

func validateTable(
	targets changefeedbase.Targets,
	tableDesc catalog.TableDescriptor,
	canHandle changefeedbase.CanHandle,
) error {
	// Technically, the only non-user table known not to work is system.jobs
	// (which creates a cycle since the resolved timestamp high-water mark is
	// saved in it), but our philosophy currently is that any use case for
	// changefeeds on system tables would be better served by e.g. better
	// logging and monitoring features.
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
	if tableDesc.Offline() {
		return errors.Errorf("CHANGEFEED cannot target offline table: %s (offline reason: %q)", tableDesc.GetName(), tableDesc.GetOfflineReason())
	}
	found, err := targets.EachHavingTableID(tableDesc.GetID(), func(t changefeedbase.Target) error {
		if tableDesc.Dropped() {
			return errors.Errorf(`"%s" was dropped`, t.StatementTimeName)
		}
		switch t.Type {
		case jobspb.ChangefeedTargetSpecification_PRIMARY_FAMILY_ONLY:
			if len(tableDesc.GetFamilies()) != 1 {
				return errors.Errorf(
					`CHANGEFEED created on a table with a single column family (%s) cannot now target a table with %d families. targets: %+v, tableDesc: %+v`,
					tableDesc.GetName(), len(tableDesc.GetFamilies()), targets, tableDesc)
			}
		case jobspb.ChangefeedTargetSpecification_EACH_FAMILY:
			if !canHandle.MultipleColumnFamilies && len(tableDesc.GetFamilies()) != 1 {
				return errors.Errorf(
					`CHANGEFEED targeting a table (%s) with multiple column families requires WITH %s and will emit multiple events per row.`,
					tableDesc.GetName(), changefeedbase.OptSplitColumnFamilies)
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
		return nil
	})
	if !found {
		return errors.Errorf(`unwatched table: %s`, tableDesc.GetName())
	}
	for _, requiredColumn := range canHandle.RequiredColumns {
		if catalog.FindColumnByName(tableDesc, requiredColumn) == nil {
			return errors.Errorf("required column %s not present on table %s", requiredColumn, tableDesc.GetName())
		}
	}

	return err
}

// WarningsForTable returns any known nonfatal issues with running a changefeed on this kind of table.
func WarningsForTable(
	tableDesc catalog.TableDescriptor, canHandle changefeedbase.CanHandle,
) []error {
	warnings := []error{}
	if !canHandle.VirtualColumns {
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
