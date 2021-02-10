// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
)

// restorationData specifies the data that is to be restored in a restoration flow.
// It can specify spans to be restored, as well as system tables that should be
// restored in the case of cluster backups.
// It also includes peripheral data, related to the data that is being restored
// such as rekey information and PK IDs to count the ingested rows.
type restorationData interface {
	// getSpans returns the data spans that we're restoring into this cluster.
	getSpans() []roachpb.Span

	// getSystemTables returns nil for non-cluster restores. It returns the
	// descriptors of the temporary system tables that should be restored into the
	// real table descriptors. The data for these temporary tables should be
	// restored in either this restorationData, or one that was previously restored.
	getSystemTables() []catalog.TableDescriptor

	// Peripheral data that is needed in the restoration flow relating to the data
	// included in this bundle.
	getRekeys() []roachpb.ImportRequest_TableRekey
	getPKIDs() map[uint64]bool

	// addTenant extends the set of data needed to restore to include a new tenant.
	addTenant(roachpb.TenantID)

	// isEmpty returns true iff there is any data to be restored.
	isEmpty() bool

	// isMainBundle returns if this data bundle should be the one that advances
	// the job's progress updates.
	isMainBundle() bool
}

// mainRestorationData is a data bundle that actually affects the highwater mark in
// the job's progress. We should only restore 1 of these.
type mainRestorationData struct {
	restorationDataBase
}

// mainRestorationData implements restorationData.
var _ restorationData = &mainRestorationData{}

// isMainBundle implements restorationData.
func (*mainRestorationData) isMainBundle() bool { return true }

type restorationDataBase struct {
	// spans is the spans included in this bundle.
	spans []roachpb.Span
	// rekeys maps old table IDs to their new table descriptor.
	rekeys []roachpb.ImportRequest_TableRekey
	// pkIDs stores the ID of the primary keys for all of the tables that we're
	// restoring for RowCount calculation.
	pkIDs map[uint64]bool

	// systemTables store the system tables that need to be restored for cluster
	// backups. Should be nil otherwise.
	systemTables []catalog.TableDescriptor
}

// restorationDataBase implements restorationData.
var _ restorationData = &restorationDataBase{}

// getRekeys implements restorationData.
func (b *restorationDataBase) getRekeys() []roachpb.ImportRequest_TableRekey {
	return b.rekeys
}

// getPKIDs implements restorationData.
func (b *restorationDataBase) getPKIDs() map[uint64]bool {
	return b.pkIDs
}

// getSpans implements restorationData.
func (b *restorationDataBase) getSpans() []roachpb.Span {
	return b.spans
}

// getSystemTables implements restorationData.
func (b *restorationDataBase) getSystemTables() []catalog.TableDescriptor {
	return b.systemTables
}

// addTenant implements restorationData.
func (b *restorationDataBase) addTenant(tenantID roachpb.TenantID) {
	prefix := keys.MakeTenantPrefix(tenantID)
	b.spans = append(b.spans, roachpb.Span{Key: prefix, EndKey: prefix.PrefixEnd()})
}

// isEmpty implements restorationData.
func (b *restorationDataBase) isEmpty() bool {
	return len(b.spans) == 0
}

// isMainBundle implements restorationData.
func (restorationDataBase) isMainBundle() bool { return false }
