// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schematelemetry

import (
	"context"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/redact"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CollectClusterSchemaForTelemetry returns a projection of the cluster's SQL
// schema as of the provided system time, suitably filtered for the purposes of
// schema telemetry.
//
// The projection may be truncated, in which case a pseudo-random subset of
// records is selected. The seed for the randomization is derived from the UUID.
//
// This function is tested in the systemschema package.
//
// TODO(postamar): monitor memory usage
func CollectClusterSchemaForTelemetry(
	ctx context.Context,
	cfg *sql.ExecutorConfig,
	asOf hlc.Timestamp,
	snapshotID uuid.UUID,
	maxRecordsInSnapshot int,
) ([]logpb.EventPayload, error) {
	// Scrape the raw catalog.
	var raw nstree.Catalog
	if err := sql.DescsTxn(ctx, cfg, func(ctx context.Context, txn isql.Txn, col *descs.Collection) error {
		err := txn.KV().SetFixedTimestamp(ctx, asOf)
		if err != nil {
			return err
		}
		raw, err = col.GetAllFromStorageUnvalidated(ctx, txn.KV())
		return err
	}); err != nil {
		return nil, err
	}
	// Determine which parts of the catalog are in this snapshot.
	rng := rand.New(rand.NewSource(int64(snapshotID.ToUint128().Lo ^ snapshotID.ToUint128().Hi)))
	nsKeys, orphanedDescIDs, descIDsInSnapshot := truncatedCatalogKeys(raw, maxRecordsInSnapshot, rng)
	meta := &eventpb.SchemaSnapshotMetadata{
		CommonEventDetails: logpb.CommonEventDetails{
			Timestamp: asOf.WallTime,
		},
		SnapshotID:    snapshotID.String(),
		AsOfTimestamp: asOf.WallTime,
		NumRecords:    uint32(len(nsKeys) + orphanedDescIDs.Len()),
	}
	events := make([]logpb.EventPayload, 1, 1+meta.NumRecords)
	events[0] = meta
	newEvent := func(id descpb.ID) *eventpb.SchemaDescriptor {
		return &eventpb.SchemaDescriptor{
			CommonEventDetails: meta.CommonEventDetails,
			SnapshotID:         meta.SnapshotID,
			DescID:             uint32(id),
		}
	}
	// Redact the descriptors.
	redacted := make(map[descpb.ID]*eventpb.SchemaDescriptor, descIDsInSnapshot.Len())
	_ = raw.ForEachDescriptor(func(rd catalog.Descriptor) error {
		if !descIDsInSnapshot.Contains(rd.GetID()) {
			return nil
		}
		ev := newEvent(rd.GetID())
		redacted[rd.GetID()] = ev
		// Redact parts of the catalog which may contain PII.
		ev.Desc = rd.NewBuilder().BuildCreatedMutable().DescriptorProto()
		redactErrs := redact.Redact(ev.Desc)
		// Add all errors to the snapshot metadata event.
		for _, err := range redactErrs {
			err = errors.Wrapf(err, " %s %q (%d)", rd.DescriptorType(), rd.GetName(), rd.GetID())
			log.Errorf(ctx, "error during schema telemetry event generation: %v", err)
			meta.Errors = append(meta.Errors, err.Error())
		}
		return nil
	})
	// Add the log events for the descriptor entries with no namespace entry.
	orphanedDescIDs.ForEach(func(id descpb.ID) {
		events = append(events, redacted[id])
	})
	// Add the log events for each of the selected namespace entries.
	_ = raw.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
		if _, found := nsKeys[descpb.NameInfo{
			ParentID:       e.GetParentID(),
			ParentSchemaID: e.GetParentSchemaID(),
			Name:           e.GetName(),
		}]; !found {
			return nil
		}
		ev := newEvent(e.GetID())
		ev.ParentDatabaseID = uint32(e.GetParentID())
		ev.ParentSchemaID = uint32(e.GetParentSchemaID())
		ev.Name = e.GetName()
		if src, ok := redacted[e.GetID()]; ok {
			ev.Desc = src.Desc
		}
		events = append(events, ev)
		return nil
	})
	return events, nil
}

func truncatedCatalogKeys(
	raw nstree.Catalog, maxJoinedRecords int, rng *rand.Rand,
) (
	namespaceKeys map[descpb.NameInfo]struct{},
	orphanedDescIDs, descIDsInSnapshot catalog.DescriptorIDSet,
) {
	// Collect all the joined record keys using the input catalog.
	descIDs := raw.OrderedDescriptorIDs()
	type joinedRecordKey struct {
		nsKey catalog.NameKey
		id    descpb.ID
	}
	keys := make([]joinedRecordKey, 0, len(descIDs))
	{
		var idsInNamespace catalog.DescriptorIDSet
		_ = raw.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			idsInNamespace.Add(e.GetID())
			keys = append(keys, joinedRecordKey{
				nsKey: e,
				id:    e.GetID(),
			})
			return nil
		})
		_ = raw.ForEachDescriptor(func(desc catalog.Descriptor) error {
			if !idsInNamespace.Contains(desc.GetID()) {
				keys = append(keys, joinedRecordKey{id: desc.GetID()})
			}
			return nil
		})
	}
	// Truncate the input catalog if necessary.
	// Discard any excess keys at random.
	if len(keys) > maxJoinedRecords {
		rng.Shuffle(len(keys), func(i, j int) {
			k := keys[i]
			keys[i] = keys[j]
			keys[j] = k
		})
		keys = keys[:maxJoinedRecords]
	}
	// Return namespace keys and orphaned descriptor IDs.
	namespaceKeys = make(map[descpb.NameInfo]struct{}, len(keys))
	for _, k := range keys {
		descIDsInSnapshot.Add(k.id)
		if k.nsKey == nil {
			orphanedDescIDs.Add(k.id)
		} else {
			namespaceKeys[descpb.NameInfo{
				ParentID:       k.nsKey.GetParentID(),
				ParentSchemaID: k.nsKey.GetParentSchemaID(),
				Name:           k.nsKey.GetName(),
			}] = struct{}{}
		}
	}
	return namespaceKeys, orphanedDescIDs, descIDsInSnapshot
}
