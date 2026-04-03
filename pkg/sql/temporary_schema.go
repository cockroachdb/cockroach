// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessionmutator"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// TempObjectCleanupInterval is a ClusterSetting controlling how often
// temporary objects get cleaned up.
var TempObjectCleanupInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.temp_object_cleaner.cleanup_interval",
	"how often to clean up orphaned temporary objects",
	30*time.Minute,
	settings.WithPublic)

// TempObjectWaitInterval is a ClusterSetting controlling how long
// after a creation a temporary object will be cleaned up.
var TempObjectWaitInterval = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.temp_object_cleaner.wait_interval",
	"how long after creation a temporary object will be cleaned up",
	30*time.Minute,
	settings.WithPublic)

// TempObjectCleanupBatchSize is a ClusterSetting controlling how many
// temporary objects are dropped per transaction during session cleanup.
// Each batch produces a single DROP statement (e.g. DROP TABLE t1, t2, ..., tN)
// which creates one schema change job. Larger batches reduce the number of
// transactions and jobs but increase per-transaction intent footprint.
var TempObjectCleanupBatchSize = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.temp_object_cleaner.batch_size",
	"number of temporary objects to drop per transaction during cleanup",
	1000,
	settings.IntInRange(1, 10000),
	settings.WithPublic)

var (
	temporaryObjectCleanerActiveCleanersMetric = metric.InitMetadata(metric.Metadata{
		Name:        "sql.temp_object_cleaner.active_cleaners",
		Help:        "number of cleaner tasks currently running on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	})
	temporaryObjectCleanerSchemasToDeleteMetric = metric.InitMetadata(metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_to_delete",
		Help:        "number of schemas to be deleted by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	})
	temporaryObjectCleanerSchemasDeletionErrorMetric = metric.InitMetadata(metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_deletion_error",
		Help:        "number of errored schema deletions by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	})
	temporaryObjectCleanerSchemasDeletionSuccessMetric = metric.InitMetadata(metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_deletion_success",
		Help:        "number of successful schema deletions by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	})
)

func (p *planner) InsertTemporarySchema(
	tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
) {
	p.sessionDataMutatorIterator.ApplyOnEachMutator(func(m sessionmutator.SessionDataMutator) {
		m.SetTemporarySchemaName(tempSchemaName)
		m.SetTemporarySchemaIDForDatabase(uint32(databaseID), uint32(schemaID))
	})
}

func (p *planner) getOrCreateTemporarySchema(
	ctx context.Context, db catalog.DatabaseDescriptor,
) (catalog.SchemaDescriptor, error) {
	tempSchemaName := p.TemporarySchemaName()
	sc, err := p.Descriptors().ByName(p.txn).MaybeGet().Schema(ctx, db, tempSchemaName)
	if sc != nil || err != nil {
		return sc, err
	}

	// The temporary schema has not been created yet.
	id, err := p.EvalContext().DescIDGenerator.GenerateUniqueDescID(ctx)
	if err != nil {
		return nil, err
	}
	b := p.Txn().NewBatch()
	if err := p.Descriptors().InsertTempSchemaToBatch(
		ctx, p.ExtendedEvalContext().Tracing.KVTracingEnabled(), db, tempSchemaName, id, b,
	); err != nil {
		return nil, err
	}
	if err := p.txn.Run(ctx, b); err != nil {
		return nil, err
	}
	p.InsertTemporarySchema(tempSchemaName, db.GetID(), id)
	return p.byIDGetterBuilder().WithoutNonPublic().Get().Schema(ctx, id)
}

// temporarySchemaName returns the session specific temporary schema name given
// the sessionID. When the session creates a temporary object for the first
// time, it must create a schema with the name returned by this function.
func temporarySchemaName(sessionID clusterunique.ID) string {
	return fmt.Sprintf("pg_temp_%d_%d", sessionID.Hi, sessionID.Lo)
}

// temporarySchemaSessionID returns the sessionID of the given temporary schema.
func temporarySchemaSessionID(scName string) (bool, clusterunique.ID, error) {
	if !strings.HasPrefix(scName, "pg_temp_") {
		return false, clusterunique.ID{}, nil
	}
	parts := strings.Split(scName, "_")
	if len(parts) != 4 {
		return false, clusterunique.ID{}, errors.Errorf("malformed temp schema name %s", scName)
	}
	hi, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return false, clusterunique.ID{}, err
	}
	lo, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return false, clusterunique.ID{}, err
	}
	return true, clusterunique.ID{Uint128: uint128.Uint128{Hi: hi, Lo: lo}}, nil
}

// tempCleanupRetryOpts returns the retry options used for temp object cleanup
// transactions.
func tempCleanupRetryOpts() retry.Options {
	return retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     1 * time.Minute,
		Multiplier:     2,
		MaxRetries:     4, // 5 total attempts
	}
}

// retryTempCleanup wraps a function with the standard temp cleanup retry logic.
// It does not accept a Closer channel (unlike the background cleaner's retry),
// but it respects context cancellation via retry.StartWithCtx, so retries will
// be interrupted during server shutdown when the connection's context is
// cancelled.
func retryTempCleanup(ctx context.Context, do func() error) error {
	return tempCleanupRetryOpts().DoWithRetryable(ctx, func(ctx context.Context) (bool, error) {
		err := do()
		if err != nil {
			if shouldStopTempObjectCleanupRetry(err) {
				return false, err
			}
			log.Dev.Warningf(ctx, "error during temp schema cleanup, retrying: %v", err)
			return true, err
		}
		return false, nil
	})
}

// tempSchemaInfo holds the metadata collected in Phase 1 of temp schema cleanup
// for a single database's temporary schema.
type tempSchemaInfo struct {
	dbDesc     catalog.DatabaseDescriptor
	schemaName string
	// views, tables, and sequences are the IDs of objects to drop, categorized
	// by type. views are dropped first, then tables, then sequences.
	views     descpb.IDs
	tables    descpb.IDs
	sequences descpb.IDs
	// tblNamesByID maps object IDs to their fully-qualified names, used to
	// construct DROP statements.
	tblNamesByID map[descpb.ID]tree.TableName
	// tblDescsByID maps sequence IDs to their descriptors, used by
	// cleanupTempSequenceDeps to resolve dependencies on permanent tables.
	tblDescsByID map[descpb.ID]catalog.TableDescriptor
	// databaseIDToTempSchemaID maps database IDs to their temporary schema IDs,
	// used by the internal executor to resolve temporary schema names.
	databaseIDToTempSchemaID map[uint32]uint32
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
//
// The cleanup is split into three phases to avoid creating a single transaction
// that exceeds KV transaction limits for large numbers of objects:
//  1. Collect metadata: a read-only transaction discovers all objects to drop.
//  2. Drop objects in batches: each batch of objects is dropped in its own
//     transaction, with retry logic.
//  3. Delete schema entries: a final transaction removes the temporary schema
//     namespace entries.
//
// This means the overall operation is not atomic — if cleanup is interrupted
// partway through, some objects may be dropped while others remain. This is
// acceptable because:
//   - The objects belong to a dead session and are not referenced by any active
//     session.
//   - The background TemporaryObjectCleaner will re-discover any remaining
//     objects on its next run and continue cleaning them up.
//   - The schema namespace entry is only removed after all objects are
//     successfully dropped, so orphaned descriptors without a schema entry
//     cannot occur.
func cleanupSessionTempObjects(
	ctx context.Context, db descs.DB, st *cluster.Settings, sessionID clusterunique.ID,
) error {
	tempSchemaName := temporarySchemaName(sessionID)

	// Phase 1: Collect metadata about objects to drop. This is a read-only
	// transaction that enumerates all temp objects across all databases.
	var schemas []tempSchemaInfo
	if err := retryTempCleanup(ctx, func() error {
		return db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			schemas = nil // Reset on retry.
			descsCol := txn.Descriptors()
			allDbDescs, err := descsCol.GetAllDatabaseDescriptors(ctx, txn.KV())
			if err != nil {
				return err
			}
			for _, dbDesc := range allDbDescs {
				tempSchema, err := descsCol.ByName(txn.KV()).MaybeGet().Schema(ctx, dbDesc, tempSchemaName)
				if err != nil {
					return err
				}
				if tempSchema == nil {
					continue
				}
				info, err := collectTempSchemaObjects(ctx, txn, descsCol, dbDesc, tempSchema)
				if err != nil {
					return err
				}
				schemas = append(schemas, info)
			}
			return nil
		})
	}); err != nil {
		return err
	}

	if len(schemas) == 0 {
		return nil
	}

	// Phase 2: Drop objects in batches. Each batch runs in its own transaction
	// to keep the intent footprint small.
	batchSize := int(TempObjectCleanupBatchSize.Get(&st.SV))
	for i := range schemas {
		if err := dropTempSchemaObjectsInBatches(ctx, db, &schemas[i], batchSize); err != nil {
			return err
		}
	}

	// Phase 3: Delete the temporary schema namespace entries now that all
	// objects have been removed.
	return retryTempCleanup(ctx, func() error {
		return db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			descsCol := txn.Descriptors()
			b := txn.KV().NewBatch()
			const kvTrace = false
			for _, info := range schemas {
				if err := descsCol.DeleteTempSchemaToBatch(
					ctx, kvTrace, info.dbDesc, tempSchemaName, b,
				); err != nil {
					return err
				}
			}
			return txn.KV().Run(ctx, b)
		})
	})
}

// collectTempSchemaObjects enumerates all objects in a temp schema and returns
// the metadata needed to drop them in batches. It is used by both the batched
// cleanup path (cleanupSessionTempObjects) and the single-transaction DISCARD
// path (cleanupTempSchemaObjects).
func collectTempSchemaObjects(
	ctx context.Context,
	txn isql.Txn,
	descsCol *descs.Collection,
	dbDesc catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
) (tempSchemaInfo, error) {
	objects, err := descsCol.GetAllObjectsInSchema(ctx, txn.KV(), dbDesc, sc)
	if err != nil {
		return tempSchemaInfo{}, err
	}

	info := tempSchemaInfo{
		dbDesc:                   dbDesc,
		schemaName:               sc.GetName(),
		tblDescsByID:             make(map[descpb.ID]catalog.TableDescriptor),
		tblNamesByID:             make(map[descpb.ID]tree.TableName),
		databaseIDToTempSchemaID: make(map[uint32]uint32),
	}

	_ = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok || desc.Dropped() {
			return nil
		}
		info.tblDescsByID[desc.GetID()] = tbl
		info.tblNamesByID[desc.GetID()] = tree.MakeTableNameWithSchema(
			tree.Name(dbDesc.GetName()), tree.Name(sc.GetName()), tree.Name(tbl.GetName()),
		)
		info.databaseIDToTempSchemaID[uint32(desc.GetParentID())] = uint32(desc.GetParentSchemaID())

		if tbl.IsSequence() &&
			tbl.GetSequenceOpts().SequenceOwner.OwnerColumnID == 0 &&
			tbl.GetSequenceOpts().SequenceOwner.OwnerTableID == 0 {
			info.sequences = append(info.sequences, desc.GetID())
		} else if tbl.GetViewQuery() != "" {
			info.views = append(info.views, desc.GetID())
		} else if !tbl.IsSequence() {
			info.tables = append(info.tables, desc.GetID())
		}
		return nil
	})
	return info, nil
}

// dropTempSchemaObjectsInBatches drops all objects in a temp schema using
// batched transactions. Objects are dropped in dependency order: views first,
// then tables, then unowned sequences.
func dropTempSchemaObjectsInBatches(
	ctx context.Context, db descs.DB, info *tempSchemaInfo, batchSize int,
) error {
	searchPath := sessiondata.DefaultSearchPathForUser(username.NodeUserName()).WithTemporarySchemaName(info.schemaName)
	override := sessiondata.InternalExecutorOverride{
		SearchPath:               &searchPath,
		User:                     username.NodeUserName(),
		DatabaseIDToTempSchemaID: info.databaseIDToTempSchemaID,
	}

	// Objects are dropped in dependency order. hasSequenceDeps is true for
	// sequences that need cleanupTempSequenceDeps to remove default expression
	// references from permanent tables before the DROP.
	for _, toDelete := range []struct {
		typeName        string
		ids             descpb.IDs
		hasSequenceDeps bool
	}{
		{"VIEW", info.views, false},
		{"TABLE", info.tables, false},
		{"SEQUENCE", info.sequences, true},
	} {
		if len(toDelete.ids) == 0 {
			continue
		}
		for start := 0; start < len(toDelete.ids); start += batchSize {
			end := start + batchSize
			if end > len(toDelete.ids) {
				end = len(toDelete.ids)
			}
			batch := toDelete.ids[start:end]

			if err := retryTempCleanup(ctx, func() error {
				return db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
					// Remove default expression references from permanent
					// tables that depend on sequences in this batch.
					if toDelete.hasSequenceDeps {
						descsCol := txn.Descriptors()
						for _, id := range batch {
							if err := cleanupTempSequenceDeps(ctx, txn, descsCol, override, info, id); err != nil {
								return err
							}
						}
					}

					var query strings.Builder
					query.WriteString("DROP ")
					query.WriteString(toDelete.typeName)
					query.WriteString(" IF EXISTS")
					for i, id := range batch {
						tbName := info.tblNamesByID[id]
						if i != 0 {
							query.WriteString(",")
						}
						query.WriteString(" ")
						query.WriteString(tbName.FQString())
					}
					query.WriteString(" CASCADE")
					_, err := txn.ExecEx(ctx, redact.Sprintf("delete-temp-%s", toDelete.typeName), txn.KV(), override, query.String())
					return err
				})
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// cleanupTempSequenceDeps removes default expression dependencies from
// permanent tables that reference a temporary sequence about to be dropped.
// It is used by both the batched cleanup path and the single-transaction
// DISCARD path.
func cleanupTempSequenceDeps(
	ctx context.Context,
	txn isql.Txn,
	descsCol *descs.Collection,
	override sessiondata.InternalExecutorOverride,
	info *tempSchemaInfo,
	seqID descpb.ID,
) error {
	desc := info.tblDescsByID[seqID]
	return desc.ForeachDependedOnBy(func(d *descpb.TableDescriptor_Reference) error {
		// Skip objects that are also temp objects being cleaned up.
		if _, ok := info.tblDescsByID[d.ID]; ok {
			return nil
		}
		dTableDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, d.ID)
		if err != nil {
			return err
		}
		dDB, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dTableDesc.GetParentID())
		if err != nil {
			return err
		}
		dSc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, dTableDesc.GetParentSchemaID())
		if err != nil {
			return err
		}
		dependentColIDs := intsets.MakeFast()
		for _, colID := range d.ColumnIDs {
			dependentColIDs.Add(int(colID))
		}
		for _, col := range dTableDesc.PublicColumns() {
			if dependentColIDs.Contains(int(col.GetID())) {
				tbName := tree.MakeTableNameWithSchema(
					tree.Name(dDB.GetName()),
					tree.Name(dSc.GetName()),
					tree.Name(dTableDesc.GetName()),
				)
				_, err = txn.ExecEx(
					ctx,
					"delete-temp-dependent-col",
					txn.KV(),
					override,
					fmt.Sprintf(
						"ALTER TABLE %s ALTER COLUMN %s DROP DEFAULT",
						tbName.FQString(),
						tree.NameString(col.GetName()),
					),
				)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// cleanupTempSchemaObjects removes all objects in a single database's temporary
// schema within the caller-provided transaction (used by the DISCARD path).
// It shares collectTempSchemaObjects and cleanupTempSequenceDeps with the
// batched cleanup path to avoid duplicating the object enumeration and sequence
// dependency cleanup logic.
//
// TODO(postamar): properly use descsCol
// We're currently unable to leverage descsCol properly because we run DROP
// statements in the transaction which cause descsCol's cached state to become
// invalid. We should either drop all objects programmatically via descsCol's
// API or avoid it entirely.
func cleanupTempSchemaObjects(
	ctx context.Context,
	txn isql.Txn,
	descsCol *descs.Collection,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
) error {
	info, err := collectTempSchemaObjects(ctx, txn, descsCol, db, sc)
	if err != nil {
		return err
	}

	searchPath := sessiondata.DefaultSearchPathForUser(username.NodeUserName()).WithTemporarySchemaName(sc.GetName())
	override := sessiondata.InternalExecutorOverride{
		SearchPath:               &searchPath,
		User:                     username.NodeUserName(),
		DatabaseIDToTempSchemaID: info.databaseIDToTempSchemaID,
	}

	// Objects are dropped in dependency order. hasSequenceDeps is true for
	// sequences that need cleanupTempSequenceDeps to remove default expression
	// references from permanent tables before the DROP.
	for _, toDelete := range []struct {
		typeName        string
		ids             descpb.IDs
		hasSequenceDeps bool
	}{
		{"VIEW", info.views, false},
		{"TABLE", info.tables, false},
		{"SEQUENCE", info.sequences, true},
	} {
		if len(toDelete.ids) == 0 {
			continue
		}
		if toDelete.hasSequenceDeps {
			for _, id := range toDelete.ids {
				if err := cleanupTempSequenceDeps(ctx, txn, descsCol, override, &info, id); err != nil {
					return err
				}
			}
		}

		var query strings.Builder
		query.WriteString("DROP ")
		query.WriteString(toDelete.typeName)
		query.WriteString(" IF EXISTS")
		for i, id := range toDelete.ids {
			tbName := info.tblNamesByID[id]
			if i != 0 {
				query.WriteString(",")
			}
			query.WriteString(" ")
			query.WriteString(tbName.FQString())
		}
		query.WriteString(" CASCADE")
		_, err = txn.ExecEx(ctx, redact.Sprintf("delete-temp-%s", toDelete.typeName), txn.KV(), override, query.String())
		if err != nil {
			return err
		}
	}
	return nil
}

// isMeta1LeaseholderFunc helps us avoid an import into pkg/storage.
type isMeta1LeaseholderFunc func(context.Context, hlc.ClockTimestamp) (bool, error)

// TemporaryObjectCleaner is a background thread job that periodically
// cleans up orphaned temporary objects by sessions which did not close
// down cleanly.
type TemporaryObjectCleaner struct {
	settings *cluster.Settings
	db       descs.DB
	codec    keys.SQLCodec
	// statusServer gives access to the SQLStatus service.
	statusServer           serverpb.SQLStatusServer
	isMeta1LeaseholderFunc isMeta1LeaseholderFunc
	testingKnobs           ExecutorTestingKnobs
	metrics                *temporaryObjectCleanerMetrics

	// waitForInstances is a function to ensure that the status server will know
	// about the set of live instances at least as of the time of startup. This
	// primarily matters during tests of the temp table infrastructure in
	// secondary tenants.
	waitForInstances func(ctx context.Context) error
}

// temporaryObjectCleanerMetrics are the metrics for TemporaryObjectCleaner
type temporaryObjectCleanerMetrics struct {
	ActiveCleaners         *metric.Gauge
	SchemasToDelete        *metric.Counter
	SchemasDeletionError   *metric.Counter
	SchemasDeletionSuccess *metric.Counter
}

var _ metric.Struct = (*temporaryObjectCleanerMetrics)(nil)

// MetricStruct implements the metrics.Struct interface.
func (m *temporaryObjectCleanerMetrics) MetricStruct() {}

// NewTemporaryObjectCleaner initializes the TemporaryObjectCleaner with the
// required arguments, but does not start it.
func NewTemporaryObjectCleaner(
	settings *cluster.Settings,
	db descs.DB,
	codec keys.SQLCodec,
	registry *metric.Registry,
	statusServer serverpb.SQLStatusServer,
	isMeta1LeaseholderFunc isMeta1LeaseholderFunc,
	testingKnobs ExecutorTestingKnobs,
	waitForInstances func(ctx context.Context) error,
) *TemporaryObjectCleaner {
	metrics := makeTemporaryObjectCleanerMetrics()
	registry.AddMetricStruct(metrics)
	return &TemporaryObjectCleaner{
		settings:               settings,
		db:                     db,
		codec:                  codec,
		statusServer:           statusServer,
		isMeta1LeaseholderFunc: isMeta1LeaseholderFunc,
		testingKnobs:           testingKnobs,
		metrics:                metrics,
		waitForInstances:       waitForInstances,
	}
}

// makeTemporaryObjectCleanerMetrics makes the metrics for the TemporaryObjectCleaner.
func makeTemporaryObjectCleanerMetrics() *temporaryObjectCleanerMetrics {
	return &temporaryObjectCleanerMetrics{
		ActiveCleaners:         metric.NewGauge(temporaryObjectCleanerActiveCleanersMetric),
		SchemasToDelete:        metric.NewCounter(temporaryObjectCleanerSchemasToDeleteMetric),
		SchemasDeletionError:   metric.NewCounter(temporaryObjectCleanerSchemasDeletionErrorMetric),
		SchemasDeletionSuccess: metric.NewCounter(temporaryObjectCleanerSchemasDeletionSuccessMetric),
	}
}

// shouldStopTempObjectCleanupRetry returns true if the error indicates that we
// should not retry the operation.
func shouldStopTempObjectCleanupRetry(err error) bool {
	if err == nil {
		return false
	}

	// Check if transaction is already poisoned. Once this happens, the
	// transaction cannot be reused regardless of retry attempts.
	return errors.HasType(err, (*kvpb.TxnAlreadyEncounteredErrorError)(nil))
}

// doTemporaryObjectCleanup performs the actual cleanup.
func (c *TemporaryObjectCleaner) doTemporaryObjectCleanup(
	ctx context.Context, closerCh <-chan struct{},
) error {
	defer log.Dev.Infof(ctx, "completed temporary object cleanup job")
	// Wrap the retry functionality with the default arguments.
	retryFunc := func(ctx context.Context, do func() error) error {
		return retry.Options{
			InitialBackoff: 1 * time.Second,
			MaxBackoff:     1 * time.Minute,
			Multiplier:     2,
			MaxRetries:     4, // 5 total attempts (4 retries + 1 initial)
			Closer:         closerCh,
		}.DoWithRetryable(ctx, func(ctx context.Context) (bool, error) {
			err := do()
			if err != nil {
				if shouldStopTempObjectCleanupRetry(err) {
					log.Dev.Warningf(ctx, "error during schema cleanup, not retryable: %v", err)
					return false, err // Don't retry
				}
				log.Dev.Warningf(ctx, "error during schema cleanup, retrying: %v", err)
				return true, err // Retry
			}
			return false, nil // Success
		})
	}

	// For tenants, we will completely skip this logic since listing
	// sessions will fan out to all pods in the tenant case. So, there
	// is no harm in executing this logic without any type of coordination.
	if c.codec.ForSystemTenant() {
		// We only want to perform the cleanup if we are holding the meta1 lease.
		// This ensures only one server can perform the job at a time.
		isLeaseHolder, err := c.isMeta1LeaseholderFunc(ctx, c.db.KV().Clock().NowAsClockTimestamp())
		if err != nil {
			return err
		}
		// For the system tenant we will check if the lease is held. For tenants
		// every single POD will try to execute this clean up logic.
		if !isLeaseHolder {
			log.Dev.Infof(ctx, "skipping temporary object cleanup run as it is not the leaseholder")
			return nil
		}
	}
	// We need to make sure that we have a somewhat up-to-date view of the
	// set of live instances. If not, we might delete a relatively new
	// table. In general this shouldn't be necessary because our wait interval
	// is extremely conservative at 30 minutes by default, but under tests,
	// it comes up.
	if c.waitForInstances != nil {
		if err := c.waitForInstances(ctx); err != nil {
			return err
		}
	}

	c.metrics.ActiveCleaners.Inc(1)
	defer c.metrics.ActiveCleaners.Dec(1)

	log.Dev.Infof(ctx, "running temporary object cleanup background job")
	var sessionIDs map[clusterunique.ID]struct{}
	if err := c.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		// Testing knob to inject errors during cleanup.
		if c.testingKnobs.TempObjectCleanupErrorInjection != nil {
			if err := retryFunc(ctx, func() (err error) {
				return c.testingKnobs.TempObjectCleanupErrorInjection()
			}); err != nil {
				return err
			}
		}

		sessionIDs = make(map[clusterunique.ID]struct{})
		// Only see temporary schemas after some delay as safety
		// mechanism.
		waitTimeForCreation := TempObjectWaitInterval.Get(&c.settings.SV)
		descsCol := txn.Descriptors()
		// Build a set of all databases with temporary objects.
		var dbs nstree.Catalog
		if err := retryFunc(ctx, func() (err error) {
			dbs, err = descsCol.GetAllDatabases(ctx, txn.KV())
			return err
		}); err != nil {
			return err
		}
		return dbs.ForEachDescriptor(func(dbDesc catalog.Descriptor) error {
			db, err := catalog.AsDatabaseDescriptor(dbDesc)
			if err != nil {
				return err
			}
			var schemas nstree.Catalog
			if err := retryFunc(ctx, func() (err error) {
				schemas, err = descsCol.GetAllSchemasInDatabase(ctx, txn.KV(), db)
				return err
			}); err != nil {
				return err
			}
			return schemas.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
				if e.GetParentSchemaID() != descpb.InvalidID {
					return nil
				}
				// Skip over any temporary objects that are not old enough,
				// we intentionally use a delay to avoid problems.
				if !e.GetMVCCTimestamp().Less(txn.KV().ReadTimestamp().Add(-waitTimeForCreation.Nanoseconds(), 0)) {
					return nil
				}
				if isTempSchema, sessionID, err := temporarySchemaSessionID(e.GetName()); err != nil {
					// This should not cause an error.
					log.Dev.Warningf(ctx, "could not parse %q as temporary schema name", e.GetName())
				} else if isTempSchema {
					sessionIDs[sessionID] = struct{}{}
				}
				return nil
			})
		})
	}); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "found %d temporary schemas", len(sessionIDs))

	if len(sessionIDs) == 0 {
		log.Dev.Infof(ctx, "early exiting temporary schema cleaner as no temporary schemas were found")
		return nil
	}

	// Get active sessions.
	var response *serverpb.ListSessionsResponse
	if err := retryFunc(ctx, func() error {
		var err error
		response, err = c.statusServer.ListSessions(
			ctx,
			&serverpb.ListSessionsRequest{
				ExcludeClosedSessions: true,
			},
		)
		if response != nil && len(response.Errors) > 0 &&
			err == nil {
			return errors.Newf("fan out rpc failed with %s on node %d", response.Errors[0].Message, response.Errors[0].NodeID)
		}
		return err
	}); err != nil {
		return err
	}
	activeSessions := make(map[uint128.Uint128]struct{})
	for _, session := range response.Sessions {
		activeSessions[uint128.FromBytes(session.ID)] = struct{}{}
	}

	// Clean up temporary data for inactive sessions.
	for sessionID := range sessionIDs {
		if _, ok := activeSessions[sessionID.Uint128]; !ok {
			log.Eventf(ctx, "cleaning up temporary object for session %q", sessionID)
			c.metrics.SchemasToDelete.Inc(1)

			// Reset the session data with the appropriate sessionID such that we can resolve
			// the given schema correctly.
			//
			// Note: cleanupSessionTempObjects has its own internal retries
			// (retryTempCleanup) around each phase. This outer retryFunc provides
			// an additional layer: if all inner retries for a phase are exhausted,
			// the outer retry restarts from Phase 1, re-collecting the (now
			// potentially fewer) remaining objects. Worst case is 5 × 5 = 25
			// attempts per phase.
			if err := retryFunc(ctx, func() error {
				return cleanupSessionTempObjects(
					ctx,
					c.db,
					c.settings,
					sessionID,
				)
			}); err != nil {
				// Log error but continue trying to delete the rest.
				log.Dev.Warningf(ctx, "failed to clean temp objects under session %q: %v", sessionID, err)
				c.metrics.SchemasDeletionError.Inc(1)
			} else {
				c.metrics.SchemasDeletionSuccess.Inc(1)
				telemetry.Inc(sqltelemetry.TempObjectCleanerDeletionCounter)
			}
		} else {
			log.Eventf(ctx, "not cleaning up %q as session is still active", sessionID)
		}
	}

	return nil
}

// Start initializes the background thread which periodically cleans up leftover temporary objects.
func (c *TemporaryObjectCleaner) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "object-cleaner", func(ctx context.Context) {
		nextTick := timeutil.Now()
		for {
			nextTickCh := time.After(nextTick.Sub(timeutil.Now()))
			if c.testingKnobs.TempObjectsCleanupCh != nil {
				nextTickCh = c.testingKnobs.TempObjectsCleanupCh
			}

			select {
			case <-nextTickCh:
				if err := c.doTemporaryObjectCleanup(ctx, stopper.ShouldQuiesce()); err != nil {
					log.Dev.Warningf(ctx, "failed to clean temp objects: %v", err)
				}
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			}
			if c.testingKnobs.OnTempObjectsCleanupDone != nil {
				c.testingKnobs.OnTempObjectsCleanupDone()
			}
			nextTick = nextTick.Add(TempObjectCleanupInterval.Get(&c.settings.SV))
			log.Dev.Infof(ctx, "temporary object cleaner next scheduled to run at %s", nextTick)
		}
	})
}
