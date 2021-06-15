// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
	io_prometheus_client "github.com/prometheus/client_model/go"
)

// TempObjectCleanupInterval is a ClusterSetting controlling how often
// temporary objects get cleaned up.
var TempObjectCleanupInterval = settings.RegisterDurationSetting(
	"sql.temp_object_cleaner.cleanup_interval",
	"how often to clean up orphaned temporary objects",
	30*time.Minute,
).WithPublic()

var (
	temporaryObjectCleanerActiveCleanersMetric = metric.Metadata{
		Name:        "sql.temp_object_cleaner.active_cleaners",
		Help:        "number of cleaner tasks currently running on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_GAUGE,
	}
	temporaryObjectCleanerSchemasToDeleteMetric = metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_to_delete",
		Help:        "number of schemas to be deleted by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	temporaryObjectCleanerSchemasDeletionErrorMetric = metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_deletion_error",
		Help:        "number of errored schema deletions by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
	temporaryObjectCleanerSchemasDeletionSuccessMetric = metric.Metadata{
		Name:        "sql.temp_object_cleaner.schemas_deletion_success",
		Help:        "number of successful schema deletions by the temp object cleaner on this node",
		Measurement: "Count",
		Unit:        metric.Unit_COUNT,
		MetricType:  io_prometheus_client.MetricType_COUNTER,
	}
)

// TemporarySchemaNameForRestorePrefix is the prefix name of the schema we
// synthesize during a full cluster restore. All temporary objects being
// restored are remapped to belong to this schema allowing the reconciliation
// job to gracefully clean up these objects when it runs.
const TemporarySchemaNameForRestorePrefix string = "pg_temp_0_"

func (p *planner) getOrCreateTemporarySchema(
	ctx context.Context, db catalog.DatabaseDescriptor,
) (catalog.SchemaDescriptor, error) {
	tempSchemaName := p.TemporarySchemaName()
	sc, err := p.Descriptors().GetMutableSchemaByName(ctx, p.txn, db, tempSchemaName, p.CommonLookupFlags(false))
	if sc != nil || err != nil {
		return sc, err
	}
	sKey := catalogkeys.NewNameKeyComponents(db.GetID(), keys.RootNamespaceID, tempSchemaName)

	// The temporary schema has not been created yet.
	id, err := catalogkv.GenerateUniqueDescID(ctx, p.ExecCfg().DB, p.ExecCfg().Codec)
	if err != nil {
		return nil, err
	}
	if err := p.CreateSchemaNamespaceEntry(ctx, catalogkeys.EncodeNameKey(p.ExecCfg().Codec, sKey), id); err != nil {
		return nil, err
	}
	p.sessionDataMutator.SetTemporarySchemaName(sKey.GetName())
	p.sessionDataMutator.SetTemporarySchemaIDForDatabase(uint32(db.GetID()), uint32(id))
	return p.Descriptors().GetImmutableSchemaByID(ctx, p.Txn(), id, p.CommonLookupFlags(true))
}

// CreateSchemaNamespaceEntry creates an entry for the schema in the
// system.namespace table.
func (p *planner) CreateSchemaNamespaceEntry(
	ctx context.Context, schemaNameKey roachpb.Key, schemaID descpb.ID,
) error {
	if p.ExtendedEvalContext().Tracing.KVTracingEnabled() {
		log.VEventf(ctx, 2, "CPut %s -> %d", schemaNameKey, schemaID)
	}

	b := &kv.Batch{}
	b.CPut(schemaNameKey, schemaID, nil)

	return p.txn.Run(ctx, b)
}

// temporarySchemaName returns the session specific temporary schema name given
// the sessionID. When the session creates a temporary object for the first
// time, it must create a schema with the name returned by this function.
func temporarySchemaName(sessionID ClusterWideID) string {
	return fmt.Sprintf("pg_temp_%d_%d", sessionID.Hi, sessionID.Lo)
}

// temporarySchemaSessionID returns the sessionID of the given temporary schema.
func temporarySchemaSessionID(scName string) (bool, ClusterWideID, error) {
	if !strings.HasPrefix(scName, "pg_temp_") {
		return false, ClusterWideID{}, nil
	}
	parts := strings.Split(scName, "_")
	if len(parts) != 4 {
		return false, ClusterWideID{}, errors.Errorf("malformed temp schema name %s", scName)
	}
	hi, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return false, ClusterWideID{}, err
	}
	lo, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		return false, ClusterWideID{}, err
	}
	return true, ClusterWideID{uint128.Uint128{Hi: hi, Lo: lo}}, nil
}

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(
	ctx context.Context,
	settings *cluster.Settings,
	leaseMgr *lease.Manager,
	db *kv.DB,
	codec keys.SQLCodec,
	ie sqlutil.InternalExecutor,
	sessionID ClusterWideID,
) error {
	tempSchemaName := temporarySchemaName(sessionID)
	return descs.Txn(ctx, settings, leaseMgr, ie, db, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		// We are going to read all database descriptor IDs, then for each database
		// we will drop all the objects under the temporary schema.
		dbIDs, err := catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, codec)
		if err != nil {
			return err
		}
		for _, id := range dbIDs {
			if err := cleanupSchemaObjects(
				ctx,
				settings,
				txn,
				descsCol,
				codec,
				ie,
				id,
				tempSchemaName,
			); err != nil {
				return err
			}
			// Even if no objects were found under the temporary schema, the schema
			// itself may still exist (eg. a temporary table was created and then
			// dropped). So we remove the namespace table entry of the temporary
			// schema.
			key := catalogkeys.MakeSchemaNameKey(codec, id, tempSchemaName)
			if err := txn.Del(ctx, key); err != nil {
				return err
			}
		}
		return nil
	})
}

// cleanupSchemaObjects removes all objects that is located within a dbID and schema.
func cleanupSchemaObjects(
	ctx context.Context,
	settings *cluster.Settings,
	txn *kv.Txn,
	descsCol *descs.Collection,
	codec keys.SQLCodec,
	ie sqlutil.InternalExecutor,
	dbID descpb.ID,
	schemaName string,
) error {
	dbDesc, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, codec, dbID)
	if err != nil {
		return err
	}
	tbNames, _, err := descsCol.GetObjectNamesAndIDs(
		ctx,
		txn,
		dbDesc,
		schemaName,
		tree.DatabaseListFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: false}},
	)
	if err != nil {
		return err
	}

	// We construct the database ID -> temp Schema ID map here so that the
	// drop statements executed by the internal executor can resolve the temporary
	// schemaID later.
	databaseIDToTempSchemaID := make(map[uint32]uint32)

	// TODO(andrei): We might want to accelerate the deletion of this data.
	var tables descpb.IDs
	var views descpb.IDs
	var sequences descpb.IDs

	tblDescsByID := make(map[descpb.ID]catalog.TableDescriptor, len(tbNames))
	tblNamesByID := make(map[descpb.ID]tree.TableName, len(tbNames))
	for _, tbName := range tbNames {
		flags := tree.ObjectLookupFlagsWithRequired()
		flags.AvoidCached = true
		_, desc, err := descsCol.GetImmutableTableByName(ctx, txn, &tbName, flags)
		if err != nil {
			return err
		}

		tblDescsByID[desc.GetID()] = desc
		tblNamesByID[desc.GetID()] = tbName

		databaseIDToTempSchemaID[uint32(desc.GetParentID())] = uint32(desc.GetParentSchemaID())

		if desc.GetSequenceOpts() != nil {
			sequences = append(sequences, desc.GetID())
		} else if desc.GetViewQuery() != "" {
			views = append(views, desc.GetID())
		} else {
			tables = append(tables, desc.GetID())
		}
	}

	searchPath := sessiondata.DefaultSearchPathForUser(security.RootUserName()).WithTemporarySchemaName(schemaName)
	override := sessiondata.InternalExecutorOverride{
		SearchPath:               &searchPath,
		User:                     security.RootUserName(),
		DatabaseIDToTempSchemaID: databaseIDToTempSchemaID,
	}

	for _, toDelete := range []struct {
		// typeName is the type of table being deleted, e.g. view, table, sequence
		typeName string
		// ids represents which ids we wish to remove.
		ids descpb.IDs
		// preHook is used to perform any operations needed before calling
		// delete on all the given ids.
		preHook func(descpb.ID) error
	}{
		// Drop views before tables to avoid deleting required dependencies.
		{"VIEW", views, nil},
		{"TABLE", tables, nil},
		// Drop sequences after tables, because then we reduce the amount of work
		// that may be needed to drop indices.
		{
			"SEQUENCE",
			sequences,
			func(id descpb.ID) error {
				desc := tblDescsByID[id]
				// For any dependent tables, we need to drop the sequence dependencies.
				// This can happen if a permanent table references a temporary table.
				return desc.ForeachDependedOnBy(func(d *descpb.TableDescriptor_Reference) error {
					// We have already cleaned out anything we are depended on if we've seen
					// the descriptor already.
					if _, ok := tblDescsByID[d.ID]; ok {
						return nil
					}
					// TODO (lucy): Use the descriptor collection to get descriptors here.
					dTableDesc, err := catalogkv.MustGetTableDescByID(ctx, txn, codec, d.ID)
					if err != nil {
						return err
					}
					db, err := catalogkv.MustGetDatabaseDescByID(ctx, txn, codec, dTableDesc.GetParentID())
					if err != nil {
						return err
					}
					schema, err := resolver.ResolveSchemaNameByID(
						ctx,
						txn,
						codec,
						dTableDesc.GetParentID(),
						dTableDesc.GetParentSchemaID(),
					)
					if err != nil {
						return err
					}
					dependentColIDs := util.MakeFastIntSet()
					for _, colID := range d.ColumnIDs {
						dependentColIDs.Add(int(colID))
					}
					for _, col := range dTableDesc.PublicColumns() {
						if dependentColIDs.Contains(int(col.GetID())) {
							tbName := tree.MakeTableNameWithSchema(
								tree.Name(db.GetName()),
								tree.Name(schema),
								tree.Name(dTableDesc.GetName()),
							)
							_, err = ie.ExecEx(
								ctx,
								"delete-temp-dependent-col",
								txn,
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
			},
		},
	} {
		if len(toDelete.ids) > 0 {
			if toDelete.preHook != nil {
				for _, id := range toDelete.ids {
					if err := toDelete.preHook(id); err != nil {
						return err
					}
				}
			}

			var query strings.Builder
			query.WriteString("DROP ")
			query.WriteString(toDelete.typeName)

			for i, id := range toDelete.ids {
				tbName := tblNamesByID[id]
				if i != 0 {
					query.WriteString(",")
				}
				query.WriteString(" ")
				query.WriteString(tbName.FQString())
			}
			query.WriteString(" CASCADE")
			_, err = ie.ExecEx(ctx, "delete-temp-"+toDelete.typeName, txn, override, query.String())
			if err != nil {
				return err
			}
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
	settings                         *cluster.Settings
	db                               *kv.DB
	codec                            keys.SQLCodec
	makeSessionBoundInternalExecutor sqlutil.SessionBoundInternalExecutorFactory
	// statusServer gives access to the SQLStatus service.
	statusServer           serverpb.SQLStatusServer
	isMeta1LeaseholderFunc isMeta1LeaseholderFunc
	testingKnobs           ExecutorTestingKnobs
	metrics                *temporaryObjectCleanerMetrics
	leaseMgr               *lease.Manager
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
	db *kv.DB,
	codec keys.SQLCodec,
	registry *metric.Registry,
	makeSessionBoundInternalExecutor sqlutil.SessionBoundInternalExecutorFactory,
	statusServer serverpb.SQLStatusServer,
	isMeta1LeaseholderFunc isMeta1LeaseholderFunc,
	testingKnobs ExecutorTestingKnobs,
	leaseMgr *lease.Manager,
) *TemporaryObjectCleaner {
	metrics := makeTemporaryObjectCleanerMetrics()
	registry.AddMetricStruct(metrics)
	return &TemporaryObjectCleaner{
		settings:                         settings,
		db:                               db,
		codec:                            codec,
		makeSessionBoundInternalExecutor: makeSessionBoundInternalExecutor,
		statusServer:                     statusServer,
		isMeta1LeaseholderFunc:           isMeta1LeaseholderFunc,
		testingKnobs:                     testingKnobs,
		metrics:                          metrics,
		leaseMgr:                         leaseMgr,
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

// doTemporaryObjectCleanup performs the actual cleanup.
func (c *TemporaryObjectCleaner) doTemporaryObjectCleanup(
	ctx context.Context, closerCh <-chan struct{},
) error {
	defer log.Infof(ctx, "completed temporary object cleanup job")
	// Wrap the retry functionality with the default arguments.
	retryFunc := func(ctx context.Context, do func() error) error {
		return retry.WithMaxAttempts(
			ctx,
			retry.Options{
				InitialBackoff: 1 * time.Second,
				MaxBackoff:     1 * time.Minute,
				Multiplier:     2,
				Closer:         closerCh,
			},
			5, // maxAttempts
			func() error {
				err := do()
				if err != nil {
					log.Warningf(ctx, "error during schema cleanup, retrying: %v", err)
				}
				return err
			},
		)
	}

	// We only want to perform the cleanup if we are holding the meta1 lease.
	// This ensures only one server can perform the job at a time.
	isLeaseholder, err := c.isMeta1LeaseholderFunc(ctx, c.db.Clock().NowAsClockTimestamp())
	if err != nil {
		return err
	}
	if !isLeaseholder {
		log.Infof(ctx, "skipping temporary object cleanup run as it is not the leaseholder")
		return nil
	}

	c.metrics.ActiveCleaners.Inc(1)
	defer c.metrics.ActiveCleaners.Dec(1)

	log.Infof(ctx, "running temporary object cleanup background job")
	// TODO(sumeer): this is not using NewTxnWithSteppingEnabled and so won't be
	// classified as FROM_SQL for purposes of admission control. Fix.
	txn := kv.NewTxn(ctx, c.db, 0)

	// Build a set of all session IDs with temporary objects.
	var dbIDs []descpb.ID
	if err := retryFunc(ctx, func() error {
		var err error
		dbIDs, err = catalogkv.GetAllDatabaseDescriptorIDs(ctx, txn, c.codec)
		return err
	}); err != nil {
		return err
	}

	sessionIDs := make(map[ClusterWideID]struct{})
	for _, dbID := range dbIDs {
		var schemaNames map[descpb.ID]string
		if err := retryFunc(ctx, func() error {
			var err error
			schemaNames, err = resolver.GetForDatabase(ctx, txn, c.codec, dbID)
			return err
		}); err != nil {
			return err
		}
		for _, scName := range schemaNames {
			isTempSchema, sessionID, err := temporarySchemaSessionID(scName)
			if err != nil {
				// This should not cause an error.
				log.Warningf(ctx, "could not parse %q as temporary schema name", scName)
				continue
			}
			if isTempSchema {
				sessionIDs[sessionID] = struct{}{}
			}
		}
	}
	log.Infof(ctx, "found %d temporary schemas", len(sessionIDs))

	if len(sessionIDs) == 0 {
		log.Infof(ctx, "early exiting temporary schema cleaner as no temporary schemas were found")
		return nil
	}

	// Get active sessions.
	var response *serverpb.ListSessionsResponse
	if err := retryFunc(ctx, func() error {
		var err error
		response, err = c.statusServer.ListSessions(
			ctx,
			&serverpb.ListSessionsRequest{},
		)
		return err
	}); err != nil {
		return err
	}
	activeSessions := make(map[uint128.Uint128]struct{})
	for _, session := range response.Sessions {
		activeSessions[uint128.FromBytes(session.ID)] = struct{}{}
	}

	// Clean up temporary data for inactive sessions.
	ie := c.makeSessionBoundInternalExecutor(ctx, &sessiondata.SessionData{})
	for sessionID := range sessionIDs {
		if _, ok := activeSessions[sessionID.Uint128]; !ok {
			log.Eventf(ctx, "cleaning up temporary object for session %q", sessionID)
			c.metrics.SchemasToDelete.Inc(1)

			// Reset the session data with the appropriate sessionID such that we can resolve
			// the given schema correctly.
			if err := retryFunc(ctx, func() error {
				return cleanupSessionTempObjects(
					ctx,
					c.settings,
					c.leaseMgr,
					c.db,
					c.codec,
					ie,
					sessionID,
				)
			}); err != nil {
				// Log error but continue trying to delete the rest.
				log.Warningf(ctx, "failed to clean temp objects under session %q: %v", sessionID, err)
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
					log.Warningf(ctx, "failed to clean temp objects: %v", err)
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
			log.Infof(ctx, "temporary object cleaner next scheduled to run at %s", nextTick)
		}
	})
}
