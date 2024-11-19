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

func (p *planner) InsertTemporarySchema(
	tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
) {
	p.sessionDataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
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

// cleanupSessionTempObjects removes all temporary objects (tables, sequences,
// views, temporary schema) created by the session.
func cleanupSessionTempObjects(
	ctx context.Context, db descs.DB, codec keys.SQLCodec, sessionID clusterunique.ID,
) error {
	tempSchemaName := temporarySchemaName(sessionID)
	return db.DescsTxn(
		ctx, func(
			ctx context.Context, txn descs.Txn,
		) error {
			// We are going to read all database descriptor IDs, then for each database
			// we will drop all the objects under the temporary schema.
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
				if err := cleanupTempSchemaObjects(
					ctx,
					txn,
					descsCol,
					codec,
					dbDesc,
					tempSchema,
				); err != nil {
					return err
				}
				// Even if no objects were found under the temporary schema, the schema
				// itself may still exist (eg. a temporary table was created and then
				// dropped). So we remove the namespace table entry of the temporary
				// schema.
				b := txn.KV().NewBatch()
				const kvTrace = false
				if err := descsCol.DeleteTempSchemaToBatch(
					ctx, kvTrace, dbDesc, tempSchemaName, b,
				); err != nil {
					return err
				}
				if err := txn.KV().Run(ctx, b); err != nil {
					return err
				}
			}
			return nil
		})
}

// cleanupTempSchemaObjects removes all objects that is located within a dbID and schema.
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
	codec keys.SQLCodec,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
) error {
	objects, err := descsCol.GetAllObjectsInSchema(ctx, txn.KV(), db, sc)
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

	tblDescsByID := make(map[descpb.ID]catalog.TableDescriptor)
	tblNamesByID := make(map[descpb.ID]tree.TableName)
	_ = objects.ForEachDescriptor(func(desc catalog.Descriptor) error {
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok || desc.Dropped() {
			return nil
		}
		tblDescsByID[desc.GetID()] = tbl
		tblNamesByID[desc.GetID()] = tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(tbl.GetName()),
		)

		databaseIDToTempSchemaID[uint32(desc.GetParentID())] = uint32(desc.GetParentSchemaID())

		// If a sequence is owned by a table column, it is dropped when the owner
		// table/column is dropped. So here we want to only drop sequences not
		// owned.
		if tbl.IsSequence() &&
			tbl.GetSequenceOpts().SequenceOwner.OwnerColumnID == 0 &&
			tbl.GetSequenceOpts().SequenceOwner.OwnerTableID == 0 {
			sequences = append(sequences, desc.GetID())
		} else if tbl.GetViewQuery() != "" {
			views = append(views, desc.GetID())
		} else if !tbl.IsSequence() {
			tables = append(tables, desc.GetID())
		}
		return nil
	})

	searchPath := sessiondata.DefaultSearchPathForUser(username.NodeUserName()).WithTemporarySchemaName(sc.GetName())
	override := sessiondata.InternalExecutorOverride{
		SearchPath:               &searchPath,
		User:                     username.NodeUserName(),
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
					dTableDesc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, d.ID)
					if err != nil {
						return err
					}
					db, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dTableDesc.GetParentID())
					if err != nil {
						return err
					}
					sc, err := descsCol.ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Schema(ctx, dTableDesc.GetParentSchemaID())
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
								tree.Name(db.GetName()),
								tree.Name(sc.GetName()),
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
			_, err = txn.ExecEx(ctx, redact.Sprintf("delete-temp-%s", toDelete.typeName), txn.KV(), override, query.String())
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
			log.Infof(ctx, "skipping temporary object cleanup run as it is not the leaseholder")
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

	log.Infof(ctx, "running temporary object cleanup background job")
	var sessionIDs map[clusterunique.ID]struct{}
	if err := c.db.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
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
					log.Warningf(ctx, "could not parse %q as temporary schema name", e.GetName())
				} else if isTempSchema {
					sessionIDs[sessionID] = struct{}{}
				}
				return nil
			})
		})
	}); err != nil {
		return err
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
			if err := retryFunc(ctx, func() error {
				return cleanupSessionTempObjects(
					ctx,
					c.db,
					c.codec,
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
