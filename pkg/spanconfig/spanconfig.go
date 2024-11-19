// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spanconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// KVAccessor mediates access to KV span configurations pertaining to a given
// tenant.
type KVAccessor interface {
	// GetSpanConfigRecords returns the span configurations that apply to or
	// overlap with the supplied targets.
	GetSpanConfigRecords(ctx context.Context, targets []Target) ([]Record, error)

	// GetAllSystemSpanConfigsThatApply returns all system span configurations
	// that apply over ranges of the supplied tenant ID. This includes those
	// set by the tenant itself and those set by the system tenant.
	GetAllSystemSpanConfigsThatApply(
		ctx context.Context, id roachpb.TenantID,
	) ([]roachpb.SpanConfig, error)

	// UpdateSpanConfigRecords updates configurations for the given targets. This
	// is a "targeted" API: the targets being deleted are expected to have been
	// present.
	//
	// Targets are not allowed to overlap with each other. When divvying up an
	// existing target into multiple others with distinct configs, callers must
	// issue deletes for the previous target and upserts for the new records.
	// The updates are performed atomically and at a timestamp within
	// [minCommitTS, maxCommitTS). Typically, this is the lease interval of the
	// reconciliation job on behalf of which the KVAccessor is acting. If we're
	// unable to commit within the interval, a commitTimestampOutOfBoundsError is
	// returned.
	UpdateSpanConfigRecords(
		ctx context.Context,
		toDelete []Target,
		toUpsert []Record,
		minCommitTS, maxCommitTS hlc.Timestamp,
	) error

	// WithTxn returns a KVAccessor that runs using the given transaction (with
	// its operations discarded if aborted, valid only if committed). If nil, a
	// transaction is created internally for every operation.
	WithTxn(context.Context, *kv.Txn) KVAccessor

	// WithISQLTxn returns a KVAccessor that runs using the given isql.Txn (with
	// its operations discarded if aborted, valid only if committed). This makes
	// it possible to use the KVAccessor in the context of a SQL transaction.
	WithISQLTxn(context.Context, isql.Txn) KVAccessor
}

// KVSubscriber presents a consistent[1] snapshot of a StoreReader and
// ProtectedTSReader that's incrementally maintained with changes made to the
// global span configurations state (system.span_configurations). The
// maintenance happens transparently.
//
// Callers can subscribe to learn about what key spans may have seen a
// configuration change. After learning about a span update through a callback
// invocation, subscribers can consult the embedded StoreReader to retrieve an
// up-to-date[2] config for the updated span. The callback is called in a single
// goroutine; it should avoid doing any long-running or blocking work.
// KVSubscriber also exposes a timestamp that indicates how up-to-date it is
// with the global state.
//
// When a callback is first installed, it's invoked with the [min,max) span --
// a shorthand to indicate that subscribers should consult the StoreReader for all
// spans of interest. Subsequent updates are of the more incremental kind. It's
// possible that the span updates received are no-ops, i.e. consulting the
// StoreReader for the given span would still retrieve the last config observed
// for the span[3].
//
// [1]: The contents of the StoreReader and ProtectedTSReader at t1 corresponds
//
//	exactly to the contents of the global span configuration state at t0
//	where t0 <= t1. If the StoreReader or ProtectedTSReader is read from at
//	t2 where t2 > t1, it's guaranteed to observe a view of the global state
//	at t >= t0.
//
// [2]: For the canonical KVSubscriber implementation, this is typically lagging
//
//	by the closed timestamp target duration.
//
// [3]: The canonical KVSubscriber implementation is bounced whenever errors
//
//	occur, which may result in the re-transmission of earlier updates
//	(typically through a coarsely targeted [min,max) span).
type KVSubscriber interface {
	StoreReader
	ProtectedTSReader

	LastUpdated() hlc.Timestamp
	Subscribe(func(ctx context.Context, updated roachpb.Span))
}

// SQLTranslator translates SQL descriptors and their corresponding zone
// configurations to constituent spans and span configurations.
//
// Concretely, for the following zone configuration hierarchy:
//
//	CREATE DATABASE db;
//	CREATE TABLE db.t1();
//	ALTER DATABASE db CONFIGURE ZONE USING num_replicas=7;
//	ALTER TABLE db.t1 CONFIGURE ZONE USING num_voters=5;
//
// The SQLTranslator produces the following translation (represented as a diff
// against RANGE DEFAULT for brevity):
//
//	Table/5{3-4}                  num_replicas=7 num_voters=5
type SQLTranslator interface {
	// Translate generates the span configuration state given a list of
	// {descriptor, named zone} IDs. Entries are unique, and are omitted for IDs
	// that don't exist.
	// Additionally, if `generateSystemSpanConfigurations` is set to true,
	// Translate will generate all the span configurations that apply to
	// `spanconfig.SystemTargets`.
	//
	// For every ID we first descend the zone configuration hierarchy with the
	// ID as the root to accumulate IDs of all leaf objects. Leaf objects are
	// tables and named zones (other than RANGE DEFAULT) which have actual span
	// configurations associated with them (as opposed to non-leaf nodes that
	// only serve to hold zone configurations for inheritance purposes). Then,
	// for each one of these accumulated IDs, we generate <span, config> tuples
	// by following up the inheritance chain to fully hydrate the span
	// configuration. Translate also accounts for and negotiates subzone spans.
	Translate(
		ctx context.Context,
		ids descpb.IDs,
		generateSystemSpanConfigurations bool,
	) ([]Record, error)
}

// FullTranslate translates the entire SQL zone configuration state to the span
// configuration state. The timestamp at which such a translation is valid is
// also returned.
func FullTranslate(ctx context.Context, s SQLTranslator) ([]Record, error) {
	// As RANGE DEFAULT is the root of all zone configurations (including other
	// named zones for the system tenant), we can construct the entire span
	// configuration state by starting from RANGE DEFAULT.
	return s.Translate(ctx, descpb.IDs{keys.RootNamespaceID},
		true /* generateSystemSpanConfigurations */)
}

// SQLWatcherHandler is the signature of a handler that can be passed into
// SQLWatcher.WatchForSQLUpdates as described below.
type SQLWatcherHandler func(context.Context, []SQLUpdate, hlc.Timestamp) error

// SQLWatcher watches for events on system.zones and system.descriptors.
type SQLWatcher interface {
	// WatchForSQLUpdates watches for updates to zones and descriptors starting
	// at the given timestamp (exclusive), informing callers periodically using
	// the given handler[1] and a checkpoint timestamp. The handler is invoked:
	// - serially, in the same thread where WatchForSQLUpdates was called;
	// - with a monotonically increasing timestamp;
	// - with updates from the last provided timestamp (exclusive) to the
	//   current one (inclusive).
	//
	// If the handler errors out, it's not invoked subsequently (and internal
	// processes are wound down accordingly). Callers are free to persist the
	// checkpoint timestamps and use it to re-establish the watcher without
	// missing any updates.
	//
	// [1]: Users should avoid doing expensive work in the handler.
	//
	// TODO(arul): Possibly get rid of this limitation.
	WatchForSQLUpdates(
		ctx context.Context,
		startTS hlc.Timestamp,
		handler SQLWatcherHandler,
	) error
}

// Reconciler is responsible for reconciling a tenant's zone configs (SQL
// construct) with the cluster's span configs (KV construct). It's the central
// engine for the span configs infrastructure; a single Reconciler instance is
// active for every tenant in the system.
type Reconciler interface {
	// Reconcile starts the incremental reconciliation process from the given
	// timestamp. If it does not find MVCC history going far back enough[1], it
	// falls back to a scan of all descriptors and zone configs before being
	// able to do more incremental work. The provided callback is invoked
	// whenever incremental progress has been made and a Checkpoint() timestamp
	// is available. A future Reconcile() attempt can make use of this timestamp
	// to reduce the amount of necessary work (provided the MVCC history is
	// still available).
	//
	// Every reconciliation process is associated with an underlying sqlliveness
	// session. Typically, this is the session associated with the auto span
	// config job driving the entire reconciliation process. Any updates issued
	// by the reconciliation process must be performed at a valid timestamp which
	// is within the [start, expiration) of the session.
	//
	// [1]: It's possible for system.{zones,descriptor} to have been GC-ed away;
	//      think suspended tenants.
	Reconcile(
		ctx context.Context,
		startTS hlc.Timestamp,
		session sqlliveness.Session,
		onCheckpoint func() error,
	) error

	// Checkpoint returns a timestamp suitable for checkpointing. A future
	// Reconcile() attempt can make use of this timestamp to reduce the
	// amount of necessary work (provided the MVCC history is
	// still available).
	Checkpoint() hlc.Timestamp
}

// Store is a data structure used to store spans and their corresponding
// configs.
type Store interface {
	StoreWriter
	StoreReader
	// ForEachOverlappingSpanConfig invokes the supplied callback on each span
	// config that overlaps with the supplied span. The config is combined with
	// all the system span configs that also apply to this span. In addition to
	// the SpanConfig, the span it applies over is passed into the callback as
	// well.
	//
	// If there are no overlapping configs for the supplied span, the supplied
	// callback is invoked on the fallback config combined with any applicable
	// system span configs
	ForEachOverlappingSpanConfig(
		context.Context, roachpb.Span, func(roachpb.Span, roachpb.SpanConfig) error,
	) error
}

// StoreWriter is the write-only portion of the Store interface.
type StoreWriter interface {
	// Apply applies a batch of non-overlapping updates atomically and returns (i)
	// the existing spans that were deleted, and (ii) the entries that were newly
	// added to make room for the batch.
	//
	// Span configs are stored in non-overlapping fashion. When an update
	// overlaps with existing configs, the existing configs are deleted. If the
	// overlap is only partial, the non-overlapping components of the existing
	// configs are re-added. If the update itself is adding an entry, that too
	// is added. This is best illustrated with the following example:
	//
	//                                        [--- X --) is a span with config X
	//                                        [xxxxxxxx) is a span being deleted
	//
	//  Store    | [--- A ----)[------------- B -----------)[---------- C -----)
	//  Update   |             [------------------ D -------------)
	//           |
	//  Deleted  |             [------------- B -----------)[---------- C -----)
	//  Added    |             [------------------ D -------------)[--- C -----)
	//  Store*   | [--- A ----)[------------------ D -------------)[--- C -----)
	//
	// Generalizing to multiple updates:
	//
	//  Store    | [--- A ----)[------------- B -----------)[---------- C -----)
	//  Updates  |             [--- D ----)        [xxxxxxxxx)       [--- E ---)
	//           |
	//  Deleted  |             [------------- B -----------)[---------- C -----)
	//  Added    |             [--- D ----)[-- B --)         [-- C -)[--- E ---)
	//  Store*   | [--- A ----)[--- D ----)[-- B --)         [-- C -)[--- E ---)
	Apply(ctx context.Context, updates ...Update) (
		deleted []Target, added []Record,
	)
}

// StoreReader is the read-only portion of the Store interface. It doubles as an
// adaptor interface for config.SystemConfig.
type StoreReader interface {
	NeedsSplit(ctx context.Context, start, end roachpb.RKey) (bool, error)
	ComputeSplitKey(ctx context.Context, start, end roachpb.RKey) (roachpb.RKey, error)
	// GetSpanConfigForKey returns the span configuration for the
	// given key and the span that the retruened configuration
	// applies to. Callers can use the returned span to check if a
	// request is completely contained by the returned config.
	GetSpanConfigForKey(ctx context.Context, key roachpb.RKey) (roachpb.SpanConfig, roachpb.Span, error)
}

// Limiter is used to limit the number of span configs installed by secondary
// tenants. It takes in a delta (typically the difference in span configs
// between the committed and uncommitted state in the txn), uses it to maintain
// an aggregate counter, and informs the caller if exceeding the prescribed
// limit.
type Limiter interface {
	ShouldLimit(ctx context.Context, txn *kv.Txn, delta int) (bool, error)
}

// Splitter returns the number of split points for the given table descriptor.
// It steps through every "unit" that we can apply configurations over (table,
// indexes, partitions and sub-partitions) and figures out the actual key
// boundaries that we may need to split over. For example:
//
//	CREATE TABLE db.parts(i INT PRIMARY KEY, j INT) PARTITION BY LIST (i) (
//		PARTITION one_and_five    VALUES IN (1, 5),
//		PARTITION four_and_three  VALUES IN (4, 3),
//		PARTITION everything_else VALUES IN (6, default)
//	);
//
// We'd spit out 15:
//
//   - 1  between start of table and start of 1st index
//   - 1  between start of index and start of 1st partition-by-list value
//   - 1  for 1st partition-by-list value
//   - 1  for 2nd partition-by-list value
//   - 1  for 3rd partition-by-list value
//   - 1  for 4th partition-by-list value
//   - 1  for 5th partition-by-list value
//   - 1  for 6th partition-by-list value
//   - 5  gap(s) between 6 partition-by-list value spans
//   - 1  between end of 6th partition-by-list value span and end of index
//   - 13 for 1st index
//   - 1  between end of 1st index and end of table
//     = 15
type Splitter interface {
	Splits(ctx context.Context, table catalog.TableDescriptor) (int, error)
}

// Delta considers both the committed and uncommitted state of a table
// descriptor and computes the difference in the number of spans we can apply a
// configuration over.
func Delta(
	ctx context.Context, s Splitter, committed, uncommitted catalog.TableDescriptor,
) (int, error) {
	if committed == nil && uncommitted == nil {
		log.Fatalf(ctx, "unexpected: got two nil table descriptors")
	}

	var nonNilDesc catalog.TableDescriptor
	if committed != nil {
		nonNilDesc = committed
	} else {
		nonNilDesc = uncommitted
	}
	if nonNilDesc.GetParentID() == systemschema.SystemDB.GetID() {
		return 0, nil // we don't count tables in the system database
	}

	uncommittedSplits, err := s.Splits(ctx, uncommitted)
	if err != nil {
		return 0, err
	}

	committedSplits, err := s.Splits(ctx, committed)
	if err != nil {
		return 0, err
	}

	delta := uncommittedSplits - committedSplits
	return delta, nil
}

// Reporter generates a conformance report over the given spans, i.e. whether
// the backing ranges conform to the span configs that apply to them.
//
// NB: The standard implementation does not use a point-in-time snapshot of span
// config state, but could be made to do so if needed. See commentary on the
// spanconfigreporter.Reporter type for more details ("... we might not have a
// point-in-time snapshot ...").
type Reporter interface {
	SpanConfigConformance(
		ctx context.Context, spans []roachpb.Span,
	) (roachpb.SpanConfigConformanceReport, error)
}

// SQLUpdate captures either a descriptor or a protected timestamp update.
// It is the unit emitted by the SQLWatcher.
type SQLUpdate struct {
	descriptorUpdate         DescriptorUpdate
	protectedTimestampUpdate ProtectedTimestampUpdate
}

// MakeDescriptorSQLUpdate returns a SQLUpdate that represents an update to a
// descriptor.
func MakeDescriptorSQLUpdate(id descpb.ID, descType catalog.DescriptorType) SQLUpdate {
	return SQLUpdate{descriptorUpdate: DescriptorUpdate{
		ID:   id,
		Type: descType,
	}}
}

// GetDescriptorUpdate returns a DescriptorUpdate.
func (d *SQLUpdate) GetDescriptorUpdate() DescriptorUpdate {
	return d.descriptorUpdate
}

// IsDescriptorUpdate returns true if the SQLUpdate represents an update to a
// descriptor.
func (d *SQLUpdate) IsDescriptorUpdate() bool {
	return d.descriptorUpdate != DescriptorUpdate{}
}

// MakeTenantProtectedTimestampSQLUpdate returns a SQLUpdate that represents an update
// to a protected timestamp record with a tenant target.
func MakeTenantProtectedTimestampSQLUpdate(tenantID roachpb.TenantID) SQLUpdate {
	return SQLUpdate{protectedTimestampUpdate: ProtectedTimestampUpdate{TenantTarget: tenantID}}
}

// MakeClusterProtectedTimestampSQLUpdate returns a SQLUpdate that represents an update
// to a protected timestamp record with a cluster target.
func MakeClusterProtectedTimestampSQLUpdate() SQLUpdate {
	return SQLUpdate{protectedTimestampUpdate: ProtectedTimestampUpdate{ClusterTarget: true}}
}

// GetProtectedTimestampUpdate returns the target of the updated protected
// timestamp record.
func (d *SQLUpdate) GetProtectedTimestampUpdate() ProtectedTimestampUpdate {
	return d.protectedTimestampUpdate
}

// IsProtectedTimestampUpdate returns true if the SQLUpdate represents an update
// to a protected timestamp record.
func (d *SQLUpdate) IsProtectedTimestampUpdate() bool {
	return d.protectedTimestampUpdate != ProtectedTimestampUpdate{}
}

// DescriptorUpdate captures the ID and the type of descriptor or zone that been
// updated.
type DescriptorUpdate struct {
	// ID of the descriptor/zone that has been updated.
	ID descpb.ID

	// Type of the descriptor/zone that has been updated. Could be either
	// the specific type or catalog.Any if no information is available.
	Type catalog.DescriptorType
}

// ProtectedTimestampUpdate captures a protected timestamp record with a cluster
// or tenant target that been updated.
type ProtectedTimestampUpdate struct {
	// ClusterTarget is set if the pts record targets a cluster.
	ClusterTarget bool
	// TenantsTarget is set if the pts record targets a tenant.
	TenantTarget roachpb.TenantID
}

// IsClusterUpdate returns true if the ProtectedTimestampUpdate has a cluster
// target.
func (p *ProtectedTimestampUpdate) IsClusterUpdate() bool {
	return p.ClusterTarget
}

// IsTenantsUpdate returns true if the ProtectedTimestampUpdate has a tenants
// target.
func (p *ProtectedTimestampUpdate) IsTenantsUpdate() bool {
	return !p.ClusterTarget
}

// Update captures a span and the corresponding config change. It's the unit of
// what can be applied to a StoreWriter. The embedded span captures what's being
// updated; the config captures what it's being updated to. An empty config
// indicates a deletion.
type Update Record

// Deletion constructs an update that represents a deletion over the given
// target.
func Deletion(target Target) (Update, error) {
	record, err := MakeRecord(target, roachpb.SpanConfig{}) // delete
	if err != nil {
		return Update{}, err
	}
	return Update(record), nil
}

// Addition constructs an update that represents adding the given config over
// the given target.
func Addition(target Target, conf roachpb.SpanConfig) (Update, error) {
	record, err := MakeRecord(target, conf)
	if err != nil {
		return Update{}, err
	}
	return Update(record), nil
}

// Deletion returns true if the update corresponds to a span config being
// deleted.
func (u Update) Deletion() bool {
	return u.config.IsEmpty()
}

// Addition returns true if the update corresponds to a span config being added.
func (u Update) Addition() bool {
	return !u.Deletion()
}

// GetTarget returns the underlying spanconfig.Record target.
func (u Update) GetTarget() Target {
	return u.target
}

// GetConfig returns the underlying spanconfig.Record config.
func (u Update) GetConfig() roachpb.SpanConfig {
	return u.config
}

// ProtectedTSReader is the read-only portion for querying protected
// timestamp information. It doubles up as an adaptor interface for
// protectedts.Cache.
type ProtectedTSReader interface {
	// GetProtectionTimestamps returns all protected timestamps that apply to any
	// part of the given key span. The time at which this protected timestamp
	// state is valid is returned as well.
	GetProtectionTimestamps(ctx context.Context, sp roachpb.Span) (
		protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, _ error,
	)
}

// EmptyProtectedTSReader returns a ProtectedTSReader which contains no records
// and is always up-to date. This is intended for testing.
func EmptyProtectedTSReader(c *hlc.Clock) ProtectedTSReader {
	return (*emptyProtectedTSReader)(c)
}

type emptyProtectedTSReader hlc.Clock

// GetProtectionTimestamps is part of the spanconfig.ProtectedTSReader
// interface.
func (r *emptyProtectedTSReader) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) ([]hlc.Timestamp, hlc.Timestamp, error) {
	return nil, (*hlc.Clock)(r).Now(), nil
}
