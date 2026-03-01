// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sessionmutator

import (
	"context"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/redact"
)

// SessionDefaults mirrors fields in Session, for restoring default
// configuration values in SET ... TO DEFAULT (or RESET ...) statements.
type SessionDefaults map[string]string

// SafeFormat implements the redact.SafeFormatter interface.
// An example output for SessionDefaults SafeFormat:
// [disallow_full_table_scans=‹true›; database=‹test›; statement_timeout=‹250ms›]
func (sd SessionDefaults) SafeFormat(s redact.SafePrinter, _ rune) {
	s.Printf("[")
	addSemiColon := false
	// Iterate through map in alphabetical order.
	sortedKeys := make([]string, 0, len(sd))
	for k := range sd {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		if addSemiColon {
			s.Print(redact.SafeString("; "))
		}
		s.Printf("%s=%s", redact.SafeString(k), sd[k])
		addSemiColon = true
	}
	s.Printf("]")
}

// String implements the fmt.Stringer interface.
func (sd SessionDefaults) String() string {
	return redact.StringWithoutMarkers(sd)
}

// SessionDataMutatorBase contains elements in a sessionDataMutator
// which is the same across all SessionData elements in the sessiondata.Stack.
type SessionDataMutatorBase struct {
	Defaults SessionDefaults
	Settings *cluster.Settings
}

// ParamStatusUpdater is a subset of RestrictedCommandResult which allows sending
// status updates. Ensure all updatable settings are in bufferableParamStatusUpdates.
type ParamStatusUpdater interface {
	BufferParamStatusUpdate(string, string)
}

// SessionDataMutatorCallbacks contains elements in a SessionDataMutator
// which are only populated when mutating the "top" sessionData element.
// It is intended for functions which should only be called once per SET
// (e.g. param status updates, which only should be sent once within
// a transaction where there may be two or more SessionData elements in
// the stack)
type SessionDataMutatorCallbacks struct {
	// ParamStatusUpdater is called when there is a ParamStatusUpdate.
	// It can be nil, in which case nothing triggers on execution.
	ParamStatusUpdater ParamStatusUpdater
	// SetCurTxnReadOnly is called when we execute SET transaction_read_only = ...
	// It can be nil, in which case nothing triggers on execution.
	SetCurTxnReadOnly func(readOnly bool) error
	// SetBufferedWritesEnabled is called when we execute SET kv_transaction_buffered_writes_enabled = ...
	// It can be nil, in which case nothing triggers on execution (apart from
	// modification of the session data).
	SetBufferedWritesEnabled func(enabled bool)
	// UpgradedIsolationLevel is called whenever the transaction isolation
	// session variable is configured and the isolation level is automatically
	// upgraded to a stronger one. It's also used when the isolation level is
	// upgraded in BEGIN or SET TRANSACTION statements.
	UpgradedIsolationLevel func(ctx context.Context, upgradedFrom tree.IsolationLevel)
	// OnTempSchemaCreation is called when the temporary schema is set
	// on the search path (the first and only time).
	// It can be nil, in which case nothing triggers on execution.
	OnTempSchemaCreation func()
	// OnDefaultIntSizeChange is called when default_int_size changes. It is
	// needed because the pgwire connection's read buffer needs to be aware
	// of the default int size in order to be able to parse the unqualified
	// INT type correctly.
	OnDefaultIntSizeChange func(int32)
	// OnApplicationNameChange is called when the application_name changes. It is
	// needed because the stats writer needs to be notified of changes to the
	// application name.
	OnApplicationNameChange func(string)
	// OnTCPKeepAliveSettingChange is called when any of the TCP keepalive session
	// variables change. Zero values mean "use the cluster setting default."
	OnTCPKeepAliveSettingChange func(idle, interval time.Duration, count int, userTimeout time.Duration)
}

// SessionDataMutatorIterator generates SessionDataMutators which allow
// the changing of SessionData on some element inside the sessiondata Stack.
type SessionDataMutatorIterator struct {
	SessionDataMutatorBase
	Sds *sessiondata.Stack
	SessionDataMutatorCallbacks
}

func MakeSessionDataMutatorIterator(
	sds *sessiondata.Stack, defaults SessionDefaults, settings *cluster.Settings,
) *SessionDataMutatorIterator {
	return &SessionDataMutatorIterator{
		Sds: sds,
		SessionDataMutatorBase: SessionDataMutatorBase{
			Defaults: defaults,
			Settings: settings,
		},
		SessionDataMutatorCallbacks: SessionDataMutatorCallbacks{},
	}
}

// Mutator returns a Mutator for the given sessionData.
func (it *SessionDataMutatorIterator) Mutator(
	applyCallbacks bool, sd *sessiondata.SessionData,
) SessionDataMutator {
	ret := SessionDataMutator{
		Data:                   sd,
		SessionDataMutatorBase: it.SessionDataMutatorBase,
	}
	// We usually apply callbacks on the first element in the stack, as the txn
	// rollback will always reset to the first element we touch in the stack,
	// in which case it should be up-to-date by default.
	if applyCallbacks {
		ret.SessionDataMutatorCallbacks = it.SessionDataMutatorCallbacks
	}
	return ret
}

// SetSessionDefaultIntSize sets the default int size for the session.
// It is exported for use in import which is a CCL package.
func (it *SessionDataMutatorIterator) SetSessionDefaultIntSize(size int32) {
	it.ApplyOnEachMutator(func(m SessionDataMutator) {
		m.SetDefaultIntSize(size)
	})
}

// ApplyOnTopMutator applies the given function on the mutator for the top
// element on the sessiondata Stack only.
func (it *SessionDataMutatorIterator) ApplyOnTopMutator(
	applyFunc func(m SessionDataMutator) error,
) error {
	return applyFunc(it.Mutator(true /* applyCallbacks */, it.Sds.Top()))
}

// ApplyOnEachMutator iterates over each mutator over all SessionData elements
// in the stack and applies the given function to them.
// It is the equivalent of SET SESSION x = y.
func (it *SessionDataMutatorIterator) ApplyOnEachMutator(applyFunc func(m SessionDataMutator)) {
	elems := it.Sds.Elems()
	for i, sd := range elems {
		applyFunc(it.Mutator(i == 0, sd))
	}
}

// ApplyOnEachMutatorError is the same as ApplyOnEachMutator, but takes in a function
// that can return an error, erroring if any of applications error.
func (it *SessionDataMutatorIterator) ApplyOnEachMutatorError(
	applyFunc func(m SessionDataMutator) error,
) error {
	elems := it.Sds.Elems()
	for i, sd := range elems {
		if err := applyFunc(it.Mutator(i == 0, sd)); err != nil {
			return err
		}
	}
	return nil
}

// SessionDataMutator is the object used by sessionVars to change the session
// state. It mostly mutates the session's SessionData, but not exclusively (e.g.
// see curTxnReadOnly).
type SessionDataMutator struct {
	Data *sessiondata.SessionData
	SessionDataMutatorBase
	SessionDataMutatorCallbacks
}

func (m *SessionDataMutator) BufferParamStatusUpdate(param string, status string) {
	if m.ParamStatusUpdater != nil {
		m.ParamStatusUpdater.BufferParamStatusUpdate(param, status)
	}
}

// SetApplicationName sets the application name.
func (m *SessionDataMutator) SetApplicationName(appName string) {
	oldName := m.Data.ApplicationName
	m.Data.ApplicationName = appName
	if m.OnApplicationNameChange != nil {
		m.OnApplicationNameChange(appName)
	}
	if oldName != appName {
		m.BufferParamStatusUpdate("application_name", appName)
	}
}

// SetAvoidBuffering sets avoid buffering option.
func (m *SessionDataMutator) SetAvoidBuffering(b bool) {
	m.Data.AvoidBuffering = b
}

func (m *SessionDataMutator) SetBytesEncodeFormat(val lex.BytesEncodeFormat) {
	m.Data.DataConversionConfig.BytesEncodeFormat = val
}

func (m *SessionDataMutator) SetExtraFloatDigits(val int32) {
	m.Data.DataConversionConfig.ExtraFloatDigits = val
}

func (m *SessionDataMutator) SetDatabase(dbName string) {
	m.Data.Database = dbName
}

func (m *SessionDataMutator) SetTemporarySchemaName(scName string) {
	if m.OnTempSchemaCreation != nil {
		m.OnTempSchemaCreation()
	}
	m.Data.SearchPath = m.Data.SearchPath.WithTemporarySchemaName(scName)
}

func (m *SessionDataMutator) SetTemporarySchemaIDForDatabase(dbID uint32, tempSchemaID uint32) {
	if m.Data.DatabaseIDToTempSchemaID == nil {
		m.Data.DatabaseIDToTempSchemaID = make(map[uint32]uint32)
	}
	m.Data.DatabaseIDToTempSchemaID[dbID] = tempSchemaID
}

func (m *SessionDataMutator) SetDefaultIntSize(size int32) {
	m.Data.DefaultIntSize = size
	if m.OnDefaultIntSizeChange != nil {
		m.OnDefaultIntSizeChange(size)
	}
}

func (m *SessionDataMutator) SetDefaultTransactionPriority(val tree.UserPriority) {
	m.Data.DefaultTxnPriority = int64(val)
}

func (m *SessionDataMutator) SetDefaultTransactionIsolationLevel(val tree.IsolationLevel) {
	m.Data.DefaultTxnIsolationLevel = int64(val)
}

func (m *SessionDataMutator) SetDefaultTransactionReadOnly(val bool) {
	m.Data.DefaultTxnReadOnly = val
}

func (m *SessionDataMutator) SetDefaultTransactionUseFollowerReads(val bool) {
	m.Data.DefaultTxnUseFollowerReads = val
}

func (m *SessionDataMutator) SetEnableSeqScan(val bool) {
	m.Data.EnableSeqScan = val
}

func (m *SessionDataMutator) SetSynchronousCommit(val bool) {
	m.Data.SynchronousCommit = val
}

func (m *SessionDataMutator) SetDirectColumnarScansEnabled(b bool) {
	m.Data.DirectColumnarScansEnabled = b
}

func (m *SessionDataMutator) SetDisablePlanGists(val bool) {
	m.Data.DisablePlanGists = val
}

func (m *SessionDataMutator) SetDistSQLMode(val sessiondatapb.DistSQLExecMode) {
	m.Data.DistSQLMode = val
}

func (m *SessionDataMutator) SetDistSQLWorkMem(val int64) {
	m.Data.WorkMemLimit = val
}

func (m *SessionDataMutator) SetForceSavepointRestart(val bool) {
	m.Data.ForceSavepointRestart = val
}

func (m *SessionDataMutator) SetZigzagJoinEnabled(val bool) {
	m.Data.ZigzagJoinEnabled = val
}

func (m *SessionDataMutator) SetIndexRecommendationsEnabled(val bool) {
	m.Data.IndexRecommendationsEnabled = val
}

func (m *SessionDataMutator) SetExperimentalDistSQLPlanning(
	val sessiondatapb.ExperimentalDistSQLPlanningMode,
) {
	m.Data.ExperimentalDistSQLPlanningMode = val
}

func (m *SessionDataMutator) SetPartiallyDistributedPlansDisabled(val bool) {
	m.Data.PartiallyDistributedPlansDisabled = val
}

func (m *SessionDataMutator) SetDistributeGroupByRowCountThreshold(val uint64) {
	m.Data.DistributeGroupByRowCountThreshold = val
}

func (m *SessionDataMutator) SetDistributeSortRowCountThreshold(val uint64) {
	m.Data.DistributeSortRowCountThreshold = val
}

func (m *SessionDataMutator) SetDistributeScanRowCountThreshold(val uint64) {
	m.Data.DistributeScanRowCountThreshold = val
}

func (m *SessionDataMutator) SetAlwaysDistributeFullScans(val bool) {
	m.Data.AlwaysDistributeFullScans = val
}

func (m *SessionDataMutator) SetUseSoftLimitForDistributeScan(val bool) {
	m.Data.UseSoftLimitForDistributeScan = val
}

func (m *SessionDataMutator) SetDistributeJoinRowCountThreshold(val uint64) {
	m.Data.DistributeJoinRowCountThreshold = val
}

func (m *SessionDataMutator) SetDisableVecUnionEagerCancellation(val bool) {
	m.Data.DisableVecUnionEagerCancellation = val
}

func (m *SessionDataMutator) SetRequireExplicitPrimaryKeys(val bool) {
	m.Data.RequireExplicitPrimaryKeys = val
}

func (m *SessionDataMutator) SetReorderJoinsLimit(val int) {
	m.Data.ReorderJoinsLimit = int64(val)
}

func (m *SessionDataMutator) SetVectorize(val sessiondatapb.VectorizeExecMode) {
	m.Data.VectorizeMode = val
}

func (m *SessionDataMutator) SetTestingVectorizeInjectPanics(val bool) {
	m.Data.TestingVectorizeInjectPanics = val
}

func (m *SessionDataMutator) SetTestingOptimizerInjectPanics(val bool) {
	m.Data.TestingOptimizerInjectPanics = val
}

func (m *SessionDataMutator) SetOptimizerFKCascadesLimit(val int) {
	m.Data.OptimizerFKCascadesLimit = int64(val)
}

func (m *SessionDataMutator) SetOptimizerUseForecasts(val bool) {
	m.Data.OptimizerUseForecasts = val
}

func (m *SessionDataMutator) SetOptimizerUseMergedPartialStatistics(val bool) {
	m.Data.OptimizerUseMergedPartialStatistics = val
}

func (m *SessionDataMutator) SetOptimizerUseHistograms(val bool) {
	m.Data.OptimizerUseHistograms = val
}

func (m *SessionDataMutator) SetOptimizerUseMultiColStats(val bool) {
	m.Data.OptimizerUseMultiColStats = val
}

func (m *SessionDataMutator) SetOptimizerUseNotVisibleIndexes(val bool) {
	m.Data.OptimizerUseNotVisibleIndexes = val
}

func (m *SessionDataMutator) SetOptimizerMergeJoinsEnabled(val bool) {
	m.Data.OptimizerMergeJoinsEnabled = val
}

func (m *SessionDataMutator) SetLocalityOptimizedSearch(val bool) {
	m.Data.LocalityOptimizedSearch = val
}

func (m *SessionDataMutator) SetImplicitSelectForUpdate(val bool) {
	m.Data.ImplicitSelectForUpdate = val
}

func (m *SessionDataMutator) SetInsertFastPath(val bool) {
	m.Data.InsertFastPath = val
}

func (m *SessionDataMutator) SetSerialNormalizationMode(val sessiondatapb.SerialNormalizationMode) {
	m.Data.SerialNormalizationMode = val
}

func (m *SessionDataMutator) SetSafeUpdates(val bool) {
	m.Data.SafeUpdates = val
}

func (m *SessionDataMutator) SetCheckFunctionBodies(val bool) {
	m.Data.CheckFunctionBodies = val
}

func (m *SessionDataMutator) SetPreferLookupJoinsForFKs(val bool) {
	m.Data.PreferLookupJoinsForFKs = val
}

func (m *SessionDataMutator) UpdateSearchPath(paths []string) {
	m.Data.SearchPath = m.Data.SearchPath.UpdatePaths(paths)
}

func (m *SessionDataMutator) SetStrictDDLAtomicity(val bool) {
	m.Data.StrictDDLAtomicity = val
}

func (m *SessionDataMutator) SetAutoCommitBeforeDDL(val bool) {
	m.Data.AutoCommitBeforeDDL = val
}

// TimeZoneFormat returns the appropriate timezone format
// to output when the `timezone` is required output.
// If the time zone is a "fixed offset" one, initialized from an offset
// and not a standard name, then we use a magic format in the Location's
// name. We attempt to parse that here and retrieve the original offset
// specified by the user.
func TimeZoneFormat(loc *time.Location) string {
	locStr := loc.String()
	_, origRepr, parsed := timeutil.ParseTimeZoneOffset(locStr, timeutil.TimeZoneStringToLocationISO8601Standard)
	if parsed {
		return origRepr
	}
	return locStr
}

func (m *SessionDataMutator) SetLocation(loc *time.Location) {
	oldLocation := TimeZoneFormat(m.Data.Location)
	m.Data.Location = loc
	if formatted := TimeZoneFormat(loc); oldLocation != formatted {
		m.BufferParamStatusUpdate("TimeZone", formatted)
	}
}

func (m *SessionDataMutator) SetCustomOption(name, val string) {
	if m.Data.CustomOptions == nil {
		m.Data.CustomOptions = make(map[string]string)
	}
	m.Data.CustomOptions[name] = val
}

func (m *SessionDataMutator) SetReadOnly(val bool) error {
	// The read-only state is special; it's set as a session variable (SET
	// transaction_read_only=<>), but it represents per-txn state, not
	// per-session. There's no field for it in the SessionData struct. Instead, we
	// call into the connEx, which modifies its TxnState. This is similar to
	// transaction_isolation.
	if m.SetCurTxnReadOnly != nil {
		return m.SetCurTxnReadOnly(val)
	}
	return nil
}

func (m *SessionDataMutator) SetStmtTimeout(timeout time.Duration) {
	m.Data.StmtTimeout = timeout
}

func (m *SessionDataMutator) SetLockTimeout(timeout time.Duration) {
	m.Data.LockTimeout = timeout
}

func (m *SessionDataMutator) SetDeadlockTimeout(timeout time.Duration) {
	m.Data.DeadlockTimeout = timeout
}

func (m *SessionDataMutator) SetIdleInSessionTimeout(timeout time.Duration) {
	m.Data.IdleInSessionTimeout = timeout
}

func (m *SessionDataMutator) SetIdleInTransactionSessionTimeout(timeout time.Duration) {
	m.Data.IdleInTransactionSessionTimeout = timeout
}

func (m *SessionDataMutator) SetTransactionTimeout(timeout time.Duration) {
	m.Data.TransactionTimeout = timeout
}

func (m *SessionDataMutator) SetAllowPrepareAsOptPlan(val bool) {
	m.Data.AllowPrepareAsOptPlan = val
}

func (m *SessionDataMutator) SetRowSecurity(val bool) {
	m.Data.RowSecurity = val
}

func (m *SessionDataMutator) SetSaveTablesPrefix(prefix string) {
	m.Data.SaveTablesPrefix = prefix
}

func (m *SessionDataMutator) SetPlacementEnabled(val bool) {
	m.Data.PlacementEnabled = val
}

func (m *SessionDataMutator) SetAutoRehomingEnabled(val bool) {
	m.Data.AutoRehomingEnabled = val
}

func (m *SessionDataMutator) SetOnUpdateRehomeRowEnabled(val bool) {
	m.Data.OnUpdateRehomeRowEnabled = val
}

func (m *SessionDataMutator) SetTempTablesEnabled(val bool) {
	m.Data.TempTablesEnabled = val
}

func (m *SessionDataMutator) SetImplicitColumnPartitioningEnabled(val bool) {
	m.Data.ImplicitColumnPartitioningEnabled = val
}

func (m *SessionDataMutator) SetOverrideMultiRegionZoneConfigEnabled(val bool) {
	m.Data.OverrideMultiRegionZoneConfigEnabled = val
}

func (m *SessionDataMutator) SetDisallowFullTableScans(val bool) {
	m.Data.DisallowFullTableScans = val
}

func (m *SessionDataMutator) SetAvoidFullTableScansInMutations(val bool) {
	m.Data.AvoidFullTableScansInMutations = val
}

func (m *SessionDataMutator) SetAlterColumnTypeGeneral(val bool) {
	m.Data.AlterColumnTypeGeneralEnabled = val
}

func (m *SessionDataMutator) SetAllowViewWithSecurityInvokerClause(val bool) {
	m.Data.AllowViewWithSecurityInvokerClause = val
}

func (m *SessionDataMutator) SetEnableSuperRegions(val bool) {
	m.Data.EnableSuperRegions = val
}

func (m *SessionDataMutator) SetEnableOverrideAlterPrimaryRegionInSuperRegion(val bool) {
	m.Data.OverrideAlterPrimaryRegionInSuperRegion = val
}

// TODO(rytaft): remove this once unique without index constraints are fully
// supported.
func (m *SessionDataMutator) SetUniqueWithoutIndexConstraints(val bool) {
	m.Data.EnableUniqueWithoutIndexConstraints = val
}

func (m *SessionDataMutator) SetUseNewSchemaChanger(val sessiondatapb.NewSchemaChangerMode) {
	m.Data.NewSchemaChangerMode = val
}

func (m *SessionDataMutator) SetDescriptorValidationMode(
	val sessiondatapb.DescriptorValidationMode,
) {
	m.Data.DescriptorValidationMode = val
}

func (m *SessionDataMutator) SetQualityOfService(val sessiondatapb.QoSLevel) {
	m.Data.DefaultTxnQualityOfService = val.Validate()
}

func (m *SessionDataMutator) SetCopyQualityOfService(val sessiondatapb.QoSLevel) {
	m.Data.CopyTxnQualityOfService = val.Validate()
}

func (m *SessionDataMutator) SetCopyWritePipeliningEnabled(val bool) {
	m.Data.CopyWritePipeliningEnabled = val
}

func (m *SessionDataMutator) SetCopyNumRetriesPerBatch(val int32) {
	m.Data.CopyNumRetriesPerBatch = val
}

func (m *SessionDataMutator) SetOptSplitScanLimit(val int32) {
	m.Data.OptSplitScanLimit = val
}

func (m *SessionDataMutator) SetStreamReplicationEnabled(val bool) {
	m.Data.EnableStreamReplication = val
}

// RecordLatestSequenceVal records that value to which the session incremented
// a sequence.
func (m *SessionDataMutator) RecordLatestSequenceVal(seqID uint32, val int64) {
	m.Data.SequenceState.RecordValue(seqID, val)
}

// SetNoticeDisplaySeverity sets the NoticeDisplaySeverity for the given session.
func (m *SessionDataMutator) SetNoticeDisplaySeverity(severity pgnotice.DisplaySeverity) {
	m.Data.NoticeDisplaySeverity = uint32(severity)
}

// initSequenceCache creates an empty sequence cache instance for the session.
func (m *SessionDataMutator) InitSequenceCache() {
	m.Data.SequenceCache = sessiondatapb.SequenceCache{}
}

// SetIntervalStyle sets the IntervalStyle for the given session.
func (m *SessionDataMutator) SetIntervalStyle(style duration.IntervalStyle) {
	oldStyle := m.Data.DataConversionConfig.IntervalStyle
	m.Data.DataConversionConfig.IntervalStyle = style
	if oldStyle != style {
		m.BufferParamStatusUpdate("IntervalStyle", strings.ToLower(style.String()))
	}
}

// SetDateStyle sets the DateStyle for the given session.
func (m *SessionDataMutator) SetDateStyle(style pgdate.DateStyle) {
	oldStyle := m.Data.DataConversionConfig.DateStyle
	m.Data.DataConversionConfig.DateStyle = style
	if oldStyle != style {
		m.BufferParamStatusUpdate("DateStyle", style.SQLString())
	}
}

// SetStubCatalogTablesEnabled sets default value for stub_catalog_tables.
func (m *SessionDataMutator) SetStubCatalogTablesEnabled(enabled bool) {
	m.Data.StubCatalogTablesEnabled = enabled
}

func (m *SessionDataMutator) SetExperimentalComputedColumnRewrites(val string) {
	m.Data.ExperimentalComputedColumnRewrites = val
}

func (m *SessionDataMutator) SetNullOrderedLast(b bool) {
	m.Data.NullOrderedLast = b
}

func (m *SessionDataMutator) SetPropagateInputOrdering(b bool) {
	m.Data.PropagateInputOrdering = b
}

func (m *SessionDataMutator) SetTxnRowsWrittenLog(val int64) {
	m.Data.TxnRowsWrittenLog = val
}

func (m *SessionDataMutator) SetTxnRowsWrittenErr(val int64) {
	m.Data.TxnRowsWrittenErr = val
}

func (m *SessionDataMutator) SetTxnRowsReadLog(val int64) {
	m.Data.TxnRowsReadLog = val
}

func (m *SessionDataMutator) SetTxnRowsReadErr(val int64) {
	m.Data.TxnRowsReadErr = val
}

func (m *SessionDataMutator) SetLargeFullScanRows(val float64) {
	m.Data.LargeFullScanRows = val
}

func (m *SessionDataMutator) SetInjectRetryErrorsEnabled(val bool) {
	m.Data.InjectRetryErrorsEnabled = val
}

func (m *SessionDataMutator) SetMaxRetriesForReadCommitted(val int32) {
	m.Data.MaxRetriesForReadCommitted = val
}

func (m *SessionDataMutator) SetJoinReaderOrderingStrategyBatchSize(val int64) {
	m.Data.JoinReaderOrderingStrategyBatchSize = val
}

func (m *SessionDataMutator) SetJoinReaderNoOrderingStrategyBatchSize(val int64) {
	m.Data.JoinReaderNoOrderingStrategyBatchSize = val
}

func (m *SessionDataMutator) SetJoinReaderIndexJoinStrategyBatchSize(val int64) {
	m.Data.JoinReaderIndexJoinStrategyBatchSize = val
}

func (m *SessionDataMutator) SetIndexJoinStreamerBatchSize(val int64) {
	m.Data.IndexJoinStreamerBatchSize = val
}

func (m *SessionDataMutator) SetParallelizeMultiKeyLookupJoinsEnabled(val bool) {
	m.Data.ParallelizeMultiKeyLookupJoinsEnabled = val
}

func (m *SessionDataMutator) SetParallelizeMultiKeyLookupJoinsAvgLookupRatio(val float64) {
	m.Data.ParallelizeMultiKeyLookupJoinsAvgLookupRatio = val
}

func (m *SessionDataMutator) SetParallelizeMultiKeyLookupJoinsMaxLookupRatio(val float64) {
	m.Data.ParallelizeMultiKeyLookupJoinsMaxLookupRatio = val
}

func (m *SessionDataMutator) SetParallelizeMultiKeyLookupJoinsAvgLookupRowSize(val int64) {
	m.Data.ParallelizeMultiKeyLookupJoinsAvgLookupRowSize = val
}

func (m *SessionDataMutator) SetParallelizeMultiKeyLookupJoinsOnlyOnMRMutations(val bool) {
	m.Data.ParallelizeMultiKeyLookupJoinsOnlyOnMRMutations = val
}

// TODO(harding): Remove this when costing scans based on average column size
// is fully supported.
func (m *SessionDataMutator) SetCostScansWithDefaultColSize(val bool) {
	m.Data.CostScansWithDefaultColSize = val
}

func (m *SessionDataMutator) SetEnableImplicitTransactionForBatchStatements(val bool) {
	m.Data.EnableImplicitTransactionForBatchStatements = val
}

func (m *SessionDataMutator) SetExpectAndIgnoreNotVisibleColumnsInCopy(val bool) {
	m.Data.ExpectAndIgnoreNotVisibleColumnsInCopy = val
}

func (m *SessionDataMutator) SetMultipleModificationsOfTable(val bool) {
	m.Data.MultipleModificationsOfTable = val
}

func (m *SessionDataMutator) SetShowPrimaryKeyConstraintOnNotVisibleColumns(val bool) {
	m.Data.ShowPrimaryKeyConstraintOnNotVisibleColumns = val
}

func (m *SessionDataMutator) SetTestingOptimizerRandomSeed(val int64) {
	m.Data.TestingOptimizerRandomSeed = val
}

func (m *SessionDataMutator) SetTestingOptimizerCostPerturbation(val float64) {
	m.Data.TestingOptimizerCostPerturbation = val
}

func (m *SessionDataMutator) SetTestingOptimizerDisableRuleProbability(val float64) {
	m.Data.TestingOptimizerDisableRuleProbability = val
}

func (m *SessionDataMutator) SetDisableOptimizerRules(val []string) {
	m.Data.DisableOptimizerRules = val
}

func (m *SessionDataMutator) SetTrigramSimilarityThreshold(val float64) {
	m.Data.TrigramSimilarityThreshold = val
}

func (m *SessionDataMutator) SetUnconstrainedNonCoveringIndexScanEnabled(val bool) {
	m.Data.UnconstrainedNonCoveringIndexScanEnabled = val
}

func (m *SessionDataMutator) SetDisableHoistProjectionInJoinLimitation(val bool) {
	m.Data.DisableHoistProjectionInJoinLimitation = val
}

func (m *SessionDataMutator) SetTroubleshootingModeEnabled(val bool) {
	m.Data.TroubleshootingMode = val
}

func (m *SessionDataMutator) SetCopyFastPathEnabled(val bool) {
	m.Data.CopyFastPathEnabled = val
}

func (m *SessionDataMutator) SetCopyFromAtomicEnabled(val bool) {
	m.Data.CopyFromAtomicEnabled = val
}

func (m *SessionDataMutator) SetCopyFromRetriesEnabled(val bool) {
	m.Data.CopyFromRetriesEnabled = val
}

func (m *SessionDataMutator) SetDeclareCursorStatementTimeoutEnabled(val bool) {
	m.Data.DeclareCursorStatementTimeoutEnabled = val
}

func (m *SessionDataMutator) SetEnforceHomeRegion(val bool) {
	m.Data.EnforceHomeRegion = val
}

func (m *SessionDataMutator) SetVariableInequalityLookupJoinEnabled(val bool) {
	m.Data.VariableInequalityLookupJoinEnabled = val
}

func (m *SessionDataMutator) SetExperimentalHashGroupJoinEnabled(val bool) {
	m.Data.ExperimentalHashGroupJoinEnabled = val
}

func (m *SessionDataMutator) SetAllowOrdinalColumnReference(val bool) {
	m.Data.AllowOrdinalColumnReferences = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedDisjunctionStats(val bool) {
	m.Data.OptimizerUseImprovedDisjunctionStats = val
}

func (m *SessionDataMutator) SetOptimizerUseLimitOrderingForStreamingGroupBy(val bool) {
	m.Data.OptimizerUseLimitOrderingForStreamingGroupBy = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedSplitDisjunctionForJoins(val bool) {
	m.Data.OptimizerUseImprovedSplitDisjunctionForJoins = val
}

func (m *SessionDataMutator) SetInjectRetryErrorsOnCommitEnabled(val bool) {
	m.Data.InjectRetryErrorsOnCommitEnabled = val
}

func (m *SessionDataMutator) SetOptimizerAlwaysUseHistograms(val bool) {
	m.Data.OptimizerAlwaysUseHistograms = val
}

func (m *SessionDataMutator) SetOptimizerHoistUncorrelatedEqualitySubqueries(val bool) {
	m.Data.OptimizerHoistUncorrelatedEqualitySubqueries = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedComputedColumnFiltersDerivation(val bool) {
	m.Data.OptimizerUseImprovedComputedColumnFiltersDerivation = val
}

func (m *SessionDataMutator) SetEnableCreateStatsUsingExtremes(val bool) {
	m.Data.EnableCreateStatsUsingExtremes = val
}

func (m *SessionDataMutator) SetEnableCreateStatsUsingExtremesBoolEnum(val bool) {
	m.Data.EnableCreateStatsUsingExtremesBoolEnum = val
}

func (m *SessionDataMutator) SetAllowRoleMembershipsToChangeDuringTransaction(val bool) {
	m.Data.AllowRoleMembershipsToChangeDuringTransaction = val
}

func (m *SessionDataMutator) SetDefaultTextSearchConfig(val string) {
	m.Data.DefaultTextSearchConfig = val
}

func (m *SessionDataMutator) SetPreparedStatementsCacheSize(val int64) {
	m.Data.PreparedStatementsCacheSize = val
}

func (m *SessionDataMutator) SetStreamerEnabled(val bool) {
	m.Data.StreamerEnabled = val
}

func (m *SessionDataMutator) SetStreamerAlwaysMaintainOrdering(val bool) {
	m.Data.StreamerAlwaysMaintainOrdering = val
}

func (m *SessionDataMutator) SetStreamerInOrderEagerMemoryUsageFraction(val float64) {
	m.Data.StreamerInOrderEagerMemoryUsageFraction = val
}

func (m *SessionDataMutator) SetStreamerOutOfOrderEagerMemoryUsageFraction(val float64) {
	m.Data.StreamerOutOfOrderEagerMemoryUsageFraction = val
}

func (m *SessionDataMutator) SetStreamerHeadOfLineOnlyFraction(val float64) {
	m.Data.StreamerHeadOfLineOnlyFraction = val
}

func (m *SessionDataMutator) SetMultipleActivePortalsEnabled(val bool) {
	m.Data.MultipleActivePortalsEnabled = val
}

func (m *SessionDataMutator) SetUnboundedParallelScans(val bool) {
	m.Data.UnboundedParallelScans = val
}

func (m *SessionDataMutator) SetReplicationMode(val sessiondatapb.ReplicationMode) {
	m.Data.ReplicationMode = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedJoinElimination(val bool) {
	m.Data.OptimizerUseImprovedJoinElimination = val
}

func (m *SessionDataMutator) SetImplicitFKLockingForSerializable(val bool) {
	m.Data.ImplicitFKLockingForSerializable = val
}

func (m *SessionDataMutator) SetDurableLockingForSerializable(val bool) {
	m.Data.DurableLockingForSerializable = val
}

func (m *SessionDataMutator) SetSharedLockingForSerializable(val bool) {
	m.Data.SharedLockingForSerializable = val
}

func (m *SessionDataMutator) SetUnsafeSettingInterlockKey(val string) {
	m.Data.UnsafeSettingInterlockKey = val
}

func (m *SessionDataMutator) SetOptimizerUseLockOpForSerializable(val bool) {
	m.Data.OptimizerUseLockOpForSerializable = val
}

func (m *SessionDataMutator) SetOptimizerUseProvidedOrderingFix(val bool) {
	m.Data.OptimizerUseProvidedOrderingFix = val
}

func (m *SessionDataMutator) SetDisableChangefeedReplication(val bool) {
	m.Data.DisableChangefeedReplication = val
}

func (m *SessionDataMutator) SetDistSQLPlanGatewayBias(val int64) {
	m.Data.DistsqlPlanGatewayBias = val
}

func (m *SessionDataMutator) SetCloseCursorsAtCommit(val bool) {
	m.Data.CloseCursorsAtCommit = val
}

func (m *SessionDataMutator) SetPLpgSQLUseStrictInto(val bool) {
	m.Data.PLpgSQLUseStrictInto = val
}

func (m *SessionDataMutator) SetOptimizerUseVirtualComputedColumnStats(val bool) {
	m.Data.OptimizerUseVirtualComputedColumnStats = val
}

func (m *SessionDataMutator) SetOptimizerUseTrigramSimilarityOptimization(val bool) {
	m.Data.OptimizerUseTrigramSimilarityOptimization = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedDistinctOnLimitHintCosting(val bool) {
	m.Data.OptimizerUseImprovedDistinctOnLimitHintCosting = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedTrigramSimilaritySelectivity(val bool) {
	m.Data.OptimizerUseImprovedTrigramSimilaritySelectivity = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedZigzagJoinCosting(val bool) {
	m.Data.OptimizerUseImprovedZigzagJoinCosting = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedMultiColumnSelectivityEstimate(val bool) {
	m.Data.OptimizerUseImprovedMultiColumnSelectivityEstimate = val
}

func (m *SessionDataMutator) SetOptimizerProveImplicationWithVirtualComputedColumns(val bool) {
	m.Data.OptimizerProveImplicationWithVirtualComputedColumns = val
}

func (m *SessionDataMutator) SetOptimizerPushOffsetIntoIndexJoin(val bool) {
	m.Data.OptimizerPushOffsetIntoIndexJoin = val
}

func (m *SessionDataMutator) SetPlanCacheMode(val sessiondatapb.PlanCacheMode) {
	m.Data.PlanCacheMode = val
}

func (m *SessionDataMutator) SetOptimizerUsePolymorphicParameterFix(val bool) {
	m.Data.OptimizerUsePolymorphicParameterFix = val
}

func (m *SessionDataMutator) SetOptimizerUseConditionalHoistFix(val bool) {
	m.Data.OptimizerUseConditionalHoistFix = val
}

func (m *SessionDataMutator) SetOptimizerPushLimitIntoProjectFilteredScan(val bool) {
	m.Data.OptimizerPushLimitIntoProjectFilteredScan = val
}

func (m *SessionDataMutator) SetBypassPCRReaderCatalogAOST(val bool) {
	m.Data.BypassPCRReaderCatalogAOST = val
}

func (m *SessionDataMutator) SetUnsafeAllowTriggersModifyingCascades(val bool) {
	m.Data.UnsafeAllowTriggersModifyingCascades = val
}

func (m *SessionDataMutator) SetRecursionDepthLimit(val int) {
	m.Data.RecursionDepthLimit = int64(val)
}

func (m *SessionDataMutator) SetLegacyVarcharTyping(val bool) {
	m.Data.LegacyVarcharTyping = val
}

func (m *SessionDataMutator) SetCatalogDigestStalenessCheckEnabled(b bool) {
	m.Data.CatalogDigestStalenessCheckEnabled = b
}

func (m *SessionDataMutator) SetOptimizerPreferBoundedCardinality(b bool) {
	m.Data.OptimizerPreferBoundedCardinality = b
}

func (m *SessionDataMutator) SetOptimizerMinRowCount(val float64) {
	m.Data.OptimizerMinRowCount = val
}

func (m *SessionDataMutator) SetBufferedWritesEnabled(b bool) {
	m.Data.BufferedWritesEnabled = b
	if m.SessionDataMutatorCallbacks.SetBufferedWritesEnabled != nil {
		m.SessionDataMutatorCallbacks.SetBufferedWritesEnabled(b)
	}
}

func (m *SessionDataMutator) SetOptimizerCheckInputMinRowCount(val float64) {
	m.Data.OptimizerCheckInputMinRowCount = val
}

func (m *SessionDataMutator) SetOptimizerPlanLookupJoinsWithReverseScans(val bool) {
	m.Data.OptimizerPlanLookupJoinsWithReverseScans = val
}

func (m *SessionDataMutator) SetRegisterLatchWaitContentionEvents(val bool) {
	m.Data.RegisterLatchWaitContentionEvents = val
}

func (m *SessionDataMutator) SetUseCPutsOnNonUniqueIndexes(val bool) {
	m.Data.UseCPutsOnNonUniqueIndexes = val
}

func (m *SessionDataMutator) SetBufferedWritesUseLockingOnNonUniqueIndexes(val bool) {
	m.Data.BufferedWritesUseLockingOnNonUniqueIndexes = val
}

func (m *SessionDataMutator) SetOptimizerUseLockElisionMultipleFamilies(val bool) {
	m.Data.OptimizerUseLockElisionMultipleFamilies = val
}

func (m *SessionDataMutator) SetOptimizerEnableLockElision(val bool) {
	m.Data.OptimizerEnableLockElision = val
}

func (m *SessionDataMutator) SetOptimizerUseDeleteRangeFastPath(val bool) {
	m.Data.OptimizerUseDeleteRangeFastPath = val
}

func (m *SessionDataMutator) SetCreateTableWithSchemaLocked(val bool) {
	m.Data.CreateTableWithSchemaLocked = val
}

func (m *SessionDataMutator) SetUsePre_25_2VariadicBuiltins(val bool) {
	m.Data.UsePre_25_2VariadicBuiltins = val
}

func (m *SessionDataMutator) SetVectorSearchBeamSize(val int32) {
	m.Data.VectorSearchBeamSize = val
}

func (m *SessionDataMutator) SetVectorSearchRerankMultiplier(val int32) {
	m.Data.VectorSearchRerankMultiplier = val
}

func (m *SessionDataMutator) SetPropagateAdmissionHeaderToLeafTransactions(val bool) {
	m.Data.PropagateAdmissionHeaderToLeafTransactions = val
}

func (m *SessionDataMutator) SetOptimizerUseExistsFilterHoistRule(val bool) {
	m.Data.OptimizerUseExistsFilterHoistRule = val
}

func (m *SessionDataMutator) SetInitialRetryBackoffForReadCommitted(val time.Duration) {
	m.Data.InitialRetryBackoffForReadCommitted = val
}

func (m *SessionDataMutator) SetUseImprovedRoutineDependencyTracking(val bool) {
	m.Data.UseImprovedRoutineDependencyTracking = val
}

func (m *SessionDataMutator) SetOptimizerDisableCrossRegionCascadeFastPathForRBRTables(val bool) {
	m.Data.OptimizerDisableCrossRegionCascadeFastPathForRBRTables = val
}

func (m *SessionDataMutator) SetDistSQLUseReducedLeafWriteSets(val bool) {
	m.Data.DistSQLUseReducedLeafWriteSets = val
}

func (m *SessionDataMutator) SetUseProcTxnControlExtendedProtocolFix(val bool) {
	m.Data.UseProcTxnControlExtendedProtocolFix = val
}

func (m *SessionDataMutator) SetAllowUnsafeInternals(val bool) {
	m.Data.AllowUnsafeInternals = val
}

func (m *SessionDataMutator) SetOptimizerUseImprovedHoistJoinProject(val bool) {
	m.Data.OptimizerUseImprovedHoistJoinProject = val
}

func (m *SessionDataMutator) SetOptimizerClampLowHistogramSelectivity(val bool) {
	m.Data.OptimizerClampLowHistogramSelectivity = val
}

func (m *SessionDataMutator) SetOptimizerClampInequalitySelectivity(val bool) {
	m.Data.OptimizerClampInequalitySelectivity = val
}

func (m *SessionDataMutator) SetOptimizerUseMaxFrequencySelectivity(val bool) {
	m.Data.OptimizerUseMaxFrequencySelectivity = val
}

func (m *SessionDataMutator) SetDisableWaitForJobsNotice(val bool) {
	m.Data.DisableWaitForJobsNotice = val
}

func (m *SessionDataMutator) SetCanaryStatsMode(val sessiondatapb.CanaryStatsMode) {
	m.Data.CanaryStatsMode = val
}

func (m *SessionDataMutator) SetUseSwapMutations(val bool) {
	m.Data.UseSwapMutations = val
}

func (m *SessionDataMutator) SetPreventUpdateSetColumnDrop(val bool) {
	m.Data.PreventUpdateSetColumnDrop = val
}

func (m *SessionDataMutator) SetUseImprovedRoutineDepsTriggersAndComputedCols(val bool) {
	m.Data.UseImprovedRoutineDepsTriggersAndComputedCols = val
}

func (m *SessionDataMutator) SetDistSQLPreventPartitioningSoftLimitedScans(val bool) {
	m.Data.DistSQLPreventPartitioningSoftLimitedScans = val
}

func (m *SessionDataMutator) SetOptimizerInlineAnyUnnestSubquery(val bool) {
	m.Data.OptimizerInlineAnyUnnestSubquery = val
}

func (m *SessionDataMutator) SetUseBackupsWithIDs(val bool) {
	m.Data.UseBackupsWithIDs = val
}

func (m *SessionDataMutator) SetTcpKeepalivesIdle(val int32) {
	m.Data.TcpKeepalivesIdle = val
	m.notifyTCPKeepAliveChange()
}

func (m *SessionDataMutator) SetTcpKeepalivesInterval(val int32) {
	m.Data.TcpKeepalivesInterval = val
	m.notifyTCPKeepAliveChange()
}

func (m *SessionDataMutator) SetTcpKeepalivesCount(val int32) {
	m.Data.TcpKeepalivesCount = val
	m.notifyTCPKeepAliveChange()
}

func (m *SessionDataMutator) SetTcpUserTimeout(val int32) {
	m.Data.TcpUserTimeout = val
	m.notifyTCPKeepAliveChange()
}

func (m *SessionDataMutator) notifyTCPKeepAliveChange() {
	if m.OnTCPKeepAliveSettingChange != nil {
		m.OnTCPKeepAliveSettingChange(
			time.Duration(m.Data.TcpKeepalivesIdle)*time.Second,
			time.Duration(m.Data.TcpKeepalivesInterval)*time.Second,
			int(m.Data.TcpKeepalivesCount),
			time.Duration(m.Data.TcpUserTimeout)*time.Millisecond,
		)
	}
}

func (m *SessionDataMutator) SetOptimizerUseMinRowCountAntiJoinFix(val bool) {
	m.Data.OptimizerUseMinRowCountAntiJoinFix = val
}
