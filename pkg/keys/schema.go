// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

type (
	// atom is a byte sequence which identifies a schema subtree in the context of
	// its parent schema node. All encoded key suffixes covered by a schema
	// subtree start with this byte sequence.
	atom = string
	// atomType is the value type associated with an atom. Most atoms have no
	// value (atomValueNone). Some keys, like RangeID-local ones, have a dynamic
	// value (such as the integer for RangeID), in which case the atomType for the
	// corresponding atom specifies how this value is encoded in the key.
	atomType int
	// valueType specifies the type of the value under a key. If a schema node is
	// "internal" and has no associated values, the valueType is valueNone. The
	// "terminal" schema nodes which can have materialized key/values in storage,
	// have valueType = valueCustom.
	//
	// TODO(pav-kv): make valueCustom more granular and customizable. These types
	// can be used as hints on how to pretty-print the values.
	valueType int
)

const (
	atomValueNone atomType = iota
	atomValueUint64
	atomValueVarInt
	atomValueVarUInt
)

const (
	valueNone valueType = iota
	valueCustom
)

// String converts the atomType to a string.
func (a atomType) String() string {
	switch a {
	case atomValueNone:
		return "none"
	case atomValueVarInt:
		return "VarInt"
	case atomValueVarUInt:
		return "VarUInt"
	case atomValueUint64:
		return "UInt64"
	default:
		return "unknown"
	}
}

// String converts the valueType to a string.
func (v valueType) String() string {
	switch v {
	case valueNone:
		return "none"
	case valueCustom:
		return "custom"
	default:
		return "unknown"
	}
}

type schemaNode struct {
	atom     atom
	atomType atomType
	valType  valueType
	anno     string
	children []schemaNode
}

type schemaNodeBuilder struct {
	atom atom
	typ  atomType
	anno string
}

func tag(atom []byte, annotation string) schemaNodeBuilder {
	return schemaNodeBuilder{atom: string(atom), anno: annotation}
}

func vTag(atom []byte, typ atomType, annotation string) schemaNodeBuilder {
	return schemaNodeBuilder{atom: string(atom), typ: typ, anno: annotation}
}

func (t schemaNodeBuilder) sub(nodes ...schemaNode) schemaNode {
	return schemaNode{atom: t.atom, atomType: t.typ, anno: t.anno, children: nodes}
}

func (t schemaNodeBuilder) val() schemaNode {
	node := t.sub()
	node.valType = valueCustom
	return node
}

func schema() *schemaNode {
	root := tag(nil, "/").sub(
		// Local keys.
		tag(LocalPrefix, "Local").sub(
			// RangeID-local keys.
			vTag(LocalRangeIDPrefix[1:], atomValueVarInt, "RangeID").sub(
				// Replicated RangeID-local keys.
				tag(LocalRangeIDReplicatedInfix, "Replicated").sub(
					tag(LocalAbortSpanSuffix, "AbortSpan").val(),
					tag(LocalReplicatedSharedLocksTransactionLatchingKeySuffix, "SharedLocks").val(),
					tag(localRangeFrozenStatusSuffix, "FrozenStatus").val(),
					tag(LocalRangeGCThresholdSuffix, "GCThreshold").val(),
					tag(LocalRangeAppliedStateSuffix, "AppliedState").val(),
					tag(LocalRangeForceFlushSuffix, "ForceFlush").val(),
					tag(LocalRangeGCHintSuffix, "GCHint").val(),
					tag(LocalRangeLeaseSuffix, "Lease").val(),
					tag(LocalRangePriorReadSummarySuffix, "PriorReadSummary").val(),
					tag(LocalRangeVersionSuffix, "RangeVersion").val(),
					tag(LocalRangeStatsLegacySuffix, "Stats").val(),                // deprecated
					tag(localTxnSpanGCThresholdSuffix, "TxnSpanGCThreshold").val(), // deprecated
				),
				// Unreplicated RangeID-local keys.
				tag(localRangeIDUnreplicatedInfix, "Unreplicated").sub(
					tag(LocalRangeTombstoneSuffix, "RangeTombstone").val(),
					tag(LocalRaftHardStateSuffix, "RaftHardState").val(),
					tag(localRaftLastIndexSuffix, "RaftLastIndex").val(), // deprecated
					vTag(LocalRaftLogSuffix, atomValueUint64, "RaftLog").val(),
					tag(LocalRaftReplicaIDSuffix, "RaftReplicaID").val(),
					tag(LocalRaftTruncatedStateSuffix, "RaftTruncatedState").val(),
				),
			),

			// Range-local keys.
			tag(LocalRangePrefix[1:], "Range").sub(
				tag(LocalRangeProbeSuffix, "Probe").val(),
				tag(LocalQueueLastProcessedSuffix, "QueueLastProcessing").val(),
				tag(LocalRangeDescriptorSuffix, "RangeDescriptor").val(),
				tag(LocalTransactionSuffix, "Transaction").val(),
			),

			// Store-local keys.
			// NB: LocalStorePrefix contains LocalPrefix, so [1:] strips it down.
			// TODO(pav-kv): reorg the constants so that this and similar tricks in
			// this file are not needed.
			tag(LocalStorePrefix[1:], "Store").sub(
				tag(localStoreClusterVersionSuffix, "ClusterVersion").val(),
				tag(localStoreGossipSuffix, "Gossip").val(),
				tag(localStoreHLCUpperBoundSuffix, "HLCUpperBound").val(),
				tag(localStoreIdentSuffix, "StoreIdent").val(),

				tag(localStoreLossOfQuorumRecoveryInfix, "LossOfQuorumRecovery").sub(
					// TODO(pav-kv): support LocalStoreUnsafeReplicaRecoveryKeyMin/Max
					tag(localStoreUnsafeReplicaRecoverySuffix[4:], "Applied").val(),
					tag(localStoreLossOfQuorumRecoveryStatusSuffix[4:], "Status").val(),
					tag(localStoreLossOfQuorumRecoveryCleanupActionsSuffix[4:], "CleanupActions").val(),
				),

				tag(localStoreNodeTombstoneSuffix, "NodeTombstone").val(),
				// TODO(pav-kv): support LocalStoreCachedSettingsKeyMin/Max
				tag(localStoreCachedSettingsSuffix, "CachedSettings").val(),

				tag(localStoreLastUpSuffix, "LastUp").val(),
				tag(localStoreLivenessRequesterMeta, "StoreLivenessRequesterMeta").val(),
				tag(localStoreLivenessSupporterMeta, "StoreLivenessSupporterMeta").val(),
				tag(localStoreLivenessSupportFor, "StoreLivenessSupportFor").val(),
				tag(localRemovedLeakedRaftEntriesSuffix, "LeakedRaftEntries").val(), // deprecated
			),

			tag(LocalRangeLockTablePrefix[1:], "LockTable").sub(
				// TODO(pav-kv): support LockTableSingleKeyStart/End
				tag(LockTableSingleKeyInfix, "SingleKey").val(),
			),
		),

		// TODO(pav-kv): support Meta1KeyMax
		tag(Meta1Prefix, "Meta1").sub(),
		// TODO(pav-kv): support Meta2KeyMax
		tag(Meta2Prefix, "Meta2").sub(),

		// TODO(pav-kv): support SystemMax
		tag(SystemPrefix, "System").sub(
			// TODO(pav-kv): support NodeLivenessKeyMax
			tag(NodeLivenessPrefix[1:], "NodeLiveness").val(),
			tag(BootstrapVersionKey[1:], "BootstrapVersion").val(),
			tag(ClusterInitGracePeriodTimestamp[1:], "ClusterInitGracePeriodTimestamp").sub(),
			tag(TrialLicenseExpiry[1:], "TrialLicenseExpiry").val(),
			tag(NodeIDGenerator[1:], "NodeIDGenerator").val(),
			tag(RangeIDGenerator[1:], "RangeIDGenerator").val(),
			tag(StoreIDGenerator[1:], "StoreIDGenerator").val(),
			tag(StatusPrefix[1:], "Status").val(),
			tag(StatusNodePrefix[1:], "StatusNode").val(),
			tag(StartupMigrationPrefix[1:], "StartupMigration").val(),
			// TODO(pav-kv): support TimeseriesKeyMax
			tag(TimeseriesPrefix[1:], "Timeseries").val(),
			// TODO(pav-kv): support SystemSpanConfigKeyMax
			tag(SystemSpanConfigPrefix[1:], "SpanConfig").sub(
				tag(SystemSpanConfigEntireKeyspace[10:], "EntireKeyspace").val(),
				tag(SystemSpanConfigHostOnTenantKeyspace[10:], "HostOnTenant").val(),
				tag(SystemSpanConfigSecondaryTenantOnEntireKeyspace[10:],
					"SecondaryTenantOnEntireKeyspace").val(),
			),
		),

		// TODO(pav-kv): support TableDataMax
		tag(TableDataMin, "TableData").sub(),

		// TODO(pav-kv): support TenantTableDataMin/Max
		vTag(TenantPrefix, atomValueVarUInt, "Tenant").sub(),
	)
	return &root
}
