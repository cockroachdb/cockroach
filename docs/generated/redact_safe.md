The following types are considered always safe for reporting:

File | Type
--|--
pkg/cli/exit/exit.go | `Code`
pkg/jobs/jobspb/wrap.go | `Type`
pkg/kv/kvserver/closedts/ctpb/entry.go | `LAI`
pkg/kv/kvserver/concurrency/lock/locking.go | `WaitPolicy`
pkg/kv/kvserver/raft.go | `SnapshotRequest_Type`
pkg/roachpb/data.go | `LeaseSequence`
pkg/roachpb/data.go | `ReplicaChangeType`
pkg/roachpb/metadata.go | `NodeID`
pkg/roachpb/metadata.go | `StoreID`
pkg/roachpb/metadata.go | `RangeID`
pkg/roachpb/metadata.go | `ReplicaID`
pkg/roachpb/metadata.go | `RangeGeneration`
pkg/roachpb/metadata.go | `ReplicaType`
pkg/roachpb/method.go | `Method`
pkg/sql/catalog/descpb/structured.go | `ID`
pkg/sql/catalog/descpb/structured.go | `FamilyID`
pkg/sql/catalog/descpb/structured.go | `IndexID`
pkg/sql/catalog/descpb/structured.go | `DescriptorVersion`
pkg/sql/catalog/descpb/structured.go | `IndexDescriptorVersion`
pkg/sql/catalog/descpb/structured.go | `ColumnID`
pkg/sql/catalog/descpb/structured.go | `MutationID`
pkg/sql/catalog/descpb/structured.go | `ConstraintValidity`
pkg/sql/catalog/descpb/structured.go | `DescriptorMutation_Direction`
pkg/sql/catalog/descpb/structured.go | `DescriptorMutation_State`
pkg/sql/catalog/descpb/structured.go | `DescriptorState`
pkg/sql/catalog/descpb/structured.go | `ConstraintType`
pkg/sql/sem/tree/table_ref.go | `ID`
pkg/sql/sem/tree/table_ref.go | `ColumnID`
pkg/storage/enginepb/mvcc3.go | `MVCCStatsDelta`
pkg/storage/enginepb/mvcc3.go | `*MVCCStats`
pkg/util/hlc/timestamp.go | `Timestamp`
pkg/util/hlc/timestamp.go | `ClockTimestamp`
vendor/github.com/cockroachdb/pebble/internal/humanize/humanize.go | `FormattedString`
docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:38:docs/generated/redact_safe.md:57:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:39:docs/generated/redact_safe.md:39:docs/generated/redact_safe.md:39:docs/generated/redact_safe.md:39:docs/generated/redact_safe.md:39:docs/generated/redact_safe.md:58:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:40:docs/generated/redact_safe.md:40:docs/generated/redact_safe.md:40:docs/generated/redact_safe.md:40:docs/generated/redact_safe.md:59:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:41:docs/generated/redact_safe.md:41:docs/generated/redact_safe.md:41:docs/generated/redact_safe.md:60:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:42:docs/generated/redact_safe.md:42:docs/generated/redact_safe.md:61:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:43:docs/generated/redact_safe.md:62:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
docs/generated/redact_safe.md:63:vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
pkg/util/log/redact.go | `reflect.TypeOf(true)`
pkg/util/log/redact.go | `reflect.TypeOf(123)`
pkg/util/log/redact.go | `reflect.TypeOf(int8(0))`
pkg/util/log/redact.go | `reflect.TypeOf(int16(0))`
pkg/util/log/redact.go | `reflect.TypeOf(int32(0))`
pkg/util/log/redact.go | `reflect.TypeOf(int64(0))`
pkg/util/log/redact.go | `reflect.TypeOf(uint8(0))`
pkg/util/log/redact.go | `reflect.TypeOf(uint16(0))`
pkg/util/log/redact.go | `reflect.TypeOf(uint32(0))`
pkg/util/log/redact.go | `reflect.TypeOf(uint64(0))`
pkg/util/log/redact.go | `reflect.TypeOf(float32(0))`
pkg/util/log/redact.go | `reflect.TypeOf(float64(0))`
pkg/util/log/redact.go | `reflect.TypeOf(complex64(0))`
pkg/util/log/redact.go | `reflect.TypeOf(complex128(0))`
pkg/util/log/redact.go | `reflect.TypeOf(os.Interrupt)`
pkg/util/log/redact.go | `reflect.TypeOf(time.Time{})`
pkg/util/log/redact.go | `reflect.TypeOf(time.Duration(0))`
pkg/util/log/redact.go | `reflect.TypeOf(encodingtype.T(0))`
pkg/util/log/redact.go | `reflect.TypeOf(Channel(0))`
vendor/github.com/cockroachdb/pebble/metrics.go:324:	// have been registered as safe with redact.RegisterSafeType and does not
