The following types are considered always safe for reporting:

File | Type
--|--
pkg/kv/kvserver/raft.go | `SnapshotRequest_Type`
pkg/roachpb/data.go | `ReplicaChangeType`
pkg/roachpb/metadata.go | `NodeID`
pkg/roachpb/metadata.go | `StoreID`
pkg/roachpb/metadata.go | `RangeID`
pkg/roachpb/metadata.go | `ReplicaID`
pkg/roachpb/metadata.go | `ReplicaType`
pkg/util/hlc/timestamp.go | `Timestamp`
vendor/github.com/cockroachdb/redact/api.go | `SafeString`
vendor/github.com/cockroachdb/redact/api.go | `SafeRune`
vendor/github.com/cockroachdb/redact/wrappers.go:40:func Safe(a interface{}) SafeValue {
vendor/github.com/cockroachdb/redact/wrappers.go | `safeWrapper`
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
