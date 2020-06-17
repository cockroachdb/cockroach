The following types are considered always safe for reporting:

File | Type
--|--
pkg/kv/kvserver/raft.go:298:func (SnapshotRequest_Type) SafeValue() {}
pkg/roachpb/data.go:2281:func (ReplicaChangeType) SafeValue() {}
pkg/roachpb/metadata.go:36:func (n NodeID) SafeValue() {}
pkg/roachpb/metadata.go:55:func (n StoreID) SafeValue() {}
pkg/roachpb/metadata.go:66:func (r RangeID) SafeValue() {}
pkg/roachpb/metadata.go:84:func (r ReplicaID) SafeValue() {}
pkg/roachpb/metadata.go:375:func (r ReplicaType) SafeValue() {}
pkg/util/hlc/timestamp.go:96:func (Timestamp) SafeValue() {}
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
