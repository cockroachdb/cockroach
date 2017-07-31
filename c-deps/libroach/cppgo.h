#ifndef CPPGO_H
#define CPPGO_H

#include <stdint.h>

template <typename T>
struct GoType {
  enum { Size = T::Size };
  typedef T Result;
  static T get(const void *data, int offset) {
    return T(reinterpret_cast<const uint8_t*>(data) + offset);
  }
};

template <>
struct GoType<bool> {
  enum { Size = sizeof(bool) };
  typedef bool Result;
  static bool get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const bool*>(ptr);
  }
};

template <>
struct GoType<int8_t> {
  enum { Size = sizeof(int8_t) };
  typedef int8_t Result;
  static int8_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const int8_t*>(ptr);
  }
};

template <>
struct GoType<int16_t> {
  enum { Size = sizeof(int16_t) };
  typedef int16_t Result;
  static int16_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const int16_t*>(ptr);
  }
};

template <>
struct GoType<int32_t> {
  enum { Size = sizeof(int32_t) };
  typedef int32_t Result;
  static int32_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const int32_t*>(ptr);
  }
};

template <>
struct GoType<int64_t> {
  enum { Size = sizeof(int64_t) };
  typedef int64_t Result;
  static int64_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const int64_t*>(ptr);
  }
};

template <>
struct GoType<uint8_t> {
  enum { Size = sizeof(uint8_t) };
  typedef uint8_t Result;
  static uint8_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const uint8_t*>(ptr);
  }
};

template <>
struct GoType<uint16_t> {
  enum { Size = sizeof(uint16_t) };
  typedef uint16_t Result;
  static uint16_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const uint16_t*>(ptr);
  }
};

template <>
struct GoType<uint32_t> {
  enum { Size = sizeof(uint32_t) };
  typedef uint32_t Result;
  static uint32_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const uint32_t*>(ptr);
  }
};

template <>
struct GoType<uint64_t> {
  enum { Size = sizeof(uint64_t) };
  typedef uint64_t Result;
  static uint64_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const uint64_t*>(ptr);
  }
};

template <>
struct GoType<uintptr_t> {
  enum { Size = sizeof(uintptr_t) };
  typedef uintptr_t Result;
  static uintptr_t get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const uintptr_t*>(ptr);
  }
};

template <>
struct GoType<float> {
  enum { Size = sizeof(float) };
  typedef float Result;
  static float get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const float*>(ptr);
  }
};

template <>
struct GoType<double> {
  enum { Size = sizeof(double) };
  typedef double Result;
  static double get(const void *data, int offset) {
    const uint8_t *ptr = reinterpret_cast<const uint8_t*>(data) + offset;
    return *reinterpret_cast<const double*>(ptr);
  }
};

class GoString {
 struct Header {
   const char *data;
   int64_t size;
 };

 public:
   enum { Size = sizeof(Header) };

 public:
  GoString(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  std::string as_string() const { return std::string(data(), size()); }
  const char* data() const { return hdr_->data; }
  int64_t size() const { return hdr_->size; }

 private:
  const Header* hdr_;
};

template <typename T>
class GoPointer {
 public:
  enum { Size = sizeof(void*) };

 public:
  GoPointer(const void *data)
    : data_(data) {
  }

  const void* get() const {
    return *reinterpret_cast<const uint8_t* const*>(data_);
  }
  operator bool() const {
    return get() != nullptr;
  }
  T operator*() const {
    return GoType<T>::get(get(), 0);
  }
  T operator->() const {
    return GoType<T>::get(get(), 0);
  }

 private:
  const void* data_;
};

template <typename C>
class GoIterator {
 public:
  typedef typename C::value_type value_type;
  typedef typename C::reference_type reference_type;
  typedef GoIterator<C> self_type;

 public:
  GoIterator(const C *c, int index)
    : container_(c),
      index_(index) {
  }
  GoIterator(const self_type& x)
    : container_(x.container_),
      index_(x.index_) {
  }

  value_type operator*() const {
    return (*container_)[index_];
  }

  self_type operator++(int) {
    self_type t = *this;
    return ++t;
  }
  self_type& operator++() {
    ++index_;
    return *this;
  }
  self_type operator--(int) {
    self_type t = *this;
    return --*t;
  }
  self_type& operator--() {
    --index_;
    return *this;
  }

  bool operator==(const self_type& other) const {
    return container_ == other.container_ && index_ == other.index_;
  }
  bool operator!=(const self_type& other) const {
    return !(*this == other);
  }

 private:
  const C *container_;
  int index_;
};

template <typename T>
class GoSlice {
  struct Header {
    const uint8_t *data;
    int64_t size;
    int64_t cap;
  };

 public:
  enum { Size = sizeof(Header) };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef GoIterator<GoSlice<T> > iterator;
  typedef iterator const_iterator;

 public:
  GoSlice(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  value_type operator[](int i) const {
    return GoType<T>::get(hdr_->data, i * GoType<T>::Size);
  }

  int64_t size() const { return hdr_->size; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }  

 private:
  const Header* hdr_;
};

template <typename T, int N>
class GoArray {
 public:
  enum { Size = N * GoType<T>::Size };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef GoIterator<GoArray<T, N> > iterator;
  typedef iterator const_iterator;

 public:
  GoArray(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  value_type operator[](int i) const {
    return GoType<T>::get(data_, i * GoType<T>::Size);
  }

  int64_t size() const { return N; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }  

 private:
  const uint8_t *data_;
};

namespace roachpb { class BatchRequest; }

namespace roachpb { class Header; }

namespace hlc { class Timestamp; }

namespace hlc {
class Timestamp {
 public:
  enum { Size = 16 };

 public:
  Timestamp(const void *data)
    : data_(data) {
  }

  int64_t wall_time() const;
  int32_t logical() const;

 private:
  const void *data_;
};
}  // namespace hlc

namespace roachpb { class ReplicaDescriptor; }

namespace roachpb {
class ReplicaDescriptor {
 public:
  enum { Size = 12 };

 public:
  ReplicaDescriptor(const void *data)
    : data_(data) {
  }

  int32_t node_id() const;
  int32_t store_id() const;
  int32_t replica_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class Transaction; }

namespace enginepb { class TxnMeta; }

namespace uuid { class UUID; }

namespace uuid {
class UUID {
 public:
  enum { Size = 16 };

 public:
  UUID(const void *data)
    : data_(data) {
  }

  GoArray<uint8_t,16> uuid() const;

 private:
  const void *data_;
};
}  // namespace uuid

namespace enginepb {
class TxnMeta {
 public:
  enum { Size = 80 };

 public:
  TxnMeta(const void *data)
    : data_(data) {
  }

  GoPointer<uuid::UUID > id() const;
  int32_t isolation() const;
  GoSlice<uint8_t> key() const;
  uint32_t epoch() const;
  hlc::Timestamp timestamp() const;
  int32_t priority() const;
  int32_t sequence() const;
  int32_t batch_index() const;

 private:
  const void *data_;
};
}  // namespace enginepb

namespace roachpb { class ObservedTimestamp; }

namespace roachpb {
class ObservedTimestamp {
 public:
  enum { Size = 24 };

 public:
  ObservedTimestamp(const void *data)
    : data_(data) {
  }

  int32_t node_id() const;
  hlc::Timestamp timestamp() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class Span; }

namespace roachpb {
class Span {
 public:
  enum { Size = 48 };

 public:
  Span(const void *data)
    : data_(data) {
  }

  GoSlice<uint8_t> key() const;
  GoSlice<uint8_t> end_key() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class Transaction {
 public:
  enum { Size = 208 };

 public:
  Transaction(const void *data)
    : data_(data) {
  }

  enginepb::TxnMeta txn_meta() const;
  GoString name() const;
  int32_t status() const;
  hlc::Timestamp last_heartbeat() const;
  hlc::Timestamp orig_timestamp() const;
  hlc::Timestamp max_timestamp() const;
  GoSlice<roachpb::ObservedTimestamp> observed_timestamps() const;
  bool writing() const;
  bool write_too_old() const;
  bool retry_on_push() const;
  GoSlice<roachpb::Span> intents() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class Header {
 public:
  enum { Size = 80 };

 public:
  Header(const void *data)
    : data_(data) {
  }

  hlc::Timestamp timestamp() const;
  roachpb::ReplicaDescriptor replica() const;
  int64_t range_id() const;
  double user_priority() const;
  GoPointer<roachpb::Transaction > txn() const;
  int32_t read_consistency() const;
  int64_t max_span_request_keys() const;
  bool distinct_spans() const;
  bool return_range_info() const;
  int32_t gateway_node_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RequestUnion; }

namespace roachpb { class GetRequest; }

namespace roachpb {
class GetRequest {
 public:
  enum { Size = 48 };

 public:
  GetRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class PutRequest; }

namespace roachpb { class Value; }

namespace roachpb {
class Value {
 public:
  enum { Size = 40 };

 public:
  Value(const void *data)
    : data_(data) {
  }

  GoSlice<uint8_t> raw_bytes() const;
  hlc::Timestamp timestamp() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class PutRequest {
 public:
  enum { Size = 96 };

 public:
  PutRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Value value() const;
  bool inline_() const;
  bool blind() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ConditionalPutRequest; }

namespace roachpb {
class ConditionalPutRequest {
 public:
  enum { Size = 104 };

 public:
  ConditionalPutRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Value value() const;
  GoPointer<roachpb::Value > exp_value() const;
  bool blind() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class IncrementRequest; }

namespace roachpb {
class IncrementRequest {
 public:
  enum { Size = 56 };

 public:
  IncrementRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  int64_t increment() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeleteRequest; }

namespace roachpb {
class DeleteRequest {
 public:
  enum { Size = 48 };

 public:
  DeleteRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeleteRangeRequest; }

namespace roachpb {
class DeleteRangeRequest {
 public:
  enum { Size = 56 };

 public:
  DeleteRangeRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  bool return_keys() const;
  bool inline_() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ScanRequest; }

namespace roachpb {
class ScanRequest {
 public:
  enum { Size = 48 };

 public:
  ScanRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class BeginTransactionRequest; }

namespace roachpb {
class BeginTransactionRequest {
 public:
  enum { Size = 48 };

 public:
  BeginTransactionRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class EndTransactionRequest; }

namespace roachpb { class InternalCommitTrigger; }

namespace roachpb { class SplitTrigger; }

namespace roachpb { class RangeDescriptor; }

namespace roachpb {
class RangeDescriptor {
 public:
  enum { Size = 88 };

 public:
  RangeDescriptor(const void *data)
    : data_(data) {
  }

  int64_t range_id() const;
  GoSlice<uint8_t> start_key() const;
  GoSlice<uint8_t> end_key() const;
  GoSlice<roachpb::ReplicaDescriptor> replicas() const;
  int32_t next_replica_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class SplitTrigger {
 public:
  enum { Size = 176 };

 public:
  SplitTrigger(const void *data)
    : data_(data) {
  }

  roachpb::RangeDescriptor left_desc() const;
  roachpb::RangeDescriptor right_desc() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class MergeTrigger; }

namespace roachpb {
class MergeTrigger {
 public:
  enum { Size = 176 };

 public:
  MergeTrigger(const void *data)
    : data_(data) {
  }

  roachpb::RangeDescriptor left_desc() const;
  roachpb::RangeDescriptor right_desc() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ChangeReplicasTrigger; }

namespace roachpb {
class ChangeReplicasTrigger {
 public:
  enum { Size = 48 };

 public:
  ChangeReplicasTrigger(const void *data)
    : data_(data) {
  }

  int32_t change_type() const;
  roachpb::ReplicaDescriptor replica() const;
  GoSlice<roachpb::ReplicaDescriptor> updated_replicas() const;
  int32_t next_replica_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ModifiedSpanTrigger; }

namespace roachpb {
class ModifiedSpanTrigger {
 public:
  enum { Size = 16 };

 public:
  ModifiedSpanTrigger(const void *data)
    : data_(data) {
  }

  bool system_config_span() const;
  GoPointer<roachpb::Span > node_liveness_span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class InternalCommitTrigger {
 public:
  enum { Size = 32 };

 public:
  InternalCommitTrigger(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::SplitTrigger > split_trigger() const;
  GoPointer<roachpb::MergeTrigger > merge_trigger() const;
  GoPointer<roachpb::ChangeReplicasTrigger > change_replicas_trigger() const;
  GoPointer<roachpb::ModifiedSpanTrigger > modified_span_trigger() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class EndTransactionRequest {
 public:
  enum { Size = 104 };

 public:
  EndTransactionRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  bool commit() const;
  GoPointer<hlc::Timestamp > deadline() const;
  GoPointer<roachpb::InternalCommitTrigger > internal_commit_trigger() const;
  GoSlice<roachpb::Span> intent_spans() const;
  bool require1_pc() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminSplitRequest; }

namespace roachpb {
class AdminSplitRequest {
 public:
  enum { Size = 72 };

 public:
  AdminSplitRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  GoSlice<uint8_t> split_key() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminMergeRequest; }

namespace roachpb {
class AdminMergeRequest {
 public:
  enum { Size = 48 };

 public:
  AdminMergeRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminTransferLeaseRequest; }

namespace roachpb {
class AdminTransferLeaseRequest {
 public:
  enum { Size = 56 };

 public:
  AdminTransferLeaseRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  int32_t target() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminChangeReplicasRequest; }

namespace roachpb { class ReplicationTarget; }

namespace roachpb {
class ReplicationTarget {
 public:
  enum { Size = 8 };

 public:
  ReplicationTarget(const void *data)
    : data_(data) {
  }

  int32_t node_id() const;
  int32_t store_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class AdminChangeReplicasRequest {
 public:
  enum { Size = 80 };

 public:
  AdminChangeReplicasRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  int32_t change_type() const;
  GoSlice<roachpb::ReplicationTarget> targets() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class HeartbeatTxnRequest; }

namespace roachpb {
class HeartbeatTxnRequest {
 public:
  enum { Size = 64 };

 public:
  HeartbeatTxnRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  hlc::Timestamp now() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class GCRequest; }

namespace roachpb { class GCRequest_GCKey; }

namespace roachpb {
class GCRequest_GCKey {
 public:
  enum { Size = 40 };

 public:
  GCRequest_GCKey(const void *data)
    : data_(data) {
  }

  GoSlice<uint8_t> key() const;
  hlc::Timestamp timestamp() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class GCRequest {
 public:
  enum { Size = 104 };

 public:
  GCRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  GoSlice<roachpb::GCRequest_GCKey> keys() const;
  hlc::Timestamp threshold() const;
  hlc::Timestamp txn_span_gcthreshold() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class PushTxnRequest; }

namespace roachpb {
class PushTxnRequest {
 public:
  enum { Size = 376 };

 public:
  PushTxnRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Transaction pusher_txn() const;
  enginepb::TxnMeta pushee_txn() const;
  hlc::Timestamp push_to() const;
  hlc::Timestamp now() const;
  int32_t push_type() const;
  bool force() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RangeLookupRequest; }

namespace roachpb {
class RangeLookupRequest {
 public:
  enum { Size = 56 };

 public:
  RangeLookupRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  int32_t max_ranges() const;
  bool reverse() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ResolveIntentRequest; }

namespace roachpb {
class ResolveIntentRequest {
 public:
  enum { Size = 136 };

 public:
  ResolveIntentRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  enginepb::TxnMeta intent_txn() const;
  int32_t status() const;
  bool poison() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ResolveIntentRangeRequest; }

namespace roachpb {
class ResolveIntentRangeRequest {
 public:
  enum { Size = 136 };

 public:
  ResolveIntentRangeRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  enginepb::TxnMeta intent_txn() const;
  int32_t status() const;
  bool poison() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class MergeRequest; }

namespace roachpb {
class MergeRequest {
 public:
  enum { Size = 88 };

 public:
  MergeRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Value value() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TruncateLogRequest; }

namespace roachpb {
class TruncateLogRequest {
 public:
  enum { Size = 64 };

 public:
  TruncateLogRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  uint64_t index() const;
  int64_t range_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RequestLeaseRequest; }

namespace roachpb { class Lease; }

namespace roachpb {
class Lease {
 public:
  enum { Size = 80 };

 public:
  Lease(const void *data)
    : data_(data) {
  }

  hlc::Timestamp start() const;
  hlc::Timestamp expiration() const;
  roachpb::ReplicaDescriptor replica() const;
  hlc::Timestamp deprecated_start_stasis() const;
  GoPointer<hlc::Timestamp > proposed_ts() const;
  GoPointer<int64_t > epoch() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class RequestLeaseRequest {
 public:
  enum { Size = 208 };

 public:
  RequestLeaseRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Lease lease() const;
  roachpb::Lease prev_lease() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ReverseScanRequest; }

namespace roachpb {
class ReverseScanRequest {
 public:
  enum { Size = 48 };

 public:
  ReverseScanRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ComputeChecksumRequest; }

namespace roachpb {
class ComputeChecksumRequest {
 public:
  enum { Size = 72 };

 public:
  ComputeChecksumRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  uint32_t version() const;
  uuid::UUID checksum_id() const;
  bool snapshot() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeprecatedVerifyChecksumRequest; }

namespace roachpb {
class DeprecatedVerifyChecksumRequest {
 public:
  enum { Size = 48 };

 public:
  DeprecatedVerifyChecksumRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class CheckConsistencyRequest; }

namespace roachpb {
class CheckConsistencyRequest {
 public:
  enum { Size = 56 };

 public:
  CheckConsistencyRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  bool with_diff() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class NoopRequest; }

namespace roachpb {
class NoopRequest {
 public:
  enum { Size = 0 };

 public:
  NoopRequest(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class InitPutRequest; }

namespace roachpb {
class InitPutRequest {
 public:
  enum { Size = 96 };

 public:
  InitPutRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Value value() const;
  bool blind() const;
  bool fail_on_tombstones() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TransferLeaseRequest; }

namespace roachpb {
class TransferLeaseRequest {
 public:
  enum { Size = 208 };

 public:
  TransferLeaseRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Lease lease() const;
  roachpb::Lease prev_lease() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class LeaseInfoRequest; }

namespace roachpb {
class LeaseInfoRequest {
 public:
  enum { Size = 48 };

 public:
  LeaseInfoRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class WriteBatchRequest; }

namespace roachpb {
class WriteBatchRequest {
 public:
  enum { Size = 120 };

 public:
  WriteBatchRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::Span data_span() const;
  GoSlice<uint8_t> data() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportRequest; }

namespace roachpb { class ExportStorage; }

namespace roachpb { class ExportStorage_LocalFilePath; }

namespace roachpb {
class ExportStorage_LocalFilePath {
 public:
  enum { Size = 16 };

 public:
  ExportStorage_LocalFilePath(const void *data)
    : data_(data) {
  }

  GoString path() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportStorage_Http; }

namespace roachpb {
class ExportStorage_Http {
 public:
  enum { Size = 16 };

 public:
  ExportStorage_Http(const void *data)
    : data_(data) {
  }

  GoString base_uri() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportStorage_GCS; }

namespace roachpb {
class ExportStorage_GCS {
 public:
  enum { Size = 32 };

 public:
  ExportStorage_GCS(const void *data)
    : data_(data) {
  }

  GoString bucket() const;
  GoString prefix() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportStorage_S3; }

namespace roachpb {
class ExportStorage_S3 {
 public:
  enum { Size = 80 };

 public:
  ExportStorage_S3(const void *data)
    : data_(data) {
  }

  GoString bucket() const;
  GoString prefix() const;
  GoString access_key() const;
  GoString secret() const;
  GoString temp_token() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportStorage_Azure; }

namespace roachpb {
class ExportStorage_Azure {
 public:
  enum { Size = 64 };

 public:
  ExportStorage_Azure(const void *data)
    : data_(data) {
  }

  GoString container() const;
  GoString prefix() const;
  GoString account_name() const;
  GoString account_key() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ExportStorage {
 public:
  enum { Size = 64 };

 public:
  ExportStorage(const void *data)
    : data_(data) {
  }

  int32_t provider() const;
  roachpb::ExportStorage_LocalFilePath local_file() const;
  roachpb::ExportStorage_Http http_path() const;
  GoPointer<roachpb::ExportStorage_GCS > google_cloud_config() const;
  GoPointer<roachpb::ExportStorage_S3 > s3_config() const;
  GoPointer<roachpb::ExportStorage_Azure > azure_config() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ExportRequest {
 public:
  enum { Size = 128 };

 public:
  ExportRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  roachpb::ExportStorage storage() const;
  hlc::Timestamp start_time() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ImportRequest; }

namespace roachpb { class ImportRequest_File; }

namespace roachpb {
class ImportRequest_File {
 public:
  enum { Size = 104 };

 public:
  ImportRequest_File(const void *data)
    : data_(data) {
  }

  roachpb::ExportStorage dir() const;
  GoString path() const;
  GoSlice<uint8_t> sha512() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ImportRequest_TableRekey; }

namespace roachpb {
class ImportRequest_TableRekey {
 public:
  enum { Size = 32 };

 public:
  ImportRequest_TableRekey(const void *data)
    : data_(data) {
  }

  uint32_t old_id() const;
  GoSlice<uint8_t> new_desc() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ImportRequest {
 public:
  enum { Size = 144 };

 public:
  ImportRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  GoSlice<roachpb::ImportRequest_File> files() const;
  roachpb::Span data_span() const;
  GoSlice<roachpb::ImportRequest_TableRekey> rekeys() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class QueryTxnRequest; }

namespace roachpb {
class QueryTxnRequest {
 public:
  enum { Size = 160 };

 public:
  QueryTxnRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  enginepb::TxnMeta txn() const;
  bool wait_for_update() const;
  GoSlice<uuid::UUID> known_waiting_txns() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminScatterRequest; }

namespace roachpb {
class AdminScatterRequest {
 public:
  enum { Size = 48 };

 public:
  AdminScatterRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AddSSTableRequest; }

namespace roachpb {
class AddSSTableRequest {
 public:
  enum { Size = 72 };

 public:
  AddSSTableRequest(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  GoSlice<uint8_t> data() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class RequestUnion {
 public:
  enum { Size = 288 };

 public:
  RequestUnion(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::GetRequest > get() const;
  GoPointer<roachpb::PutRequest > put() const;
  GoPointer<roachpb::ConditionalPutRequest > conditional_put() const;
  GoPointer<roachpb::IncrementRequest > increment() const;
  GoPointer<roachpb::DeleteRequest > delete_() const;
  GoPointer<roachpb::DeleteRangeRequest > delete_range() const;
  GoPointer<roachpb::ScanRequest > scan() const;
  GoPointer<roachpb::BeginTransactionRequest > begin_transaction() const;
  GoPointer<roachpb::EndTransactionRequest > end_transaction() const;
  GoPointer<roachpb::AdminSplitRequest > admin_split() const;
  GoPointer<roachpb::AdminMergeRequest > admin_merge() const;
  GoPointer<roachpb::AdminTransferLeaseRequest > admin_transfer_lease() const;
  GoPointer<roachpb::AdminChangeReplicasRequest > admin_change_replicas() const;
  GoPointer<roachpb::HeartbeatTxnRequest > heartbeat_txn() const;
  GoPointer<roachpb::GCRequest > gc() const;
  GoPointer<roachpb::PushTxnRequest > push_txn() const;
  GoPointer<roachpb::RangeLookupRequest > range_lookup() const;
  GoPointer<roachpb::ResolveIntentRequest > resolve_intent() const;
  GoPointer<roachpb::ResolveIntentRangeRequest > resolve_intent_range() const;
  GoPointer<roachpb::MergeRequest > merge() const;
  GoPointer<roachpb::TruncateLogRequest > truncate_log() const;
  GoPointer<roachpb::RequestLeaseRequest > request_lease() const;
  GoPointer<roachpb::ReverseScanRequest > reverse_scan() const;
  GoPointer<roachpb::ComputeChecksumRequest > compute_checksum() const;
  GoPointer<roachpb::DeprecatedVerifyChecksumRequest > deprecated_verify_checksum() const;
  GoPointer<roachpb::CheckConsistencyRequest > check_consistency() const;
  GoPointer<roachpb::NoopRequest > noop() const;
  GoPointer<roachpb::InitPutRequest > init_put() const;
  GoPointer<roachpb::TransferLeaseRequest > transfer_lease() const;
  GoPointer<roachpb::LeaseInfoRequest > lease_info() const;
  GoPointer<roachpb::WriteBatchRequest > write_batch() const;
  GoPointer<roachpb::ExportRequest > export_() const;
  GoPointer<roachpb::ImportRequest > import() const;
  GoPointer<roachpb::QueryTxnRequest > query_txn() const;
  GoPointer<roachpb::AdminScatterRequest > admin_scatter() const;
  GoPointer<roachpb::AddSSTableRequest > add_sstable() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class BatchRequest {
 public:
  enum { Size = 104 };

 public:
  BatchRequest(const void *data)
    : data_(data) {
  }

  roachpb::Header header() const;
  GoSlice<roachpb::RequestUnion> requests() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class BatchResponse; }

namespace roachpb { class BatchResponse_Header; }

namespace roachpb { class Error; }

namespace roachpb { class ErrorDetail; }

namespace roachpb { class NotLeaseHolderError; }

namespace roachpb {
class NotLeaseHolderError {
 public:
  enum { Size = 56 };

 public:
  NotLeaseHolderError(const void *data)
    : data_(data) {
  }

  roachpb::ReplicaDescriptor replica() const;
  GoPointer<roachpb::ReplicaDescriptor > lease_holder() const;
  GoPointer<roachpb::Lease > lease() const;
  int64_t range_id() const;
  GoString custom_msg() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RangeNotFoundError; }

namespace roachpb {
class RangeNotFoundError {
 public:
  enum { Size = 8 };

 public:
  RangeNotFoundError(const void *data)
    : data_(data) {
  }

  int64_t range_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RangeKeyMismatchError; }

namespace roachpb {
class RangeKeyMismatchError {
 public:
  enum { Size = 64 };

 public:
  RangeKeyMismatchError(const void *data)
    : data_(data) {
  }

  GoSlice<uint8_t> request_start_key() const;
  GoSlice<uint8_t> request_end_key() const;
  GoPointer<roachpb::RangeDescriptor > mismatched_range() const;
  GoPointer<roachpb::RangeDescriptor > suggested_range() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ReadWithinUncertaintyIntervalError; }

namespace roachpb {
class ReadWithinUncertaintyIntervalError {
 public:
  enum { Size = 32 };

 public:
  ReadWithinUncertaintyIntervalError(const void *data)
    : data_(data) {
  }

  hlc::Timestamp read_timestamp() const;
  hlc::Timestamp existing_timestamp() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TransactionAbortedError; }

namespace roachpb {
class TransactionAbortedError {
 public:
  enum { Size = 0 };

 public:
  TransactionAbortedError(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class TransactionPushError; }

namespace roachpb {
class TransactionPushError {
 public:
  enum { Size = 208 };

 public:
  TransactionPushError(const void *data)
    : data_(data) {
  }

  roachpb::Transaction pushee_txn() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TransactionRetryError; }

namespace roachpb {
class TransactionRetryError {
 public:
  enum { Size = 4 };

 public:
  TransactionRetryError(const void *data)
    : data_(data) {
  }

  int32_t reason() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TransactionReplayError; }

namespace roachpb {
class TransactionReplayError {
 public:
  enum { Size = 0 };

 public:
  TransactionReplayError(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class TransactionStatusError; }

namespace roachpb {
class TransactionStatusError {
 public:
  enum { Size = 16 };

 public:
  TransactionStatusError(const void *data)
    : data_(data) {
  }

  GoString msg() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class WriteIntentError; }

namespace roachpb { class Intent; }

namespace roachpb {
class Intent {
 public:
  enum { Size = 136 };

 public:
  Intent(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  enginepb::TxnMeta txn() const;
  int32_t status() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class WriteIntentError {
 public:
  enum { Size = 24 };

 public:
  WriteIntentError(const void *data)
    : data_(data) {
  }

  GoSlice<roachpb::Intent> intents() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class WriteTooOldError; }

namespace roachpb {
class WriteTooOldError {
 public:
  enum { Size = 32 };

 public:
  WriteTooOldError(const void *data)
    : data_(data) {
  }

  hlc::Timestamp timestamp() const;
  hlc::Timestamp actual_timestamp() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class OpRequiresTxnError; }

namespace roachpb {
class OpRequiresTxnError {
 public:
  enum { Size = 0 };

 public:
  OpRequiresTxnError(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class ConditionFailedError; }

namespace roachpb {
class ConditionFailedError {
 public:
  enum { Size = 8 };

 public:
  ConditionFailedError(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::Value > actual_value() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class LeaseRejectedError; }

namespace roachpb {
class LeaseRejectedError {
 public:
  enum { Size = 176 };

 public:
  LeaseRejectedError(const void *data)
    : data_(data) {
  }

  GoString message() const;
  roachpb::Lease requested() const;
  roachpb::Lease existing() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class NodeUnavailableError; }

namespace roachpb {
class NodeUnavailableError {
 public:
  enum { Size = 0 };

 public:
  NodeUnavailableError(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class SendError; }

namespace roachpb {
class SendError {
 public:
  enum { Size = 16 };

 public:
  SendError(const void *data)
    : data_(data) {
  }

  GoString message() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AmbiguousResultError; }

namespace roachpb {
class AmbiguousResultError {
 public:
  enum { Size = 24 };

 public:
  AmbiguousResultError(const void *data)
    : data_(data) {
  }

  GoString message() const;
  GoPointer<roachpb::Error > wrapped_err() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class StoreNotFoundError; }

namespace roachpb {
class StoreNotFoundError {
 public:
  enum { Size = 4 };

 public:
  StoreNotFoundError(const void *data)
    : data_(data) {
  }

  int32_t store_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class HandledRetryableTxnError; }

namespace roachpb {
class HandledRetryableTxnError {
 public:
  enum { Size = 32 };

 public:
  HandledRetryableTxnError(const void *data)
    : data_(data) {
  }

  GoString msg() const;
  GoPointer<uuid::UUID > txn_id() const;
  GoPointer<roachpb::Transaction > transaction() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RaftGroupDeletedError; }

namespace roachpb {
class RaftGroupDeletedError {
 public:
  enum { Size = 0 };

 public:
  RaftGroupDeletedError(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class ReplicaCorruptionError; }

namespace roachpb {
class ReplicaCorruptionError {
 public:
  enum { Size = 24 };

 public:
  ReplicaCorruptionError(const void *data)
    : data_(data) {
  }

  GoString error_msg() const;
  bool processed() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ReplicaTooOldError; }

namespace roachpb {
class ReplicaTooOldError {
 public:
  enum { Size = 4 };

 public:
  ReplicaTooOldError(const void *data)
    : data_(data) {
  }

  int32_t replica_id() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ErrorDetail {
 public:
  enum { Size = 176 };

 public:
  ErrorDetail(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::NotLeaseHolderError > not_lease_holder() const;
  GoPointer<roachpb::RangeNotFoundError > range_not_found() const;
  GoPointer<roachpb::RangeKeyMismatchError > range_key_mismatch() const;
  GoPointer<roachpb::ReadWithinUncertaintyIntervalError > read_within_uncertainty_interval() const;
  GoPointer<roachpb::TransactionAbortedError > transaction_aborted() const;
  GoPointer<roachpb::TransactionPushError > transaction_push() const;
  GoPointer<roachpb::TransactionRetryError > transaction_retry() const;
  GoPointer<roachpb::TransactionReplayError > transaction_replay() const;
  GoPointer<roachpb::TransactionStatusError > transaction_status() const;
  GoPointer<roachpb::WriteIntentError > write_intent() const;
  GoPointer<roachpb::WriteTooOldError > write_too_old() const;
  GoPointer<roachpb::OpRequiresTxnError > op_requires_txn() const;
  GoPointer<roachpb::ConditionFailedError > condition_failed() const;
  GoPointer<roachpb::LeaseRejectedError > lease_rejected() const;
  GoPointer<roachpb::NodeUnavailableError > node_unavailable() const;
  GoPointer<roachpb::SendError > send() const;
  GoPointer<roachpb::AmbiguousResultError > ambiguous_result() const;
  GoPointer<roachpb::StoreNotFoundError > store_not_found() const;
  GoPointer<roachpb::HandledRetryableTxnError > handled_retryable_txn_error() const;
  GoPointer<roachpb::RaftGroupDeletedError > raft_group_deleted() const;
  GoPointer<roachpb::ReplicaCorruptionError > replica_corruption() const;
  GoPointer<roachpb::ReplicaTooOldError > replica_too_old() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ErrPosition; }

namespace roachpb {
class ErrPosition {
 public:
  enum { Size = 4 };

 public:
  ErrPosition(const void *data)
    : data_(data) {
  }

  int32_t index() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class Error {
 public:
  enum { Size = 72 };

 public:
  Error(const void *data)
    : data_(data) {
  }

  GoString message() const;
  int32_t transaction_restart() const;
  GoPointer<roachpb::Transaction > unexposed_txn() const;
  int32_t origin_node() const;
  GoPointer<roachpb::ErrorDetail > detail() const;
  GoPointer<roachpb::ErrPosition > index() const;
  hlc::Timestamp now() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class BatchResponse_Header {
 public:
  enum { Size = 72 };

 public:
  BatchResponse_Header(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::Error > error() const;
  hlc::Timestamp timestamp() const;
  GoPointer<roachpb::Transaction > txn() const;
  hlc::Timestamp now() const;
  // TODO: collected_spans []tracing.RecordedSpan[slice] 48

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ResponseUnion; }

namespace roachpb { class GetResponse; }

namespace roachpb { class ResponseHeader; }

namespace roachpb { class RangeInfo; }

namespace roachpb {
class RangeInfo {
 public:
  enum { Size = 168 };

 public:
  RangeInfo(const void *data)
    : data_(data) {
  }

  roachpb::RangeDescriptor desc() const;
  roachpb::Lease lease() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ResponseHeader {
 public:
  enum { Size = 48 };

 public:
  ResponseHeader(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::Transaction > txn() const;
  GoPointer<roachpb::Span > resume_span() const;
  int64_t num_keys() const;
  GoSlice<roachpb::RangeInfo> range_infos() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class GetResponse {
 public:
  enum { Size = 56 };

 public:
  GetResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoPointer<roachpb::Value > value() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class PutResponse; }

namespace roachpb {
class PutResponse {
 public:
  enum { Size = 48 };

 public:
  PutResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ConditionalPutResponse; }

namespace roachpb {
class ConditionalPutResponse {
 public:
  enum { Size = 48 };

 public:
  ConditionalPutResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class IncrementResponse; }

namespace roachpb {
class IncrementResponse {
 public:
  enum { Size = 56 };

 public:
  IncrementResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  int64_t new_value() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeleteResponse; }

namespace roachpb {
class DeleteResponse {
 public:
  enum { Size = 48 };

 public:
  DeleteResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeleteRangeResponse; }

namespace roachpb {
class DeleteRangeResponse {
 public:
  enum { Size = 72 };

 public:
  DeleteRangeResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<GoSlice<uint8_t> > keys() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ScanResponse; }

namespace roachpb { class KeyValue; }

namespace roachpb {
class KeyValue {
 public:
  enum { Size = 64 };

 public:
  KeyValue(const void *data)
    : data_(data) {
  }

  GoSlice<uint8_t> key() const;
  roachpb::Value value() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ScanResponse {
 public:
  enum { Size = 72 };

 public:
  ScanResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<roachpb::KeyValue> rows() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class BeginTransactionResponse; }

namespace roachpb {
class BeginTransactionResponse {
 public:
  enum { Size = 48 };

 public:
  BeginTransactionResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class EndTransactionResponse; }

namespace roachpb {
class EndTransactionResponse {
 public:
  enum { Size = 56 };

 public:
  EndTransactionResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  bool one_phase_commit() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminSplitResponse; }

namespace roachpb {
class AdminSplitResponse {
 public:
  enum { Size = 48 };

 public:
  AdminSplitResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminMergeResponse; }

namespace roachpb {
class AdminMergeResponse {
 public:
  enum { Size = 48 };

 public:
  AdminMergeResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminTransferLeaseResponse; }

namespace roachpb {
class AdminTransferLeaseResponse {
 public:
  enum { Size = 48 };

 public:
  AdminTransferLeaseResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminChangeReplicasResponse; }

namespace roachpb {
class AdminChangeReplicasResponse {
 public:
  enum { Size = 48 };

 public:
  AdminChangeReplicasResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class HeartbeatTxnResponse; }

namespace roachpb {
class HeartbeatTxnResponse {
 public:
  enum { Size = 48 };

 public:
  HeartbeatTxnResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class GCResponse; }

namespace roachpb {
class GCResponse {
 public:
  enum { Size = 48 };

 public:
  GCResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class PushTxnResponse; }

namespace roachpb {
class PushTxnResponse {
 public:
  enum { Size = 256 };

 public:
  PushTxnResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  roachpb::Transaction pushee_txn() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RangeLookupResponse; }

namespace roachpb {
class RangeLookupResponse {
 public:
  enum { Size = 96 };

 public:
  RangeLookupResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<roachpb::RangeDescriptor> ranges() const;
  GoSlice<roachpb::RangeDescriptor> prefetched_ranges() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ResolveIntentResponse; }

namespace roachpb {
class ResolveIntentResponse {
 public:
  enum { Size = 48 };

 public:
  ResolveIntentResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ResolveIntentRangeResponse; }

namespace roachpb {
class ResolveIntentRangeResponse {
 public:
  enum { Size = 48 };

 public:
  ResolveIntentRangeResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class MergeResponse; }

namespace roachpb {
class MergeResponse {
 public:
  enum { Size = 48 };

 public:
  MergeResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class TruncateLogResponse; }

namespace roachpb {
class TruncateLogResponse {
 public:
  enum { Size = 48 };

 public:
  TruncateLogResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class RequestLeaseResponse; }

namespace roachpb {
class RequestLeaseResponse {
 public:
  enum { Size = 48 };

 public:
  RequestLeaseResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ReverseScanResponse; }

namespace roachpb {
class ReverseScanResponse {
 public:
  enum { Size = 72 };

 public:
  ReverseScanResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<roachpb::KeyValue> rows() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ComputeChecksumResponse; }

namespace roachpb {
class ComputeChecksumResponse {
 public:
  enum { Size = 48 };

 public:
  ComputeChecksumResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class DeprecatedVerifyChecksumResponse; }

namespace roachpb {
class DeprecatedVerifyChecksumResponse {
 public:
  enum { Size = 48 };

 public:
  DeprecatedVerifyChecksumResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class CheckConsistencyResponse; }

namespace roachpb {
class CheckConsistencyResponse {
 public:
  enum { Size = 48 };

 public:
  CheckConsistencyResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class NoopResponse; }

namespace roachpb {
class NoopResponse {
 public:
  enum { Size = 0 };

 public:
  NoopResponse(const void *data) {
  }
};
}  // namespace roachpb

namespace roachpb { class InitPutResponse; }

namespace roachpb {
class InitPutResponse {
 public:
  enum { Size = 48 };

 public:
  InitPutResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class LeaseInfoResponse; }

namespace roachpb {
class LeaseInfoResponse {
 public:
  enum { Size = 128 };

 public:
  LeaseInfoResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  roachpb::Lease lease() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class WriteBatchResponse; }

namespace roachpb {
class WriteBatchResponse {
 public:
  enum { Size = 48 };

 public:
  WriteBatchResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ExportResponse; }

namespace roachpb { class ExportResponse_File; }

namespace roachpb { class BulkOpSummary; }

namespace roachpb {
class BulkOpSummary {
 public:
  enum { Size = 32 };

 public:
  BulkOpSummary(const void *data)
    : data_(data) {
  }

  int64_t data_size() const;
  int64_t rows() const;
  int64_t index_entries() const;
  int64_t system_records() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ExportResponse_File {
 public:
  enum { Size = 120 };

 public:
  ExportResponse_File(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;
  GoString path() const;
  GoSlice<uint8_t> sha512() const;
  roachpb::BulkOpSummary exported() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ExportResponse {
 public:
  enum { Size = 72 };

 public:
  ExportResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<roachpb::ExportResponse_File> files() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class ImportResponse; }

namespace roachpb {
class ImportResponse {
 public:
  enum { Size = 80 };

 public:
  ImportResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  roachpb::BulkOpSummary imported() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class QueryTxnResponse; }

namespace roachpb {
class QueryTxnResponse {
 public:
  enum { Size = 280 };

 public:
  QueryTxnResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  roachpb::Transaction queried_txn() const;
  GoSlice<uuid::UUID> waiting_txns() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AdminScatterResponse; }

namespace roachpb { class AdminScatterResponse_Range; }

namespace roachpb {
class AdminScatterResponse_Range {
 public:
  enum { Size = 48 };

 public:
  AdminScatterResponse_Range(const void *data)
    : data_(data) {
  }

  roachpb::Span span() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class AdminScatterResponse {
 public:
  enum { Size = 72 };

 public:
  AdminScatterResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;
  GoSlice<roachpb::AdminScatterResponse_Range> ranges() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb { class AddSSTableResponse; }

namespace roachpb {
class AddSSTableResponse {
 public:
  enum { Size = 48 };

 public:
  AddSSTableResponse(const void *data)
    : data_(data) {
  }

  roachpb::ResponseHeader response_header() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class ResponseUnion {
 public:
  enum { Size = 280 };

 public:
  ResponseUnion(const void *data)
    : data_(data) {
  }

  GoPointer<roachpb::GetResponse > get() const;
  GoPointer<roachpb::PutResponse > put() const;
  GoPointer<roachpb::ConditionalPutResponse > conditional_put() const;
  GoPointer<roachpb::IncrementResponse > increment() const;
  GoPointer<roachpb::DeleteResponse > delete_() const;
  GoPointer<roachpb::DeleteRangeResponse > delete_range() const;
  GoPointer<roachpb::ScanResponse > scan() const;
  GoPointer<roachpb::BeginTransactionResponse > begin_transaction() const;
  GoPointer<roachpb::EndTransactionResponse > end_transaction() const;
  GoPointer<roachpb::AdminSplitResponse > admin_split() const;
  GoPointer<roachpb::AdminMergeResponse > admin_merge() const;
  GoPointer<roachpb::AdminTransferLeaseResponse > admin_transfer_lease() const;
  GoPointer<roachpb::AdminChangeReplicasResponse > admin_change_replicas() const;
  GoPointer<roachpb::HeartbeatTxnResponse > heartbeat_txn() const;
  GoPointer<roachpb::GCResponse > gc() const;
  GoPointer<roachpb::PushTxnResponse > push_txn() const;
  GoPointer<roachpb::RangeLookupResponse > range_lookup() const;
  GoPointer<roachpb::ResolveIntentResponse > resolve_intent() const;
  GoPointer<roachpb::ResolveIntentRangeResponse > resolve_intent_range() const;
  GoPointer<roachpb::MergeResponse > merge() const;
  GoPointer<roachpb::TruncateLogResponse > truncate_log() const;
  GoPointer<roachpb::RequestLeaseResponse > request_lease() const;
  GoPointer<roachpb::ReverseScanResponse > reverse_scan() const;
  GoPointer<roachpb::ComputeChecksumResponse > compute_checksum() const;
  GoPointer<roachpb::DeprecatedVerifyChecksumResponse > deprecated_verify_checksum() const;
  GoPointer<roachpb::CheckConsistencyResponse > check_consistency() const;
  GoPointer<roachpb::NoopResponse > noop() const;
  GoPointer<roachpb::InitPutResponse > init_put() const;
  GoPointer<roachpb::LeaseInfoResponse > lease_info() const;
  GoPointer<roachpb::WriteBatchResponse > write_batch() const;
  GoPointer<roachpb::ExportResponse > export_() const;
  GoPointer<roachpb::ImportResponse > import() const;
  GoPointer<roachpb::QueryTxnResponse > query_txn() const;
  GoPointer<roachpb::AdminScatterResponse > admin_scatter() const;
  GoPointer<roachpb::AddSSTableResponse > add_sstable() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
class BatchResponse {
 public:
  enum { Size = 96 };

 public:
  BatchResponse(const void *data)
    : data_(data) {
  }

  roachpb::BatchResponse_Header batch_response__header() const;
  GoSlice<roachpb::ResponseUnion> responses() const;

 private:
  const void *data_;
};
}  // namespace roachpb

namespace roachpb {
inline roachpb::Header BatchRequest::header() const {
  return GoType<roachpb::Header>::get(data_, 0);
}
inline GoSlice<roachpb::RequestUnion> BatchRequest::requests() const {
  return GoType<GoSlice<roachpb::RequestUnion> >::get(data_, 80);
}
}  // namespace roachpb

namespace roachpb {
inline hlc::Timestamp Header::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 0);
}
inline roachpb::ReplicaDescriptor Header::replica() const {
  return GoType<roachpb::ReplicaDescriptor>::get(data_, 16);
}
inline int64_t Header::range_id() const {
  return GoType<int64_t>::get(data_, 32);
}
inline double Header::user_priority() const {
  return GoType<double>::get(data_, 40);
}
inline GoPointer<roachpb::Transaction > Header::txn() const {
  return GoType<GoPointer<roachpb::Transaction > >::get(data_, 48);
}
inline int32_t Header::read_consistency() const {
  return GoType<int32_t>::get(data_, 56);
}
inline int64_t Header::max_span_request_keys() const {
  return GoType<int64_t>::get(data_, 64);
}
inline bool Header::distinct_spans() const {
  return GoType<bool>::get(data_, 72);
}
inline bool Header::return_range_info() const {
  return GoType<bool>::get(data_, 73);
}
inline int32_t Header::gateway_node_id() const {
  return GoType<int32_t>::get(data_, 76);
}
}  // namespace roachpb

namespace hlc {
inline int64_t Timestamp::wall_time() const {
  return GoType<int64_t>::get(data_, 0);
}
inline int32_t Timestamp::logical() const {
  return GoType<int32_t>::get(data_, 8);
}
}  // namespace hlc

namespace roachpb {
inline int32_t ReplicaDescriptor::node_id() const {
  return GoType<int32_t>::get(data_, 0);
}
inline int32_t ReplicaDescriptor::store_id() const {
  return GoType<int32_t>::get(data_, 4);
}
inline int32_t ReplicaDescriptor::replica_id() const {
  return GoType<int32_t>::get(data_, 8);
}
}  // namespace roachpb

namespace roachpb {
inline enginepb::TxnMeta Transaction::txn_meta() const {
  return GoType<enginepb::TxnMeta>::get(data_, 0);
}
inline GoString Transaction::name() const {
  return GoType<GoString>::get(data_, 80);
}
inline int32_t Transaction::status() const {
  return GoType<int32_t>::get(data_, 96);
}
inline hlc::Timestamp Transaction::last_heartbeat() const {
  return GoType<hlc::Timestamp>::get(data_, 104);
}
inline hlc::Timestamp Transaction::orig_timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 120);
}
inline hlc::Timestamp Transaction::max_timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 136);
}
inline GoSlice<roachpb::ObservedTimestamp> Transaction::observed_timestamps() const {
  return GoType<GoSlice<roachpb::ObservedTimestamp> >::get(data_, 152);
}
inline bool Transaction::writing() const {
  return GoType<bool>::get(data_, 176);
}
inline bool Transaction::write_too_old() const {
  return GoType<bool>::get(data_, 177);
}
inline bool Transaction::retry_on_push() const {
  return GoType<bool>::get(data_, 178);
}
inline GoSlice<roachpb::Span> Transaction::intents() const {
  return GoType<GoSlice<roachpb::Span> >::get(data_, 184);
}
}  // namespace roachpb

namespace enginepb {
inline GoPointer<uuid::UUID > TxnMeta::id() const {
  return GoType<GoPointer<uuid::UUID > >::get(data_, 0);
}
inline int32_t TxnMeta::isolation() const {
  return GoType<int32_t>::get(data_, 8);
}
inline GoSlice<uint8_t> TxnMeta::key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 16);
}
inline uint32_t TxnMeta::epoch() const {
  return GoType<uint32_t>::get(data_, 40);
}
inline hlc::Timestamp TxnMeta::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 48);
}
inline int32_t TxnMeta::priority() const {
  return GoType<int32_t>::get(data_, 64);
}
inline int32_t TxnMeta::sequence() const {
  return GoType<int32_t>::get(data_, 68);
}
inline int32_t TxnMeta::batch_index() const {
  return GoType<int32_t>::get(data_, 72);
}
}  // namespace enginepb

namespace uuid {
inline GoArray<uint8_t,16> UUID::uuid() const {
  return GoType<GoArray<uint8_t,16> >::get(data_, 0);
}
}  // namespace uuid

namespace roachpb {
inline int32_t ObservedTimestamp::node_id() const {
  return GoType<int32_t>::get(data_, 0);
}
inline hlc::Timestamp ObservedTimestamp::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 8);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<uint8_t> Span::key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 0);
}
inline GoSlice<uint8_t> Span::end_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::GetRequest > RequestUnion::get() const {
  return GoType<GoPointer<roachpb::GetRequest > >::get(data_, 0);
}
inline GoPointer<roachpb::PutRequest > RequestUnion::put() const {
  return GoType<GoPointer<roachpb::PutRequest > >::get(data_, 8);
}
inline GoPointer<roachpb::ConditionalPutRequest > RequestUnion::conditional_put() const {
  return GoType<GoPointer<roachpb::ConditionalPutRequest > >::get(data_, 16);
}
inline GoPointer<roachpb::IncrementRequest > RequestUnion::increment() const {
  return GoType<GoPointer<roachpb::IncrementRequest > >::get(data_, 24);
}
inline GoPointer<roachpb::DeleteRequest > RequestUnion::delete_() const {
  return GoType<GoPointer<roachpb::DeleteRequest > >::get(data_, 32);
}
inline GoPointer<roachpb::DeleteRangeRequest > RequestUnion::delete_range() const {
  return GoType<GoPointer<roachpb::DeleteRangeRequest > >::get(data_, 40);
}
inline GoPointer<roachpb::ScanRequest > RequestUnion::scan() const {
  return GoType<GoPointer<roachpb::ScanRequest > >::get(data_, 48);
}
inline GoPointer<roachpb::BeginTransactionRequest > RequestUnion::begin_transaction() const {
  return GoType<GoPointer<roachpb::BeginTransactionRequest > >::get(data_, 56);
}
inline GoPointer<roachpb::EndTransactionRequest > RequestUnion::end_transaction() const {
  return GoType<GoPointer<roachpb::EndTransactionRequest > >::get(data_, 64);
}
inline GoPointer<roachpb::AdminSplitRequest > RequestUnion::admin_split() const {
  return GoType<GoPointer<roachpb::AdminSplitRequest > >::get(data_, 72);
}
inline GoPointer<roachpb::AdminMergeRequest > RequestUnion::admin_merge() const {
  return GoType<GoPointer<roachpb::AdminMergeRequest > >::get(data_, 80);
}
inline GoPointer<roachpb::AdminTransferLeaseRequest > RequestUnion::admin_transfer_lease() const {
  return GoType<GoPointer<roachpb::AdminTransferLeaseRequest > >::get(data_, 88);
}
inline GoPointer<roachpb::AdminChangeReplicasRequest > RequestUnion::admin_change_replicas() const {
  return GoType<GoPointer<roachpb::AdminChangeReplicasRequest > >::get(data_, 96);
}
inline GoPointer<roachpb::HeartbeatTxnRequest > RequestUnion::heartbeat_txn() const {
  return GoType<GoPointer<roachpb::HeartbeatTxnRequest > >::get(data_, 104);
}
inline GoPointer<roachpb::GCRequest > RequestUnion::gc() const {
  return GoType<GoPointer<roachpb::GCRequest > >::get(data_, 112);
}
inline GoPointer<roachpb::PushTxnRequest > RequestUnion::push_txn() const {
  return GoType<GoPointer<roachpb::PushTxnRequest > >::get(data_, 120);
}
inline GoPointer<roachpb::RangeLookupRequest > RequestUnion::range_lookup() const {
  return GoType<GoPointer<roachpb::RangeLookupRequest > >::get(data_, 128);
}
inline GoPointer<roachpb::ResolveIntentRequest > RequestUnion::resolve_intent() const {
  return GoType<GoPointer<roachpb::ResolveIntentRequest > >::get(data_, 136);
}
inline GoPointer<roachpb::ResolveIntentRangeRequest > RequestUnion::resolve_intent_range() const {
  return GoType<GoPointer<roachpb::ResolveIntentRangeRequest > >::get(data_, 144);
}
inline GoPointer<roachpb::MergeRequest > RequestUnion::merge() const {
  return GoType<GoPointer<roachpb::MergeRequest > >::get(data_, 152);
}
inline GoPointer<roachpb::TruncateLogRequest > RequestUnion::truncate_log() const {
  return GoType<GoPointer<roachpb::TruncateLogRequest > >::get(data_, 160);
}
inline GoPointer<roachpb::RequestLeaseRequest > RequestUnion::request_lease() const {
  return GoType<GoPointer<roachpb::RequestLeaseRequest > >::get(data_, 168);
}
inline GoPointer<roachpb::ReverseScanRequest > RequestUnion::reverse_scan() const {
  return GoType<GoPointer<roachpb::ReverseScanRequest > >::get(data_, 176);
}
inline GoPointer<roachpb::ComputeChecksumRequest > RequestUnion::compute_checksum() const {
  return GoType<GoPointer<roachpb::ComputeChecksumRequest > >::get(data_, 184);
}
inline GoPointer<roachpb::DeprecatedVerifyChecksumRequest > RequestUnion::deprecated_verify_checksum() const {
  return GoType<GoPointer<roachpb::DeprecatedVerifyChecksumRequest > >::get(data_, 192);
}
inline GoPointer<roachpb::CheckConsistencyRequest > RequestUnion::check_consistency() const {
  return GoType<GoPointer<roachpb::CheckConsistencyRequest > >::get(data_, 200);
}
inline GoPointer<roachpb::NoopRequest > RequestUnion::noop() const {
  return GoType<GoPointer<roachpb::NoopRequest > >::get(data_, 208);
}
inline GoPointer<roachpb::InitPutRequest > RequestUnion::init_put() const {
  return GoType<GoPointer<roachpb::InitPutRequest > >::get(data_, 216);
}
inline GoPointer<roachpb::TransferLeaseRequest > RequestUnion::transfer_lease() const {
  return GoType<GoPointer<roachpb::TransferLeaseRequest > >::get(data_, 224);
}
inline GoPointer<roachpb::LeaseInfoRequest > RequestUnion::lease_info() const {
  return GoType<GoPointer<roachpb::LeaseInfoRequest > >::get(data_, 232);
}
inline GoPointer<roachpb::WriteBatchRequest > RequestUnion::write_batch() const {
  return GoType<GoPointer<roachpb::WriteBatchRequest > >::get(data_, 240);
}
inline GoPointer<roachpb::ExportRequest > RequestUnion::export_() const {
  return GoType<GoPointer<roachpb::ExportRequest > >::get(data_, 248);
}
inline GoPointer<roachpb::ImportRequest > RequestUnion::import() const {
  return GoType<GoPointer<roachpb::ImportRequest > >::get(data_, 256);
}
inline GoPointer<roachpb::QueryTxnRequest > RequestUnion::query_txn() const {
  return GoType<GoPointer<roachpb::QueryTxnRequest > >::get(data_, 264);
}
inline GoPointer<roachpb::AdminScatterRequest > RequestUnion::admin_scatter() const {
  return GoType<GoPointer<roachpb::AdminScatterRequest > >::get(data_, 272);
}
inline GoPointer<roachpb::AddSSTableRequest > RequestUnion::add_sstable() const {
  return GoType<GoPointer<roachpb::AddSSTableRequest > >::get(data_, 280);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span GetRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span PutRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Value PutRequest::value() const {
  return GoType<roachpb::Value>::get(data_, 48);
}
inline bool PutRequest::inline_() const {
  return GoType<bool>::get(data_, 88);
}
inline bool PutRequest::blind() const {
  return GoType<bool>::get(data_, 89);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<uint8_t> Value::raw_bytes() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 0);
}
inline hlc::Timestamp Value::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ConditionalPutRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Value ConditionalPutRequest::value() const {
  return GoType<roachpb::Value>::get(data_, 48);
}
inline GoPointer<roachpb::Value > ConditionalPutRequest::exp_value() const {
  return GoType<GoPointer<roachpb::Value > >::get(data_, 88);
}
inline bool ConditionalPutRequest::blind() const {
  return GoType<bool>::get(data_, 96);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span IncrementRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline int64_t IncrementRequest::increment() const {
  return GoType<int64_t>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span DeleteRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span DeleteRangeRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline bool DeleteRangeRequest::return_keys() const {
  return GoType<bool>::get(data_, 48);
}
inline bool DeleteRangeRequest::inline_() const {
  return GoType<bool>::get(data_, 49);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ScanRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span BeginTransactionRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span EndTransactionRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline bool EndTransactionRequest::commit() const {
  return GoType<bool>::get(data_, 48);
}
inline GoPointer<hlc::Timestamp > EndTransactionRequest::deadline() const {
  return GoType<GoPointer<hlc::Timestamp > >::get(data_, 56);
}
inline GoPointer<roachpb::InternalCommitTrigger > EndTransactionRequest::internal_commit_trigger() const {
  return GoType<GoPointer<roachpb::InternalCommitTrigger > >::get(data_, 64);
}
inline GoSlice<roachpb::Span> EndTransactionRequest::intent_spans() const {
  return GoType<GoSlice<roachpb::Span> >::get(data_, 72);
}
inline bool EndTransactionRequest::require1_pc() const {
  return GoType<bool>::get(data_, 96);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::SplitTrigger > InternalCommitTrigger::split_trigger() const {
  return GoType<GoPointer<roachpb::SplitTrigger > >::get(data_, 0);
}
inline GoPointer<roachpb::MergeTrigger > InternalCommitTrigger::merge_trigger() const {
  return GoType<GoPointer<roachpb::MergeTrigger > >::get(data_, 8);
}
inline GoPointer<roachpb::ChangeReplicasTrigger > InternalCommitTrigger::change_replicas_trigger() const {
  return GoType<GoPointer<roachpb::ChangeReplicasTrigger > >::get(data_, 16);
}
inline GoPointer<roachpb::ModifiedSpanTrigger > InternalCommitTrigger::modified_span_trigger() const {
  return GoType<GoPointer<roachpb::ModifiedSpanTrigger > >::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::RangeDescriptor SplitTrigger::left_desc() const {
  return GoType<roachpb::RangeDescriptor>::get(data_, 0);
}
inline roachpb::RangeDescriptor SplitTrigger::right_desc() const {
  return GoType<roachpb::RangeDescriptor>::get(data_, 88);
}
}  // namespace roachpb

namespace roachpb {
inline int64_t RangeDescriptor::range_id() const {
  return GoType<int64_t>::get(data_, 0);
}
inline GoSlice<uint8_t> RangeDescriptor::start_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 8);
}
inline GoSlice<uint8_t> RangeDescriptor::end_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 32);
}
inline GoSlice<roachpb::ReplicaDescriptor> RangeDescriptor::replicas() const {
  return GoType<GoSlice<roachpb::ReplicaDescriptor> >::get(data_, 56);
}
inline int32_t RangeDescriptor::next_replica_id() const {
  return GoType<int32_t>::get(data_, 80);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::RangeDescriptor MergeTrigger::left_desc() const {
  return GoType<roachpb::RangeDescriptor>::get(data_, 0);
}
inline roachpb::RangeDescriptor MergeTrigger::right_desc() const {
  return GoType<roachpb::RangeDescriptor>::get(data_, 88);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t ChangeReplicasTrigger::change_type() const {
  return GoType<int32_t>::get(data_, 0);
}
inline roachpb::ReplicaDescriptor ChangeReplicasTrigger::replica() const {
  return GoType<roachpb::ReplicaDescriptor>::get(data_, 4);
}
inline GoSlice<roachpb::ReplicaDescriptor> ChangeReplicasTrigger::updated_replicas() const {
  return GoType<GoSlice<roachpb::ReplicaDescriptor> >::get(data_, 16);
}
inline int32_t ChangeReplicasTrigger::next_replica_id() const {
  return GoType<int32_t>::get(data_, 40);
}
}  // namespace roachpb

namespace roachpb {
inline bool ModifiedSpanTrigger::system_config_span() const {
  return GoType<bool>::get(data_, 0);
}
inline GoPointer<roachpb::Span > ModifiedSpanTrigger::node_liveness_span() const {
  return GoType<GoPointer<roachpb::Span > >::get(data_, 8);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminSplitRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline GoSlice<uint8_t> AdminSplitRequest::split_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminMergeRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminTransferLeaseRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline int32_t AdminTransferLeaseRequest::target() const {
  return GoType<int32_t>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminChangeReplicasRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline int32_t AdminChangeReplicasRequest::change_type() const {
  return GoType<int32_t>::get(data_, 48);
}
inline GoSlice<roachpb::ReplicationTarget> AdminChangeReplicasRequest::targets() const {
  return GoType<GoSlice<roachpb::ReplicationTarget> >::get(data_, 56);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t ReplicationTarget::node_id() const {
  return GoType<int32_t>::get(data_, 0);
}
inline int32_t ReplicationTarget::store_id() const {
  return GoType<int32_t>::get(data_, 4);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span HeartbeatTxnRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline hlc::Timestamp HeartbeatTxnRequest::now() const {
  return GoType<hlc::Timestamp>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span GCRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline GoSlice<roachpb::GCRequest_GCKey> GCRequest::keys() const {
  return GoType<GoSlice<roachpb::GCRequest_GCKey> >::get(data_, 48);
}
inline hlc::Timestamp GCRequest::threshold() const {
  return GoType<hlc::Timestamp>::get(data_, 72);
}
inline hlc::Timestamp GCRequest::txn_span_gcthreshold() const {
  return GoType<hlc::Timestamp>::get(data_, 88);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<uint8_t> GCRequest_GCKey::key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 0);
}
inline hlc::Timestamp GCRequest_GCKey::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span PushTxnRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Transaction PushTxnRequest::pusher_txn() const {
  return GoType<roachpb::Transaction>::get(data_, 48);
}
inline enginepb::TxnMeta PushTxnRequest::pushee_txn() const {
  return GoType<enginepb::TxnMeta>::get(data_, 256);
}
inline hlc::Timestamp PushTxnRequest::push_to() const {
  return GoType<hlc::Timestamp>::get(data_, 336);
}
inline hlc::Timestamp PushTxnRequest::now() const {
  return GoType<hlc::Timestamp>::get(data_, 352);
}
inline int32_t PushTxnRequest::push_type() const {
  return GoType<int32_t>::get(data_, 368);
}
inline bool PushTxnRequest::force() const {
  return GoType<bool>::get(data_, 372);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span RangeLookupRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline int32_t RangeLookupRequest::max_ranges() const {
  return GoType<int32_t>::get(data_, 48);
}
inline bool RangeLookupRequest::reverse() const {
  return GoType<bool>::get(data_, 52);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ResolveIntentRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline enginepb::TxnMeta ResolveIntentRequest::intent_txn() const {
  return GoType<enginepb::TxnMeta>::get(data_, 48);
}
inline int32_t ResolveIntentRequest::status() const {
  return GoType<int32_t>::get(data_, 128);
}
inline bool ResolveIntentRequest::poison() const {
  return GoType<bool>::get(data_, 132);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ResolveIntentRangeRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline enginepb::TxnMeta ResolveIntentRangeRequest::intent_txn() const {
  return GoType<enginepb::TxnMeta>::get(data_, 48);
}
inline int32_t ResolveIntentRangeRequest::status() const {
  return GoType<int32_t>::get(data_, 128);
}
inline bool ResolveIntentRangeRequest::poison() const {
  return GoType<bool>::get(data_, 132);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span MergeRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Value MergeRequest::value() const {
  return GoType<roachpb::Value>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span TruncateLogRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline uint64_t TruncateLogRequest::index() const {
  return GoType<uint64_t>::get(data_, 48);
}
inline int64_t TruncateLogRequest::range_id() const {
  return GoType<int64_t>::get(data_, 56);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span RequestLeaseRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Lease RequestLeaseRequest::lease() const {
  return GoType<roachpb::Lease>::get(data_, 48);
}
inline roachpb::Lease RequestLeaseRequest::prev_lease() const {
  return GoType<roachpb::Lease>::get(data_, 128);
}
}  // namespace roachpb

namespace roachpb {
inline hlc::Timestamp Lease::start() const {
  return GoType<hlc::Timestamp>::get(data_, 0);
}
inline hlc::Timestamp Lease::expiration() const {
  return GoType<hlc::Timestamp>::get(data_, 16);
}
inline roachpb::ReplicaDescriptor Lease::replica() const {
  return GoType<roachpb::ReplicaDescriptor>::get(data_, 32);
}
inline hlc::Timestamp Lease::deprecated_start_stasis() const {
  return GoType<hlc::Timestamp>::get(data_, 48);
}
inline GoPointer<hlc::Timestamp > Lease::proposed_ts() const {
  return GoType<GoPointer<hlc::Timestamp > >::get(data_, 64);
}
inline GoPointer<int64_t > Lease::epoch() const {
  return GoType<GoPointer<int64_t > >::get(data_, 72);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ReverseScanRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ComputeChecksumRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline uint32_t ComputeChecksumRequest::version() const {
  return GoType<uint32_t>::get(data_, 48);
}
inline uuid::UUID ComputeChecksumRequest::checksum_id() const {
  return GoType<uuid::UUID>::get(data_, 52);
}
inline bool ComputeChecksumRequest::snapshot() const {
  return GoType<bool>::get(data_, 68);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span DeprecatedVerifyChecksumRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span CheckConsistencyRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline bool CheckConsistencyRequest::with_diff() const {
  return GoType<bool>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span InitPutRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Value InitPutRequest::value() const {
  return GoType<roachpb::Value>::get(data_, 48);
}
inline bool InitPutRequest::blind() const {
  return GoType<bool>::get(data_, 88);
}
inline bool InitPutRequest::fail_on_tombstones() const {
  return GoType<bool>::get(data_, 89);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span TransferLeaseRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Lease TransferLeaseRequest::lease() const {
  return GoType<roachpb::Lease>::get(data_, 48);
}
inline roachpb::Lease TransferLeaseRequest::prev_lease() const {
  return GoType<roachpb::Lease>::get(data_, 128);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span LeaseInfoRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span WriteBatchRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::Span WriteBatchRequest::data_span() const {
  return GoType<roachpb::Span>::get(data_, 48);
}
inline GoSlice<uint8_t> WriteBatchRequest::data() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 96);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ExportRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline roachpb::ExportStorage ExportRequest::storage() const {
  return GoType<roachpb::ExportStorage>::get(data_, 48);
}
inline hlc::Timestamp ExportRequest::start_time() const {
  return GoType<hlc::Timestamp>::get(data_, 112);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t ExportStorage::provider() const {
  return GoType<int32_t>::get(data_, 0);
}
inline roachpb::ExportStorage_LocalFilePath ExportStorage::local_file() const {
  return GoType<roachpb::ExportStorage_LocalFilePath>::get(data_, 8);
}
inline roachpb::ExportStorage_Http ExportStorage::http_path() const {
  return GoType<roachpb::ExportStorage_Http>::get(data_, 24);
}
inline GoPointer<roachpb::ExportStorage_GCS > ExportStorage::google_cloud_config() const {
  return GoType<GoPointer<roachpb::ExportStorage_GCS > >::get(data_, 40);
}
inline GoPointer<roachpb::ExportStorage_S3 > ExportStorage::s3_config() const {
  return GoType<GoPointer<roachpb::ExportStorage_S3 > >::get(data_, 48);
}
inline GoPointer<roachpb::ExportStorage_Azure > ExportStorage::azure_config() const {
  return GoType<GoPointer<roachpb::ExportStorage_Azure > >::get(data_, 56);
}
}  // namespace roachpb

namespace roachpb {
inline GoString ExportStorage_LocalFilePath::path() const {
  return GoType<GoString>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoString ExportStorage_Http::base_uri() const {
  return GoType<GoString>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoString ExportStorage_GCS::bucket() const {
  return GoType<GoString>::get(data_, 0);
}
inline GoString ExportStorage_GCS::prefix() const {
  return GoType<GoString>::get(data_, 16);
}
}  // namespace roachpb

namespace roachpb {
inline GoString ExportStorage_S3::bucket() const {
  return GoType<GoString>::get(data_, 0);
}
inline GoString ExportStorage_S3::prefix() const {
  return GoType<GoString>::get(data_, 16);
}
inline GoString ExportStorage_S3::access_key() const {
  return GoType<GoString>::get(data_, 32);
}
inline GoString ExportStorage_S3::secret() const {
  return GoType<GoString>::get(data_, 48);
}
inline GoString ExportStorage_S3::temp_token() const {
  return GoType<GoString>::get(data_, 64);
}
}  // namespace roachpb

namespace roachpb {
inline GoString ExportStorage_Azure::container() const {
  return GoType<GoString>::get(data_, 0);
}
inline GoString ExportStorage_Azure::prefix() const {
  return GoType<GoString>::get(data_, 16);
}
inline GoString ExportStorage_Azure::account_name() const {
  return GoType<GoString>::get(data_, 32);
}
inline GoString ExportStorage_Azure::account_key() const {
  return GoType<GoString>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ImportRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline GoSlice<roachpb::ImportRequest_File> ImportRequest::files() const {
  return GoType<GoSlice<roachpb::ImportRequest_File> >::get(data_, 48);
}
inline roachpb::Span ImportRequest::data_span() const {
  return GoType<roachpb::Span>::get(data_, 72);
}
inline GoSlice<roachpb::ImportRequest_TableRekey> ImportRequest::rekeys() const {
  return GoType<GoSlice<roachpb::ImportRequest_TableRekey> >::get(data_, 120);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ExportStorage ImportRequest_File::dir() const {
  return GoType<roachpb::ExportStorage>::get(data_, 0);
}
inline GoString ImportRequest_File::path() const {
  return GoType<GoString>::get(data_, 64);
}
inline GoSlice<uint8_t> ImportRequest_File::sha512() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 80);
}
}  // namespace roachpb

namespace roachpb {
inline uint32_t ImportRequest_TableRekey::old_id() const {
  return GoType<uint32_t>::get(data_, 0);
}
inline GoSlice<uint8_t> ImportRequest_TableRekey::new_desc() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 8);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span QueryTxnRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline enginepb::TxnMeta QueryTxnRequest::txn() const {
  return GoType<enginepb::TxnMeta>::get(data_, 48);
}
inline bool QueryTxnRequest::wait_for_update() const {
  return GoType<bool>::get(data_, 128);
}
inline GoSlice<uuid::UUID> QueryTxnRequest::known_waiting_txns() const {
  return GoType<GoSlice<uuid::UUID> >::get(data_, 136);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminScatterRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AddSSTableRequest::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline GoSlice<uint8_t> AddSSTableRequest::data() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::BatchResponse_Header BatchResponse::batch_response__header() const {
  return GoType<roachpb::BatchResponse_Header>::get(data_, 0);
}
inline GoSlice<roachpb::ResponseUnion> BatchResponse::responses() const {
  return GoType<GoSlice<roachpb::ResponseUnion> >::get(data_, 72);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::Error > BatchResponse_Header::error() const {
  return GoType<GoPointer<roachpb::Error > >::get(data_, 0);
}
inline hlc::Timestamp BatchResponse_Header::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 8);
}
inline GoPointer<roachpb::Transaction > BatchResponse_Header::txn() const {
  return GoType<GoPointer<roachpb::Transaction > >::get(data_, 24);
}
inline hlc::Timestamp BatchResponse_Header::now() const {
  return GoType<hlc::Timestamp>::get(data_, 32);
}
}  // namespace roachpb

namespace roachpb {
inline GoString Error::message() const {
  return GoType<GoString>::get(data_, 0);
}
inline int32_t Error::transaction_restart() const {
  return GoType<int32_t>::get(data_, 16);
}
inline GoPointer<roachpb::Transaction > Error::unexposed_txn() const {
  return GoType<GoPointer<roachpb::Transaction > >::get(data_, 24);
}
inline int32_t Error::origin_node() const {
  return GoType<int32_t>::get(data_, 32);
}
inline GoPointer<roachpb::ErrorDetail > Error::detail() const {
  return GoType<GoPointer<roachpb::ErrorDetail > >::get(data_, 40);
}
inline GoPointer<roachpb::ErrPosition > Error::index() const {
  return GoType<GoPointer<roachpb::ErrPosition > >::get(data_, 48);
}
inline hlc::Timestamp Error::now() const {
  return GoType<hlc::Timestamp>::get(data_, 56);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::NotLeaseHolderError > ErrorDetail::not_lease_holder() const {
  return GoType<GoPointer<roachpb::NotLeaseHolderError > >::get(data_, 0);
}
inline GoPointer<roachpb::RangeNotFoundError > ErrorDetail::range_not_found() const {
  return GoType<GoPointer<roachpb::RangeNotFoundError > >::get(data_, 8);
}
inline GoPointer<roachpb::RangeKeyMismatchError > ErrorDetail::range_key_mismatch() const {
  return GoType<GoPointer<roachpb::RangeKeyMismatchError > >::get(data_, 16);
}
inline GoPointer<roachpb::ReadWithinUncertaintyIntervalError > ErrorDetail::read_within_uncertainty_interval() const {
  return GoType<GoPointer<roachpb::ReadWithinUncertaintyIntervalError > >::get(data_, 24);
}
inline GoPointer<roachpb::TransactionAbortedError > ErrorDetail::transaction_aborted() const {
  return GoType<GoPointer<roachpb::TransactionAbortedError > >::get(data_, 32);
}
inline GoPointer<roachpb::TransactionPushError > ErrorDetail::transaction_push() const {
  return GoType<GoPointer<roachpb::TransactionPushError > >::get(data_, 40);
}
inline GoPointer<roachpb::TransactionRetryError > ErrorDetail::transaction_retry() const {
  return GoType<GoPointer<roachpb::TransactionRetryError > >::get(data_, 48);
}
inline GoPointer<roachpb::TransactionReplayError > ErrorDetail::transaction_replay() const {
  return GoType<GoPointer<roachpb::TransactionReplayError > >::get(data_, 56);
}
inline GoPointer<roachpb::TransactionStatusError > ErrorDetail::transaction_status() const {
  return GoType<GoPointer<roachpb::TransactionStatusError > >::get(data_, 64);
}
inline GoPointer<roachpb::WriteIntentError > ErrorDetail::write_intent() const {
  return GoType<GoPointer<roachpb::WriteIntentError > >::get(data_, 72);
}
inline GoPointer<roachpb::WriteTooOldError > ErrorDetail::write_too_old() const {
  return GoType<GoPointer<roachpb::WriteTooOldError > >::get(data_, 80);
}
inline GoPointer<roachpb::OpRequiresTxnError > ErrorDetail::op_requires_txn() const {
  return GoType<GoPointer<roachpb::OpRequiresTxnError > >::get(data_, 88);
}
inline GoPointer<roachpb::ConditionFailedError > ErrorDetail::condition_failed() const {
  return GoType<GoPointer<roachpb::ConditionFailedError > >::get(data_, 96);
}
inline GoPointer<roachpb::LeaseRejectedError > ErrorDetail::lease_rejected() const {
  return GoType<GoPointer<roachpb::LeaseRejectedError > >::get(data_, 104);
}
inline GoPointer<roachpb::NodeUnavailableError > ErrorDetail::node_unavailable() const {
  return GoType<GoPointer<roachpb::NodeUnavailableError > >::get(data_, 112);
}
inline GoPointer<roachpb::SendError > ErrorDetail::send() const {
  return GoType<GoPointer<roachpb::SendError > >::get(data_, 120);
}
inline GoPointer<roachpb::AmbiguousResultError > ErrorDetail::ambiguous_result() const {
  return GoType<GoPointer<roachpb::AmbiguousResultError > >::get(data_, 128);
}
inline GoPointer<roachpb::StoreNotFoundError > ErrorDetail::store_not_found() const {
  return GoType<GoPointer<roachpb::StoreNotFoundError > >::get(data_, 136);
}
inline GoPointer<roachpb::HandledRetryableTxnError > ErrorDetail::handled_retryable_txn_error() const {
  return GoType<GoPointer<roachpb::HandledRetryableTxnError > >::get(data_, 144);
}
inline GoPointer<roachpb::RaftGroupDeletedError > ErrorDetail::raft_group_deleted() const {
  return GoType<GoPointer<roachpb::RaftGroupDeletedError > >::get(data_, 152);
}
inline GoPointer<roachpb::ReplicaCorruptionError > ErrorDetail::replica_corruption() const {
  return GoType<GoPointer<roachpb::ReplicaCorruptionError > >::get(data_, 160);
}
inline GoPointer<roachpb::ReplicaTooOldError > ErrorDetail::replica_too_old() const {
  return GoType<GoPointer<roachpb::ReplicaTooOldError > >::get(data_, 168);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ReplicaDescriptor NotLeaseHolderError::replica() const {
  return GoType<roachpb::ReplicaDescriptor>::get(data_, 0);
}
inline GoPointer<roachpb::ReplicaDescriptor > NotLeaseHolderError::lease_holder() const {
  return GoType<GoPointer<roachpb::ReplicaDescriptor > >::get(data_, 16);
}
inline GoPointer<roachpb::Lease > NotLeaseHolderError::lease() const {
  return GoType<GoPointer<roachpb::Lease > >::get(data_, 24);
}
inline int64_t NotLeaseHolderError::range_id() const {
  return GoType<int64_t>::get(data_, 32);
}
inline GoString NotLeaseHolderError::custom_msg() const {
  return GoType<GoString>::get(data_, 40);
}
}  // namespace roachpb

namespace roachpb {
inline int64_t RangeNotFoundError::range_id() const {
  return GoType<int64_t>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<uint8_t> RangeKeyMismatchError::request_start_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 0);
}
inline GoSlice<uint8_t> RangeKeyMismatchError::request_end_key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 24);
}
inline GoPointer<roachpb::RangeDescriptor > RangeKeyMismatchError::mismatched_range() const {
  return GoType<GoPointer<roachpb::RangeDescriptor > >::get(data_, 48);
}
inline GoPointer<roachpb::RangeDescriptor > RangeKeyMismatchError::suggested_range() const {
  return GoType<GoPointer<roachpb::RangeDescriptor > >::get(data_, 56);
}
}  // namespace roachpb

namespace roachpb {
inline hlc::Timestamp ReadWithinUncertaintyIntervalError::read_timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 0);
}
inline hlc::Timestamp ReadWithinUncertaintyIntervalError::existing_timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 16);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline roachpb::Transaction TransactionPushError::pushee_txn() const {
  return GoType<roachpb::Transaction>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t TransactionRetryError::reason() const {
  return GoType<int32_t>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline GoString TransactionStatusError::msg() const {
  return GoType<GoString>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<roachpb::Intent> WriteIntentError::intents() const {
  return GoType<GoSlice<roachpb::Intent> >::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span Intent::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline enginepb::TxnMeta Intent::txn() const {
  return GoType<enginepb::TxnMeta>::get(data_, 48);
}
inline int32_t Intent::status() const {
  return GoType<int32_t>::get(data_, 128);
}
}  // namespace roachpb

namespace roachpb {
inline hlc::Timestamp WriteTooOldError::timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 0);
}
inline hlc::Timestamp WriteTooOldError::actual_timestamp() const {
  return GoType<hlc::Timestamp>::get(data_, 16);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::Value > ConditionFailedError::actual_value() const {
  return GoType<GoPointer<roachpb::Value > >::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoString LeaseRejectedError::message() const {
  return GoType<GoString>::get(data_, 0);
}
inline roachpb::Lease LeaseRejectedError::requested() const {
  return GoType<roachpb::Lease>::get(data_, 16);
}
inline roachpb::Lease LeaseRejectedError::existing() const {
  return GoType<roachpb::Lease>::get(data_, 96);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline GoString SendError::message() const {
  return GoType<GoString>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoString AmbiguousResultError::message() const {
  return GoType<GoString>::get(data_, 0);
}
inline GoPointer<roachpb::Error > AmbiguousResultError::wrapped_err() const {
  return GoType<GoPointer<roachpb::Error > >::get(data_, 16);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t StoreNotFoundError::store_id() const {
  return GoType<int32_t>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoString HandledRetryableTxnError::msg() const {
  return GoType<GoString>::get(data_, 0);
}
inline GoPointer<uuid::UUID > HandledRetryableTxnError::txn_id() const {
  return GoType<GoPointer<uuid::UUID > >::get(data_, 16);
}
inline GoPointer<roachpb::Transaction > HandledRetryableTxnError::transaction() const {
  return GoType<GoPointer<roachpb::Transaction > >::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline GoString ReplicaCorruptionError::error_msg() const {
  return GoType<GoString>::get(data_, 0);
}
inline bool ReplicaCorruptionError::processed() const {
  return GoType<bool>::get(data_, 16);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t ReplicaTooOldError::replica_id() const {
  return GoType<int32_t>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline int32_t ErrPosition::index() const {
  return GoType<int32_t>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::GetResponse > ResponseUnion::get() const {
  return GoType<GoPointer<roachpb::GetResponse > >::get(data_, 0);
}
inline GoPointer<roachpb::PutResponse > ResponseUnion::put() const {
  return GoType<GoPointer<roachpb::PutResponse > >::get(data_, 8);
}
inline GoPointer<roachpb::ConditionalPutResponse > ResponseUnion::conditional_put() const {
  return GoType<GoPointer<roachpb::ConditionalPutResponse > >::get(data_, 16);
}
inline GoPointer<roachpb::IncrementResponse > ResponseUnion::increment() const {
  return GoType<GoPointer<roachpb::IncrementResponse > >::get(data_, 24);
}
inline GoPointer<roachpb::DeleteResponse > ResponseUnion::delete_() const {
  return GoType<GoPointer<roachpb::DeleteResponse > >::get(data_, 32);
}
inline GoPointer<roachpb::DeleteRangeResponse > ResponseUnion::delete_range() const {
  return GoType<GoPointer<roachpb::DeleteRangeResponse > >::get(data_, 40);
}
inline GoPointer<roachpb::ScanResponse > ResponseUnion::scan() const {
  return GoType<GoPointer<roachpb::ScanResponse > >::get(data_, 48);
}
inline GoPointer<roachpb::BeginTransactionResponse > ResponseUnion::begin_transaction() const {
  return GoType<GoPointer<roachpb::BeginTransactionResponse > >::get(data_, 56);
}
inline GoPointer<roachpb::EndTransactionResponse > ResponseUnion::end_transaction() const {
  return GoType<GoPointer<roachpb::EndTransactionResponse > >::get(data_, 64);
}
inline GoPointer<roachpb::AdminSplitResponse > ResponseUnion::admin_split() const {
  return GoType<GoPointer<roachpb::AdminSplitResponse > >::get(data_, 72);
}
inline GoPointer<roachpb::AdminMergeResponse > ResponseUnion::admin_merge() const {
  return GoType<GoPointer<roachpb::AdminMergeResponse > >::get(data_, 80);
}
inline GoPointer<roachpb::AdminTransferLeaseResponse > ResponseUnion::admin_transfer_lease() const {
  return GoType<GoPointer<roachpb::AdminTransferLeaseResponse > >::get(data_, 88);
}
inline GoPointer<roachpb::AdminChangeReplicasResponse > ResponseUnion::admin_change_replicas() const {
  return GoType<GoPointer<roachpb::AdminChangeReplicasResponse > >::get(data_, 96);
}
inline GoPointer<roachpb::HeartbeatTxnResponse > ResponseUnion::heartbeat_txn() const {
  return GoType<GoPointer<roachpb::HeartbeatTxnResponse > >::get(data_, 104);
}
inline GoPointer<roachpb::GCResponse > ResponseUnion::gc() const {
  return GoType<GoPointer<roachpb::GCResponse > >::get(data_, 112);
}
inline GoPointer<roachpb::PushTxnResponse > ResponseUnion::push_txn() const {
  return GoType<GoPointer<roachpb::PushTxnResponse > >::get(data_, 120);
}
inline GoPointer<roachpb::RangeLookupResponse > ResponseUnion::range_lookup() const {
  return GoType<GoPointer<roachpb::RangeLookupResponse > >::get(data_, 128);
}
inline GoPointer<roachpb::ResolveIntentResponse > ResponseUnion::resolve_intent() const {
  return GoType<GoPointer<roachpb::ResolveIntentResponse > >::get(data_, 136);
}
inline GoPointer<roachpb::ResolveIntentRangeResponse > ResponseUnion::resolve_intent_range() const {
  return GoType<GoPointer<roachpb::ResolveIntentRangeResponse > >::get(data_, 144);
}
inline GoPointer<roachpb::MergeResponse > ResponseUnion::merge() const {
  return GoType<GoPointer<roachpb::MergeResponse > >::get(data_, 152);
}
inline GoPointer<roachpb::TruncateLogResponse > ResponseUnion::truncate_log() const {
  return GoType<GoPointer<roachpb::TruncateLogResponse > >::get(data_, 160);
}
inline GoPointer<roachpb::RequestLeaseResponse > ResponseUnion::request_lease() const {
  return GoType<GoPointer<roachpb::RequestLeaseResponse > >::get(data_, 168);
}
inline GoPointer<roachpb::ReverseScanResponse > ResponseUnion::reverse_scan() const {
  return GoType<GoPointer<roachpb::ReverseScanResponse > >::get(data_, 176);
}
inline GoPointer<roachpb::ComputeChecksumResponse > ResponseUnion::compute_checksum() const {
  return GoType<GoPointer<roachpb::ComputeChecksumResponse > >::get(data_, 184);
}
inline GoPointer<roachpb::DeprecatedVerifyChecksumResponse > ResponseUnion::deprecated_verify_checksum() const {
  return GoType<GoPointer<roachpb::DeprecatedVerifyChecksumResponse > >::get(data_, 192);
}
inline GoPointer<roachpb::CheckConsistencyResponse > ResponseUnion::check_consistency() const {
  return GoType<GoPointer<roachpb::CheckConsistencyResponse > >::get(data_, 200);
}
inline GoPointer<roachpb::NoopResponse > ResponseUnion::noop() const {
  return GoType<GoPointer<roachpb::NoopResponse > >::get(data_, 208);
}
inline GoPointer<roachpb::InitPutResponse > ResponseUnion::init_put() const {
  return GoType<GoPointer<roachpb::InitPutResponse > >::get(data_, 216);
}
inline GoPointer<roachpb::LeaseInfoResponse > ResponseUnion::lease_info() const {
  return GoType<GoPointer<roachpb::LeaseInfoResponse > >::get(data_, 224);
}
inline GoPointer<roachpb::WriteBatchResponse > ResponseUnion::write_batch() const {
  return GoType<GoPointer<roachpb::WriteBatchResponse > >::get(data_, 232);
}
inline GoPointer<roachpb::ExportResponse > ResponseUnion::export_() const {
  return GoType<GoPointer<roachpb::ExportResponse > >::get(data_, 240);
}
inline GoPointer<roachpb::ImportResponse > ResponseUnion::import() const {
  return GoType<GoPointer<roachpb::ImportResponse > >::get(data_, 248);
}
inline GoPointer<roachpb::QueryTxnResponse > ResponseUnion::query_txn() const {
  return GoType<GoPointer<roachpb::QueryTxnResponse > >::get(data_, 256);
}
inline GoPointer<roachpb::AdminScatterResponse > ResponseUnion::admin_scatter() const {
  return GoType<GoPointer<roachpb::AdminScatterResponse > >::get(data_, 264);
}
inline GoPointer<roachpb::AddSSTableResponse > ResponseUnion::add_sstable() const {
  return GoType<GoPointer<roachpb::AddSSTableResponse > >::get(data_, 272);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader GetResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoPointer<roachpb::Value > GetResponse::value() const {
  return GoType<GoPointer<roachpb::Value > >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline GoPointer<roachpb::Transaction > ResponseHeader::txn() const {
  return GoType<GoPointer<roachpb::Transaction > >::get(data_, 0);
}
inline GoPointer<roachpb::Span > ResponseHeader::resume_span() const {
  return GoType<GoPointer<roachpb::Span > >::get(data_, 8);
}
inline int64_t ResponseHeader::num_keys() const {
  return GoType<int64_t>::get(data_, 16);
}
inline GoSlice<roachpb::RangeInfo> ResponseHeader::range_infos() const {
  return GoType<GoSlice<roachpb::RangeInfo> >::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::RangeDescriptor RangeInfo::desc() const {
  return GoType<roachpb::RangeDescriptor>::get(data_, 0);
}
inline roachpb::Lease RangeInfo::lease() const {
  return GoType<roachpb::Lease>::get(data_, 88);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader PutResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ConditionalPutResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader IncrementResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline int64_t IncrementResponse::new_value() const {
  return GoType<int64_t>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader DeleteResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader DeleteRangeResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<GoSlice<uint8_t> > DeleteRangeResponse::keys() const {
  return GoType<GoSlice<GoSlice<uint8_t> > >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ScanResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<roachpb::KeyValue> ScanResponse::rows() const {
  return GoType<GoSlice<roachpb::KeyValue> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline GoSlice<uint8_t> KeyValue::key() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 0);
}
inline roachpb::Value KeyValue::value() const {
  return GoType<roachpb::Value>::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader BeginTransactionResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader EndTransactionResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline bool EndTransactionResponse::one_phase_commit() const {
  return GoType<bool>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AdminSplitResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AdminMergeResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AdminTransferLeaseResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AdminChangeReplicasResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader HeartbeatTxnResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader GCResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader PushTxnResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline roachpb::Transaction PushTxnResponse::pushee_txn() const {
  return GoType<roachpb::Transaction>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader RangeLookupResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<roachpb::RangeDescriptor> RangeLookupResponse::ranges() const {
  return GoType<GoSlice<roachpb::RangeDescriptor> >::get(data_, 48);
}
inline GoSlice<roachpb::RangeDescriptor> RangeLookupResponse::prefetched_ranges() const {
  return GoType<GoSlice<roachpb::RangeDescriptor> >::get(data_, 72);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ResolveIntentResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ResolveIntentRangeResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader MergeResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader TruncateLogResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader RequestLeaseResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ReverseScanResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<roachpb::KeyValue> ReverseScanResponse::rows() const {
  return GoType<GoSlice<roachpb::KeyValue> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ComputeChecksumResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader DeprecatedVerifyChecksumResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader CheckConsistencyResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader InitPutResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader LeaseInfoResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline roachpb::Lease LeaseInfoResponse::lease() const {
  return GoType<roachpb::Lease>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader WriteBatchResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ExportResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<roachpb::ExportResponse_File> ExportResponse::files() const {
  return GoType<GoSlice<roachpb::ExportResponse_File> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span ExportResponse_File::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
inline GoString ExportResponse_File::path() const {
  return GoType<GoString>::get(data_, 48);
}
inline GoSlice<uint8_t> ExportResponse_File::sha512() const {
  return GoType<GoSlice<uint8_t> >::get(data_, 64);
}
inline roachpb::BulkOpSummary ExportResponse_File::exported() const {
  return GoType<roachpb::BulkOpSummary>::get(data_, 88);
}
}  // namespace roachpb

namespace roachpb {
inline int64_t BulkOpSummary::data_size() const {
  return GoType<int64_t>::get(data_, 0);
}
inline int64_t BulkOpSummary::rows() const {
  return GoType<int64_t>::get(data_, 8);
}
inline int64_t BulkOpSummary::index_entries() const {
  return GoType<int64_t>::get(data_, 16);
}
inline int64_t BulkOpSummary::system_records() const {
  return GoType<int64_t>::get(data_, 24);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader ImportResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline roachpb::BulkOpSummary ImportResponse::imported() const {
  return GoType<roachpb::BulkOpSummary>::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader QueryTxnResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline roachpb::Transaction QueryTxnResponse::queried_txn() const {
  return GoType<roachpb::Transaction>::get(data_, 48);
}
inline GoSlice<uuid::UUID> QueryTxnResponse::waiting_txns() const {
  return GoType<GoSlice<uuid::UUID> >::get(data_, 256);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AdminScatterResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
inline GoSlice<roachpb::AdminScatterResponse_Range> AdminScatterResponse::ranges() const {
  return GoType<GoSlice<roachpb::AdminScatterResponse_Range> >::get(data_, 48);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::Span AdminScatterResponse_Range::span() const {
  return GoType<roachpb::Span>::get(data_, 0);
}
}  // namespace roachpb

namespace roachpb {
inline roachpb::ResponseHeader AddSSTableResponse::response_header() const {
  return GoType<roachpb::ResponseHeader>::get(data_, 0);
}
}  // namespace roachpb

#endif  // CPPGO_H
