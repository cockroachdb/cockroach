#ifndef CPPGO_H
#define CPPGO_H

#include <type_traits>
#include <stdint.h>
#include <rocksdb/slice.h>

namespace go {

template <typename T, typename E = void>
struct Type;

template <typename T>
struct Type<T, typename std::enable_if<std::is_class<T>::value>::type> {
  enum { Size = T::Size };
  static T get(const void *data) {
    return T(data);
  }
};

template <typename T>
struct Type<T, typename std::enable_if<std::is_arithmetic<T>::value>::type> {
  enum { Size = sizeof(T) };
  static T get(const void *data) {
    return *reinterpret_cast<const T*>(data);
  }
};

class String {
 struct Header {
   const char *data;
   int64_t size;
 };

 public:
   enum { Size = sizeof(Header) };

 public:
  String(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  std::string as_string() const { return std::string(data(), size()); }
  rocksdb::Slice as_slice() const { return rocksdb::Slice(data(), size()); }
  const char* data() const { return hdr_->data; }
  int64_t size() const { return hdr_->size; }

 private:
  const Header* hdr_;
};

template <typename T>
class Pointer {
 public:
  enum { Size = sizeof(void*) };

 public:
  Pointer(const void *data)
    : data_(data) {
  }

  const void* get() const {
    return *reinterpret_cast<const uint8_t* const*>(data_);
  }
  operator bool() const {
    return get() != nullptr;
  }
  T operator*() const {
    return Type<T>::get(get());
  }
  T operator->() const {
    return Type<T>::get(get());
  }

 private:
  const void* data_;
};

template <typename C>
class Iterator {
 public:
  typedef typename C::value_type value_type;
  typedef typename C::reference_type reference_type;
  typedef Iterator<C> self_type;

 public:
  Iterator(const C *c, int index)
    : container_(c),
      index_(index) {
  }
  Iterator(const self_type& x)
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
class Slice {
  struct Header {
    const uint8_t *data;
    int64_t size;
    int64_t cap;
  };

 public:
  enum { Size = sizeof(Header) };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef Iterator<Slice<T>> iterator;
  typedef iterator const_iterator;

 public:
  Slice(const void *data)
    : hdr_(reinterpret_cast<const Header*>(data)) {
  }

  value_type operator[](int i) const {
    return Type<T>::get(hdr_->data + i * Type<T>::Size);
  }

  int64_t size() const { return hdr_->size; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }

 private:
  const Header* hdr_;
};

template <typename T, int N>
class Array {
 public:
  enum { Size = N * Type<T>::Size };

  typedef T value_type;
  typedef value_type &reference_type;
  typedef Iterator<Array<T, N>> iterator;
  typedef iterator const_iterator;

 public:
  Array(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  value_type operator[](int i) const {
    return Type<T>::get(data_ + i * Type<T>::Size);
  }

  int64_t size() const { return N; }

  const_iterator begin() const { return const_iterator(this, 0); }
  const_iterator end() const { return const_iterator(this, size()); }

 private:
  const uint8_t *data_;
};

namespace hlc {

class Timestamp {
 public:
  enum { Size = 16 };

 public:
  Timestamp(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int64_t wall_time() const {
    return Type<int64_t>::get(data_ + 0);
  }
  int32_t logical() const {
    return Type<int32_t>::get(data_ + 8);
  }

 private:
  const uint8_t *data_;
};

} // namespace hlc

namespace uuid {

class UUID {
 public:
  enum { Size = 16 };

 public:
  UUID(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Array<uint8_t,16> uuid() const {
    return Type<Array<uint8_t,16>>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

} // namespace uuid

namespace enginepb {

class TxnMeta {
 public:
  enum { Size = 80 };

 public:
  TxnMeta(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<uuid::UUID> id() const {
    return Type<Pointer<uuid::UUID>>::get(data_ + 0);
  }
  int32_t isolation() const {
    return Type<int32_t>::get(data_ + 8);
  }
  Slice<uint8_t> key() const {
    return Type<Slice<uint8_t>>::get(data_ + 16);
  }
  uint32_t epoch() const {
    return Type<uint32_t>::get(data_ + 40);
  }
  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 48);
  }
  int32_t priority() const {
    return Type<int32_t>::get(data_ + 64);
  }
  int32_t sequence() const {
    return Type<int32_t>::get(data_ + 68);
  }
  int32_t batch_index() const {
    return Type<int32_t>::get(data_ + 72);
  }

 private:
  const uint8_t *data_;
};

} // namespace enginepb

namespace time {

class Time {
 public:
  enum { Size = 24 };

 public:
  // UNEXPORTED: wall uint64 0
  // UNEXPORTED: ext int64 8
  // UNEXPORTED: loc *time.Location 16
  Time(const void *data) {
  }
};

} // namespace time

namespace tracing {

class RecordedSpan_LogRecord_Field {
 public:
  enum { Size = 32 };

 public:
  RecordedSpan_LogRecord_Field(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String key() const {
    return Type<String>::get(data_ + 0);
  }
  String value() const {
    return Type<String>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class RecordedSpan_LogRecord {
 public:
  enum { Size = 48 };

 public:
  RecordedSpan_LogRecord(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  time::Time time() const {
    return Type<time::Time>::get(data_ + 0);
  }
  Slice<tracing::RecordedSpan_LogRecord_Field> fields() const {
    return Type<Slice<tracing::RecordedSpan_LogRecord_Field>>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class RecordedSpan {
 public:
  enum { Size = 112 };

 public:
  RecordedSpan(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  uint64_t trace_id() const {
    return Type<uint64_t>::get(data_ + 0);
  }
  uint64_t span_id() const {
    return Type<uint64_t>::get(data_ + 8);
  }
  uint64_t parent_span_id() const {
    return Type<uint64_t>::get(data_ + 16);
  }
  String operation() const {
    return Type<String>::get(data_ + 24);
  }
  // UNIMPLEMENTED: baggage map[string]string 40
  // UNIMPLEMENTED: tags map[string]string 48
  time::Time start_time() const {
    return Type<time::Time>::get(data_ + 56);
  }
  int64_t duration() const {
    return Type<int64_t>::get(data_ + 80);
  }
  Slice<tracing::RecordedSpan_LogRecord> logs() const {
    return Type<Slice<tracing::RecordedSpan_LogRecord>>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

} // namespace tracing

namespace roachpb {

class ReplicaDescriptor {
 public:
  enum { Size = 12 };

 public:
  ReplicaDescriptor(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t node_id() const {
    return Type<int32_t>::get(data_ + 0);
  }
  int32_t store_id() const {
    return Type<int32_t>::get(data_ + 4);
  }
  int32_t replica_id() const {
    return Type<int32_t>::get(data_ + 8);
  }

 private:
  const uint8_t *data_;
};

class ObservedTimestamp {
 public:
  enum { Size = 24 };

 public:
  ObservedTimestamp(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t node_id() const {
    return Type<int32_t>::get(data_ + 0);
  }
  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 8);
  }

 private:
  const uint8_t *data_;
};

class Span {
 public:
  enum { Size = 48 };

 public:
  Span(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<uint8_t> key() const {
    return Type<Slice<uint8_t>>::get(data_ + 0);
  }
  Slice<uint8_t> end_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class Transaction {
 public:
  enum { Size = 208 };

 public:
  Transaction(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  enginepb::TxnMeta txn_meta() const {
    return Type<enginepb::TxnMeta>::get(data_ + 0);
  }
  String name() const {
    return Type<String>::get(data_ + 80);
  }
  int32_t status() const {
    return Type<int32_t>::get(data_ + 96);
  }
  hlc::Timestamp last_heartbeat() const {
    return Type<hlc::Timestamp>::get(data_ + 104);
  }
  hlc::Timestamp orig_timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 120);
  }
  hlc::Timestamp max_timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 136);
  }
  Slice<roachpb::ObservedTimestamp> observed_timestamps() const {
    return Type<Slice<roachpb::ObservedTimestamp>>::get(data_ + 152);
  }
  bool writing() const {
    return Type<bool>::get(data_ + 176);
  }
  bool write_too_old() const {
    return Type<bool>::get(data_ + 177);
  }
  bool retry_on_push() const {
    return Type<bool>::get(data_ + 178);
  }
  Slice<roachpb::Span> intents() const {
    return Type<Slice<roachpb::Span>>::get(data_ + 184);
  }

 private:
  const uint8_t *data_;
};

class Header {
 public:
  enum { Size = 80 };

 public:
  Header(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 0);
  }
  roachpb::ReplicaDescriptor replica() const {
    return Type<roachpb::ReplicaDescriptor>::get(data_ + 16);
  }
  int64_t range_id() const {
    return Type<int64_t>::get(data_ + 32);
  }
  double user_priority() const {
    return Type<double>::get(data_ + 40);
  }
  Pointer<roachpb::Transaction> txn() const {
    return Type<Pointer<roachpb::Transaction>>::get(data_ + 48);
  }
  int32_t read_consistency() const {
    return Type<int32_t>::get(data_ + 56);
  }
  int64_t max_span_request_keys() const {
    return Type<int64_t>::get(data_ + 64);
  }
  bool distinct_spans() const {
    return Type<bool>::get(data_ + 72);
  }
  bool return_range_info() const {
    return Type<bool>::get(data_ + 73);
  }
  int32_t gateway_node_id() const {
    return Type<int32_t>::get(data_ + 76);
  }

 private:
  const uint8_t *data_;
};

class GetRequest {
 public:
  enum { Size = 48 };

 public:
  GetRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class Value {
 public:
  enum { Size = 40 };

 public:
  Value(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<uint8_t> raw_bytes() const {
    return Type<Slice<uint8_t>>::get(data_ + 0);
  }
  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class PutRequest {
 public:
  enum { Size = 96 };

 public:
  PutRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Value value() const {
    return Type<roachpb::Value>::get(data_ + 48);
  }
  bool inline_() const {
    return Type<bool>::get(data_ + 88);
  }
  bool blind() const {
    return Type<bool>::get(data_ + 89);
  }

 private:
  const uint8_t *data_;
};

class ConditionalPutRequest {
 public:
  enum { Size = 104 };

 public:
  ConditionalPutRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Value value() const {
    return Type<roachpb::Value>::get(data_ + 48);
  }
  Pointer<roachpb::Value> exp_value() const {
    return Type<Pointer<roachpb::Value>>::get(data_ + 88);
  }
  bool blind() const {
    return Type<bool>::get(data_ + 96);
  }

 private:
  const uint8_t *data_;
};

class IncrementRequest {
 public:
  enum { Size = 56 };

 public:
  IncrementRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  int64_t increment() const {
    return Type<int64_t>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class DeleteRequest {
 public:
  enum { Size = 48 };

 public:
  DeleteRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class DeleteRangeRequest {
 public:
  enum { Size = 56 };

 public:
  DeleteRangeRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  bool return_keys() const {
    return Type<bool>::get(data_ + 48);
  }
  bool inline_() const {
    return Type<bool>::get(data_ + 49);
  }

 private:
  const uint8_t *data_;
};

class ScanRequest {
 public:
  enum { Size = 48 };

 public:
  ScanRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class BeginTransactionRequest {
 public:
  enum { Size = 48 };

 public:
  BeginTransactionRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class RangeDescriptor {
 public:
  enum { Size = 88 };

 public:
  RangeDescriptor(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int64_t range_id() const {
    return Type<int64_t>::get(data_ + 0);
  }
  Slice<uint8_t> start_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 8);
  }
  Slice<uint8_t> end_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 32);
  }
  Slice<roachpb::ReplicaDescriptor> replicas() const {
    return Type<Slice<roachpb::ReplicaDescriptor>>::get(data_ + 56);
  }
  int32_t next_replica_id() const {
    return Type<int32_t>::get(data_ + 80);
  }

 private:
  const uint8_t *data_;
};

class SplitTrigger {
 public:
  enum { Size = 176 };

 public:
  SplitTrigger(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::RangeDescriptor left_desc() const {
    return Type<roachpb::RangeDescriptor>::get(data_ + 0);
  }
  roachpb::RangeDescriptor right_desc() const {
    return Type<roachpb::RangeDescriptor>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

class MergeTrigger {
 public:
  enum { Size = 176 };

 public:
  MergeTrigger(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::RangeDescriptor left_desc() const {
    return Type<roachpb::RangeDescriptor>::get(data_ + 0);
  }
  roachpb::RangeDescriptor right_desc() const {
    return Type<roachpb::RangeDescriptor>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

class ChangeReplicasTrigger {
 public:
  enum { Size = 48 };

 public:
  ChangeReplicasTrigger(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t change_type() const {
    return Type<int32_t>::get(data_ + 0);
  }
  roachpb::ReplicaDescriptor replica() const {
    return Type<roachpb::ReplicaDescriptor>::get(data_ + 4);
  }
  Slice<roachpb::ReplicaDescriptor> updated_replicas() const {
    return Type<Slice<roachpb::ReplicaDescriptor>>::get(data_ + 16);
  }
  int32_t next_replica_id() const {
    return Type<int32_t>::get(data_ + 40);
  }

 private:
  const uint8_t *data_;
};

class ModifiedSpanTrigger {
 public:
  enum { Size = 16 };

 public:
  ModifiedSpanTrigger(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  bool system_config_span() const {
    return Type<bool>::get(data_ + 0);
  }
  Pointer<roachpb::Span> node_liveness_span() const {
    return Type<Pointer<roachpb::Span>>::get(data_ + 8);
  }

 private:
  const uint8_t *data_;
};

class InternalCommitTrigger {
 public:
  enum { Size = 32 };

 public:
  InternalCommitTrigger(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::SplitTrigger> split_trigger() const {
    return Type<Pointer<roachpb::SplitTrigger>>::get(data_ + 0);
  }
  Pointer<roachpb::MergeTrigger> merge_trigger() const {
    return Type<Pointer<roachpb::MergeTrigger>>::get(data_ + 8);
  }
  Pointer<roachpb::ChangeReplicasTrigger> change_replicas_trigger() const {
    return Type<Pointer<roachpb::ChangeReplicasTrigger>>::get(data_ + 16);
  }
  Pointer<roachpb::ModifiedSpanTrigger> modified_span_trigger() const {
    return Type<Pointer<roachpb::ModifiedSpanTrigger>>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class EndTransactionRequest {
 public:
  enum { Size = 104 };

 public:
  EndTransactionRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  bool commit() const {
    return Type<bool>::get(data_ + 48);
  }
  Pointer<hlc::Timestamp> deadline() const {
    return Type<Pointer<hlc::Timestamp>>::get(data_ + 56);
  }
  Pointer<roachpb::InternalCommitTrigger> internal_commit_trigger() const {
    return Type<Pointer<roachpb::InternalCommitTrigger>>::get(data_ + 64);
  }
  Slice<roachpb::Span> intent_spans() const {
    return Type<Slice<roachpb::Span>>::get(data_ + 72);
  }
  bool require1_pc() const {
    return Type<bool>::get(data_ + 96);
  }

 private:
  const uint8_t *data_;
};

class AdminSplitRequest {
 public:
  enum { Size = 72 };

 public:
  AdminSplitRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  Slice<uint8_t> split_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class AdminMergeRequest {
 public:
  enum { Size = 48 };

 public:
  AdminMergeRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AdminTransferLeaseRequest {
 public:
  enum { Size = 56 };

 public:
  AdminTransferLeaseRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  int32_t target() const {
    return Type<int32_t>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class ReplicationTarget {
 public:
  enum { Size = 8 };

 public:
  ReplicationTarget(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t node_id() const {
    return Type<int32_t>::get(data_ + 0);
  }
  int32_t store_id() const {
    return Type<int32_t>::get(data_ + 4);
  }

 private:
  const uint8_t *data_;
};

class AdminChangeReplicasRequest {
 public:
  enum { Size = 80 };

 public:
  AdminChangeReplicasRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  int32_t change_type() const {
    return Type<int32_t>::get(data_ + 48);
  }
  Slice<roachpb::ReplicationTarget> targets() const {
    return Type<Slice<roachpb::ReplicationTarget>>::get(data_ + 56);
  }

 private:
  const uint8_t *data_;
};

class HeartbeatTxnRequest {
 public:
  enum { Size = 64 };

 public:
  HeartbeatTxnRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  hlc::Timestamp now() const {
    return Type<hlc::Timestamp>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class GCRequest_GCKey {
 public:
  enum { Size = 40 };

 public:
  GCRequest_GCKey(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<uint8_t> key() const {
    return Type<Slice<uint8_t>>::get(data_ + 0);
  }
  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class GCRequest {
 public:
  enum { Size = 104 };

 public:
  GCRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  Slice<roachpb::GCRequest_GCKey> keys() const {
    return Type<Slice<roachpb::GCRequest_GCKey>>::get(data_ + 48);
  }
  hlc::Timestamp threshold() const {
    return Type<hlc::Timestamp>::get(data_ + 72);
  }
  hlc::Timestamp txn_span_gcthreshold() const {
    return Type<hlc::Timestamp>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

class PushTxnRequest {
 public:
  enum { Size = 376 };

 public:
  PushTxnRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Transaction pusher_txn() const {
    return Type<roachpb::Transaction>::get(data_ + 48);
  }
  enginepb::TxnMeta pushee_txn() const {
    return Type<enginepb::TxnMeta>::get(data_ + 256);
  }
  hlc::Timestamp push_to() const {
    return Type<hlc::Timestamp>::get(data_ + 336);
  }
  hlc::Timestamp now() const {
    return Type<hlc::Timestamp>::get(data_ + 352);
  }
  int32_t push_type() const {
    return Type<int32_t>::get(data_ + 368);
  }
  bool force() const {
    return Type<bool>::get(data_ + 372);
  }

 private:
  const uint8_t *data_;
};

class RangeLookupRequest {
 public:
  enum { Size = 56 };

 public:
  RangeLookupRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  int32_t max_ranges() const {
    return Type<int32_t>::get(data_ + 48);
  }
  bool reverse() const {
    return Type<bool>::get(data_ + 52);
  }

 private:
  const uint8_t *data_;
};

class ResolveIntentRequest {
 public:
  enum { Size = 136 };

 public:
  ResolveIntentRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  enginepb::TxnMeta intent_txn() const {
    return Type<enginepb::TxnMeta>::get(data_ + 48);
  }
  int32_t status() const {
    return Type<int32_t>::get(data_ + 128);
  }
  bool poison() const {
    return Type<bool>::get(data_ + 132);
  }

 private:
  const uint8_t *data_;
};

class ResolveIntentRangeRequest {
 public:
  enum { Size = 136 };

 public:
  ResolveIntentRangeRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  enginepb::TxnMeta intent_txn() const {
    return Type<enginepb::TxnMeta>::get(data_ + 48);
  }
  int32_t status() const {
    return Type<int32_t>::get(data_ + 128);
  }
  bool poison() const {
    return Type<bool>::get(data_ + 132);
  }

 private:
  const uint8_t *data_;
};

class MergeRequest {
 public:
  enum { Size = 88 };

 public:
  MergeRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Value value() const {
    return Type<roachpb::Value>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class TruncateLogRequest {
 public:
  enum { Size = 64 };

 public:
  TruncateLogRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  uint64_t index() const {
    return Type<uint64_t>::get(data_ + 48);
  }
  int64_t range_id() const {
    return Type<int64_t>::get(data_ + 56);
  }

 private:
  const uint8_t *data_;
};

class Lease {
 public:
  enum { Size = 80 };

 public:
  Lease(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  hlc::Timestamp start() const {
    return Type<hlc::Timestamp>::get(data_ + 0);
  }
  hlc::Timestamp expiration() const {
    return Type<hlc::Timestamp>::get(data_ + 16);
  }
  roachpb::ReplicaDescriptor replica() const {
    return Type<roachpb::ReplicaDescriptor>::get(data_ + 32);
  }
  hlc::Timestamp deprecated_start_stasis() const {
    return Type<hlc::Timestamp>::get(data_ + 48);
  }
  Pointer<hlc::Timestamp> proposed_ts() const {
    return Type<Pointer<hlc::Timestamp>>::get(data_ + 64);
  }
  Pointer<int64_t> epoch() const {
    return Type<Pointer<int64_t>>::get(data_ + 72);
  }

 private:
  const uint8_t *data_;
};

class RequestLeaseRequest {
 public:
  enum { Size = 208 };

 public:
  RequestLeaseRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Lease lease() const {
    return Type<roachpb::Lease>::get(data_ + 48);
  }
  roachpb::Lease prev_lease() const {
    return Type<roachpb::Lease>::get(data_ + 128);
  }

 private:
  const uint8_t *data_;
};

class ReverseScanRequest {
 public:
  enum { Size = 48 };

 public:
  ReverseScanRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ComputeChecksumRequest {
 public:
  enum { Size = 72 };

 public:
  ComputeChecksumRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  uint32_t version() const {
    return Type<uint32_t>::get(data_ + 48);
  }
  uuid::UUID checksum_id() const {
    return Type<uuid::UUID>::get(data_ + 52);
  }
  bool snapshot() const {
    return Type<bool>::get(data_ + 68);
  }

 private:
  const uint8_t *data_;
};

class DeprecatedVerifyChecksumRequest {
 public:
  enum { Size = 48 };

 public:
  DeprecatedVerifyChecksumRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class CheckConsistencyRequest {
 public:
  enum { Size = 56 };

 public:
  CheckConsistencyRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  bool with_diff() const {
    return Type<bool>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class NoopRequest {
 public:
  enum { Size = 0 };

 public:
  NoopRequest(const void *data) {
  }
};

class InitPutRequest {
 public:
  enum { Size = 96 };

 public:
  InitPutRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Value value() const {
    return Type<roachpb::Value>::get(data_ + 48);
  }
  bool blind() const {
    return Type<bool>::get(data_ + 88);
  }
  bool fail_on_tombstones() const {
    return Type<bool>::get(data_ + 89);
  }

 private:
  const uint8_t *data_;
};

class TransferLeaseRequest {
 public:
  enum { Size = 208 };

 public:
  TransferLeaseRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Lease lease() const {
    return Type<roachpb::Lease>::get(data_ + 48);
  }
  roachpb::Lease prev_lease() const {
    return Type<roachpb::Lease>::get(data_ + 128);
  }

 private:
  const uint8_t *data_;
};

class LeaseInfoRequest {
 public:
  enum { Size = 48 };

 public:
  LeaseInfoRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class WriteBatchRequest {
 public:
  enum { Size = 120 };

 public:
  WriteBatchRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::Span data_span() const {
    return Type<roachpb::Span>::get(data_ + 48);
  }
  Slice<uint8_t> data() const {
    return Type<Slice<uint8_t>>::get(data_ + 96);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage_LocalFilePath {
 public:
  enum { Size = 16 };

 public:
  ExportStorage_LocalFilePath(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String path() const {
    return Type<String>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage_Http {
 public:
  enum { Size = 16 };

 public:
  ExportStorage_Http(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String base_uri() const {
    return Type<String>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage_GCS {
 public:
  enum { Size = 32 };

 public:
  ExportStorage_GCS(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String bucket() const {
    return Type<String>::get(data_ + 0);
  }
  String prefix() const {
    return Type<String>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage_S3 {
 public:
  enum { Size = 80 };

 public:
  ExportStorage_S3(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String bucket() const {
    return Type<String>::get(data_ + 0);
  }
  String prefix() const {
    return Type<String>::get(data_ + 16);
  }
  String access_key() const {
    return Type<String>::get(data_ + 32);
  }
  String secret() const {
    return Type<String>::get(data_ + 48);
  }
  String temp_token() const {
    return Type<String>::get(data_ + 64);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage_Azure {
 public:
  enum { Size = 64 };

 public:
  ExportStorage_Azure(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String container() const {
    return Type<String>::get(data_ + 0);
  }
  String prefix() const {
    return Type<String>::get(data_ + 16);
  }
  String account_name() const {
    return Type<String>::get(data_ + 32);
  }
  String account_key() const {
    return Type<String>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class ExportStorage {
 public:
  enum { Size = 64 };

 public:
  ExportStorage(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t provider() const {
    return Type<int32_t>::get(data_ + 0);
  }
  roachpb::ExportStorage_LocalFilePath local_file() const {
    return Type<roachpb::ExportStorage_LocalFilePath>::get(data_ + 8);
  }
  roachpb::ExportStorage_Http http_path() const {
    return Type<roachpb::ExportStorage_Http>::get(data_ + 24);
  }
  Pointer<roachpb::ExportStorage_GCS> google_cloud_config() const {
    return Type<Pointer<roachpb::ExportStorage_GCS>>::get(data_ + 40);
  }
  Pointer<roachpb::ExportStorage_S3> s3_config() const {
    return Type<Pointer<roachpb::ExportStorage_S3>>::get(data_ + 48);
  }
  Pointer<roachpb::ExportStorage_Azure> azure_config() const {
    return Type<Pointer<roachpb::ExportStorage_Azure>>::get(data_ + 56);
  }

 private:
  const uint8_t *data_;
};

class ExportRequest {
 public:
  enum { Size = 128 };

 public:
  ExportRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  roachpb::ExportStorage storage() const {
    return Type<roachpb::ExportStorage>::get(data_ + 48);
  }
  hlc::Timestamp start_time() const {
    return Type<hlc::Timestamp>::get(data_ + 112);
  }

 private:
  const uint8_t *data_;
};

class ImportRequest_File {
 public:
  enum { Size = 104 };

 public:
  ImportRequest_File(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ExportStorage dir() const {
    return Type<roachpb::ExportStorage>::get(data_ + 0);
  }
  String path() const {
    return Type<String>::get(data_ + 64);
  }
  Slice<uint8_t> sha512() const {
    return Type<Slice<uint8_t>>::get(data_ + 80);
  }

 private:
  const uint8_t *data_;
};

class ImportRequest_TableRekey {
 public:
  enum { Size = 32 };

 public:
  ImportRequest_TableRekey(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  uint32_t old_id() const {
    return Type<uint32_t>::get(data_ + 0);
  }
  Slice<uint8_t> new_desc() const {
    return Type<Slice<uint8_t>>::get(data_ + 8);
  }

 private:
  const uint8_t *data_;
};

class ImportRequest {
 public:
  enum { Size = 144 };

 public:
  ImportRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  Slice<roachpb::ImportRequest_File> files() const {
    return Type<Slice<roachpb::ImportRequest_File>>::get(data_ + 48);
  }
  roachpb::Span data_span() const {
    return Type<roachpb::Span>::get(data_ + 72);
  }
  Slice<roachpb::ImportRequest_TableRekey> rekeys() const {
    return Type<Slice<roachpb::ImportRequest_TableRekey>>::get(data_ + 120);
  }

 private:
  const uint8_t *data_;
};

class QueryTxnRequest {
 public:
  enum { Size = 160 };

 public:
  QueryTxnRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  enginepb::TxnMeta txn() const {
    return Type<enginepb::TxnMeta>::get(data_ + 48);
  }
  bool wait_for_update() const {
    return Type<bool>::get(data_ + 128);
  }
  Slice<uuid::UUID> known_waiting_txns() const {
    return Type<Slice<uuid::UUID>>::get(data_ + 136);
  }

 private:
  const uint8_t *data_;
};

class AdminScatterRequest {
 public:
  enum { Size = 48 };

 public:
  AdminScatterRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AddSSTableRequest {
 public:
  enum { Size = 72 };

 public:
  AddSSTableRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  Slice<uint8_t> data() const {
    return Type<Slice<uint8_t>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class RequestUnion {
 public:
  enum { Size = 288 };

 public:
  RequestUnion(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::GetRequest> get() const {
    return Type<Pointer<roachpb::GetRequest>>::get(data_ + 0);
  }
  Pointer<roachpb::PutRequest> put() const {
    return Type<Pointer<roachpb::PutRequest>>::get(data_ + 8);
  }
  Pointer<roachpb::ConditionalPutRequest> conditional_put() const {
    return Type<Pointer<roachpb::ConditionalPutRequest>>::get(data_ + 16);
  }
  Pointer<roachpb::IncrementRequest> increment() const {
    return Type<Pointer<roachpb::IncrementRequest>>::get(data_ + 24);
  }
  Pointer<roachpb::DeleteRequest> delete_() const {
    return Type<Pointer<roachpb::DeleteRequest>>::get(data_ + 32);
  }
  Pointer<roachpb::DeleteRangeRequest> delete_range() const {
    return Type<Pointer<roachpb::DeleteRangeRequest>>::get(data_ + 40);
  }
  Pointer<roachpb::ScanRequest> scan() const {
    return Type<Pointer<roachpb::ScanRequest>>::get(data_ + 48);
  }
  Pointer<roachpb::BeginTransactionRequest> begin_transaction() const {
    return Type<Pointer<roachpb::BeginTransactionRequest>>::get(data_ + 56);
  }
  Pointer<roachpb::EndTransactionRequest> end_transaction() const {
    return Type<Pointer<roachpb::EndTransactionRequest>>::get(data_ + 64);
  }
  Pointer<roachpb::AdminSplitRequest> admin_split() const {
    return Type<Pointer<roachpb::AdminSplitRequest>>::get(data_ + 72);
  }
  Pointer<roachpb::AdminMergeRequest> admin_merge() const {
    return Type<Pointer<roachpb::AdminMergeRequest>>::get(data_ + 80);
  }
  Pointer<roachpb::AdminTransferLeaseRequest> admin_transfer_lease() const {
    return Type<Pointer<roachpb::AdminTransferLeaseRequest>>::get(data_ + 88);
  }
  Pointer<roachpb::AdminChangeReplicasRequest> admin_change_replicas() const {
    return Type<Pointer<roachpb::AdminChangeReplicasRequest>>::get(data_ + 96);
  }
  Pointer<roachpb::HeartbeatTxnRequest> heartbeat_txn() const {
    return Type<Pointer<roachpb::HeartbeatTxnRequest>>::get(data_ + 104);
  }
  Pointer<roachpb::GCRequest> gc() const {
    return Type<Pointer<roachpb::GCRequest>>::get(data_ + 112);
  }
  Pointer<roachpb::PushTxnRequest> push_txn() const {
    return Type<Pointer<roachpb::PushTxnRequest>>::get(data_ + 120);
  }
  Pointer<roachpb::RangeLookupRequest> range_lookup() const {
    return Type<Pointer<roachpb::RangeLookupRequest>>::get(data_ + 128);
  }
  Pointer<roachpb::ResolveIntentRequest> resolve_intent() const {
    return Type<Pointer<roachpb::ResolveIntentRequest>>::get(data_ + 136);
  }
  Pointer<roachpb::ResolveIntentRangeRequest> resolve_intent_range() const {
    return Type<Pointer<roachpb::ResolveIntentRangeRequest>>::get(data_ + 144);
  }
  Pointer<roachpb::MergeRequest> merge() const {
    return Type<Pointer<roachpb::MergeRequest>>::get(data_ + 152);
  }
  Pointer<roachpb::TruncateLogRequest> truncate_log() const {
    return Type<Pointer<roachpb::TruncateLogRequest>>::get(data_ + 160);
  }
  Pointer<roachpb::RequestLeaseRequest> request_lease() const {
    return Type<Pointer<roachpb::RequestLeaseRequest>>::get(data_ + 168);
  }
  Pointer<roachpb::ReverseScanRequest> reverse_scan() const {
    return Type<Pointer<roachpb::ReverseScanRequest>>::get(data_ + 176);
  }
  Pointer<roachpb::ComputeChecksumRequest> compute_checksum() const {
    return Type<Pointer<roachpb::ComputeChecksumRequest>>::get(data_ + 184);
  }
  Pointer<roachpb::DeprecatedVerifyChecksumRequest> deprecated_verify_checksum() const {
    return Type<Pointer<roachpb::DeprecatedVerifyChecksumRequest>>::get(data_ + 192);
  }
  Pointer<roachpb::CheckConsistencyRequest> check_consistency() const {
    return Type<Pointer<roachpb::CheckConsistencyRequest>>::get(data_ + 200);
  }
  Pointer<roachpb::NoopRequest> noop() const {
    return Type<Pointer<roachpb::NoopRequest>>::get(data_ + 208);
  }
  Pointer<roachpb::InitPutRequest> init_put() const {
    return Type<Pointer<roachpb::InitPutRequest>>::get(data_ + 216);
  }
  Pointer<roachpb::TransferLeaseRequest> transfer_lease() const {
    return Type<Pointer<roachpb::TransferLeaseRequest>>::get(data_ + 224);
  }
  Pointer<roachpb::LeaseInfoRequest> lease_info() const {
    return Type<Pointer<roachpb::LeaseInfoRequest>>::get(data_ + 232);
  }
  Pointer<roachpb::WriteBatchRequest> write_batch() const {
    return Type<Pointer<roachpb::WriteBatchRequest>>::get(data_ + 240);
  }
  Pointer<roachpb::ExportRequest> export_() const {
    return Type<Pointer<roachpb::ExportRequest>>::get(data_ + 248);
  }
  Pointer<roachpb::ImportRequest> import() const {
    return Type<Pointer<roachpb::ImportRequest>>::get(data_ + 256);
  }
  Pointer<roachpb::QueryTxnRequest> query_txn() const {
    return Type<Pointer<roachpb::QueryTxnRequest>>::get(data_ + 264);
  }
  Pointer<roachpb::AdminScatterRequest> admin_scatter() const {
    return Type<Pointer<roachpb::AdminScatterRequest>>::get(data_ + 272);
  }
  Pointer<roachpb::AddSSTableRequest> add_sstable() const {
    return Type<Pointer<roachpb::AddSSTableRequest>>::get(data_ + 280);
  }

 private:
  const uint8_t *data_;
};

class BatchRequest {
 public:
  enum { Size = 104 };

 public:
  BatchRequest(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Header header() const {
    return Type<roachpb::Header>::get(data_ + 0);
  }
  Slice<roachpb::RequestUnion> requests() const {
    return Type<Slice<roachpb::RequestUnion>>::get(data_ + 80);
  }

 private:
  const uint8_t *data_;
};

class NotLeaseHolderError {
 public:
  enum { Size = 56 };

 public:
  NotLeaseHolderError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ReplicaDescriptor replica() const {
    return Type<roachpb::ReplicaDescriptor>::get(data_ + 0);
  }
  Pointer<roachpb::ReplicaDescriptor> lease_holder() const {
    return Type<Pointer<roachpb::ReplicaDescriptor>>::get(data_ + 16);
  }
  Pointer<roachpb::Lease> lease() const {
    return Type<Pointer<roachpb::Lease>>::get(data_ + 24);
  }
  int64_t range_id() const {
    return Type<int64_t>::get(data_ + 32);
  }
  String custom_msg() const {
    return Type<String>::get(data_ + 40);
  }

 private:
  const uint8_t *data_;
};

class RangeNotFoundError {
 public:
  enum { Size = 8 };

 public:
  RangeNotFoundError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int64_t range_id() const {
    return Type<int64_t>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class RangeKeyMismatchError {
 public:
  enum { Size = 64 };

 public:
  RangeKeyMismatchError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<uint8_t> request_start_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 0);
  }
  Slice<uint8_t> request_end_key() const {
    return Type<Slice<uint8_t>>::get(data_ + 24);
  }
  Pointer<roachpb::RangeDescriptor> mismatched_range() const {
    return Type<Pointer<roachpb::RangeDescriptor>>::get(data_ + 48);
  }
  Pointer<roachpb::RangeDescriptor> suggested_range() const {
    return Type<Pointer<roachpb::RangeDescriptor>>::get(data_ + 56);
  }

 private:
  const uint8_t *data_;
};

class ReadWithinUncertaintyIntervalError {
 public:
  enum { Size = 32 };

 public:
  ReadWithinUncertaintyIntervalError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  hlc::Timestamp read_timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 0);
  }
  hlc::Timestamp existing_timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class TransactionAbortedError {
 public:
  enum { Size = 0 };

 public:
  TransactionAbortedError(const void *data) {
  }
};

class TransactionPushError {
 public:
  enum { Size = 208 };

 public:
  TransactionPushError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Transaction pushee_txn() const {
    return Type<roachpb::Transaction>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class TransactionRetryError {
 public:
  enum { Size = 4 };

 public:
  TransactionRetryError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t reason() const {
    return Type<int32_t>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class TransactionReplayError {
 public:
  enum { Size = 0 };

 public:
  TransactionReplayError(const void *data) {
  }
};

class TransactionStatusError {
 public:
  enum { Size = 16 };

 public:
  TransactionStatusError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String msg() const {
    return Type<String>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class Intent {
 public:
  enum { Size = 136 };

 public:
  Intent(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  enginepb::TxnMeta txn() const {
    return Type<enginepb::TxnMeta>::get(data_ + 48);
  }
  int32_t status() const {
    return Type<int32_t>::get(data_ + 128);
  }

 private:
  const uint8_t *data_;
};

class WriteIntentError {
 public:
  enum { Size = 24 };

 public:
  WriteIntentError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<roachpb::Intent> intents() const {
    return Type<Slice<roachpb::Intent>>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class WriteTooOldError {
 public:
  enum { Size = 32 };

 public:
  WriteTooOldError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 0);
  }
  hlc::Timestamp actual_timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class OpRequiresTxnError {
 public:
  enum { Size = 0 };

 public:
  OpRequiresTxnError(const void *data) {
  }
};

class ConditionFailedError {
 public:
  enum { Size = 8 };

 public:
  ConditionFailedError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::Value> actual_value() const {
    return Type<Pointer<roachpb::Value>>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class LeaseRejectedError {
 public:
  enum { Size = 176 };

 public:
  LeaseRejectedError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String message() const {
    return Type<String>::get(data_ + 0);
  }
  roachpb::Lease requested() const {
    return Type<roachpb::Lease>::get(data_ + 16);
  }
  roachpb::Lease existing() const {
    return Type<roachpb::Lease>::get(data_ + 96);
  }

 private:
  const uint8_t *data_;
};

class NodeUnavailableError {
 public:
  enum { Size = 0 };

 public:
  NodeUnavailableError(const void *data) {
  }
};

class SendError {
 public:
  enum { Size = 16 };

 public:
  SendError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String message() const {
    return Type<String>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class Error;

class AmbiguousResultError {
 public:
  enum { Size = 24 };

 public:
  AmbiguousResultError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String message() const {
    return Type<String>::get(data_ + 0);
  }
  Pointer<roachpb::Error> wrapped_err() const {
    return Type<Pointer<roachpb::Error>>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class StoreNotFoundError {
 public:
  enum { Size = 4 };

 public:
  StoreNotFoundError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t store_id() const {
    return Type<int32_t>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class HandledRetryableTxnError {
 public:
  enum { Size = 32 };

 public:
  HandledRetryableTxnError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String msg() const {
    return Type<String>::get(data_ + 0);
  }
  Pointer<uuid::UUID> txn_id() const {
    return Type<Pointer<uuid::UUID>>::get(data_ + 16);
  }
  Pointer<roachpb::Transaction> transaction() const {
    return Type<Pointer<roachpb::Transaction>>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class RaftGroupDeletedError {
 public:
  enum { Size = 0 };

 public:
  RaftGroupDeletedError(const void *data) {
  }
};

class ReplicaCorruptionError {
 public:
  enum { Size = 24 };

 public:
  ReplicaCorruptionError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String error_msg() const {
    return Type<String>::get(data_ + 0);
  }
  bool processed() const {
    return Type<bool>::get(data_ + 16);
  }

 private:
  const uint8_t *data_;
};

class ReplicaTooOldError {
 public:
  enum { Size = 4 };

 public:
  ReplicaTooOldError(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t replica_id() const {
    return Type<int32_t>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ErrorDetail {
 public:
  enum { Size = 176 };

 public:
  ErrorDetail(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::NotLeaseHolderError> not_lease_holder() const {
    return Type<Pointer<roachpb::NotLeaseHolderError>>::get(data_ + 0);
  }
  Pointer<roachpb::RangeNotFoundError> range_not_found() const {
    return Type<Pointer<roachpb::RangeNotFoundError>>::get(data_ + 8);
  }
  Pointer<roachpb::RangeKeyMismatchError> range_key_mismatch() const {
    return Type<Pointer<roachpb::RangeKeyMismatchError>>::get(data_ + 16);
  }
  Pointer<roachpb::ReadWithinUncertaintyIntervalError> read_within_uncertainty_interval() const {
    return Type<Pointer<roachpb::ReadWithinUncertaintyIntervalError>>::get(data_ + 24);
  }
  Pointer<roachpb::TransactionAbortedError> transaction_aborted() const {
    return Type<Pointer<roachpb::TransactionAbortedError>>::get(data_ + 32);
  }
  Pointer<roachpb::TransactionPushError> transaction_push() const {
    return Type<Pointer<roachpb::TransactionPushError>>::get(data_ + 40);
  }
  Pointer<roachpb::TransactionRetryError> transaction_retry() const {
    return Type<Pointer<roachpb::TransactionRetryError>>::get(data_ + 48);
  }
  Pointer<roachpb::TransactionReplayError> transaction_replay() const {
    return Type<Pointer<roachpb::TransactionReplayError>>::get(data_ + 56);
  }
  Pointer<roachpb::TransactionStatusError> transaction_status() const {
    return Type<Pointer<roachpb::TransactionStatusError>>::get(data_ + 64);
  }
  Pointer<roachpb::WriteIntentError> write_intent() const {
    return Type<Pointer<roachpb::WriteIntentError>>::get(data_ + 72);
  }
  Pointer<roachpb::WriteTooOldError> write_too_old() const {
    return Type<Pointer<roachpb::WriteTooOldError>>::get(data_ + 80);
  }
  Pointer<roachpb::OpRequiresTxnError> op_requires_txn() const {
    return Type<Pointer<roachpb::OpRequiresTxnError>>::get(data_ + 88);
  }
  Pointer<roachpb::ConditionFailedError> condition_failed() const {
    return Type<Pointer<roachpb::ConditionFailedError>>::get(data_ + 96);
  }
  Pointer<roachpb::LeaseRejectedError> lease_rejected() const {
    return Type<Pointer<roachpb::LeaseRejectedError>>::get(data_ + 104);
  }
  Pointer<roachpb::NodeUnavailableError> node_unavailable() const {
    return Type<Pointer<roachpb::NodeUnavailableError>>::get(data_ + 112);
  }
  Pointer<roachpb::SendError> send() const {
    return Type<Pointer<roachpb::SendError>>::get(data_ + 120);
  }
  Pointer<roachpb::AmbiguousResultError> ambiguous_result() const {
    return Type<Pointer<roachpb::AmbiguousResultError>>::get(data_ + 128);
  }
  Pointer<roachpb::StoreNotFoundError> store_not_found() const {
    return Type<Pointer<roachpb::StoreNotFoundError>>::get(data_ + 136);
  }
  Pointer<roachpb::HandledRetryableTxnError> handled_retryable_txn_error() const {
    return Type<Pointer<roachpb::HandledRetryableTxnError>>::get(data_ + 144);
  }
  Pointer<roachpb::RaftGroupDeletedError> raft_group_deleted() const {
    return Type<Pointer<roachpb::RaftGroupDeletedError>>::get(data_ + 152);
  }
  Pointer<roachpb::ReplicaCorruptionError> replica_corruption() const {
    return Type<Pointer<roachpb::ReplicaCorruptionError>>::get(data_ + 160);
  }
  Pointer<roachpb::ReplicaTooOldError> replica_too_old() const {
    return Type<Pointer<roachpb::ReplicaTooOldError>>::get(data_ + 168);
  }

 private:
  const uint8_t *data_;
};

class ErrPosition {
 public:
  enum { Size = 4 };

 public:
  ErrPosition(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int32_t index() const {
    return Type<int32_t>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class Error {
 public:
  enum { Size = 72 };

 public:
  Error(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  String message() const {
    return Type<String>::get(data_ + 0);
  }
  int32_t transaction_restart() const {
    return Type<int32_t>::get(data_ + 16);
  }
  Pointer<roachpb::Transaction> unexposed_txn() const {
    return Type<Pointer<roachpb::Transaction>>::get(data_ + 24);
  }
  int32_t origin_node() const {
    return Type<int32_t>::get(data_ + 32);
  }
  Pointer<roachpb::ErrorDetail> detail() const {
    return Type<Pointer<roachpb::ErrorDetail>>::get(data_ + 40);
  }
  Pointer<roachpb::ErrPosition> index() const {
    return Type<Pointer<roachpb::ErrPosition>>::get(data_ + 48);
  }
  hlc::Timestamp now() const {
    return Type<hlc::Timestamp>::get(data_ + 56);
  }

 private:
  const uint8_t *data_;
};

class BatchResponse_Header {
 public:
  enum { Size = 72 };

 public:
  BatchResponse_Header(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::Error> error() const {
    return Type<Pointer<roachpb::Error>>::get(data_ + 0);
  }
  hlc::Timestamp timestamp() const {
    return Type<hlc::Timestamp>::get(data_ + 8);
  }
  Pointer<roachpb::Transaction> txn() const {
    return Type<Pointer<roachpb::Transaction>>::get(data_ + 24);
  }
  hlc::Timestamp now() const {
    return Type<hlc::Timestamp>::get(data_ + 32);
  }
  Slice<tracing::RecordedSpan> collected_spans() const {
    return Type<Slice<tracing::RecordedSpan>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class RangeInfo {
 public:
  enum { Size = 168 };

 public:
  RangeInfo(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::RangeDescriptor desc() const {
    return Type<roachpb::RangeDescriptor>::get(data_ + 0);
  }
  roachpb::Lease lease() const {
    return Type<roachpb::Lease>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

class ResponseHeader {
 public:
  enum { Size = 48 };

 public:
  ResponseHeader(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::Transaction> txn() const {
    return Type<Pointer<roachpb::Transaction>>::get(data_ + 0);
  }
  Pointer<roachpb::Span> resume_span() const {
    return Type<Pointer<roachpb::Span>>::get(data_ + 8);
  }
  int64_t num_keys() const {
    return Type<int64_t>::get(data_ + 16);
  }
  Slice<roachpb::RangeInfo> range_infos() const {
    return Type<Slice<roachpb::RangeInfo>>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class GetResponse {
 public:
  enum { Size = 56 };

 public:
  GetResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Pointer<roachpb::Value> value() const {
    return Type<Pointer<roachpb::Value>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class PutResponse {
 public:
  enum { Size = 48 };

 public:
  PutResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ConditionalPutResponse {
 public:
  enum { Size = 48 };

 public:
  ConditionalPutResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class IncrementResponse {
 public:
  enum { Size = 56 };

 public:
  IncrementResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  int64_t new_value() const {
    return Type<int64_t>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class DeleteResponse {
 public:
  enum { Size = 48 };

 public:
  DeleteResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class DeleteRangeResponse {
 public:
  enum { Size = 72 };

 public:
  DeleteRangeResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<Slice<uint8_t>> keys() const {
    return Type<Slice<Slice<uint8_t>>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class KeyValue {
 public:
  enum { Size = 64 };

 public:
  KeyValue(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Slice<uint8_t> key() const {
    return Type<Slice<uint8_t>>::get(data_ + 0);
  }
  roachpb::Value value() const {
    return Type<roachpb::Value>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class ScanResponse {
 public:
  enum { Size = 72 };

 public:
  ScanResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<roachpb::KeyValue> rows() const {
    return Type<Slice<roachpb::KeyValue>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class BeginTransactionResponse {
 public:
  enum { Size = 48 };

 public:
  BeginTransactionResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class EndTransactionResponse {
 public:
  enum { Size = 56 };

 public:
  EndTransactionResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  bool one_phase_commit() const {
    return Type<bool>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class AdminSplitResponse {
 public:
  enum { Size = 48 };

 public:
  AdminSplitResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AdminMergeResponse {
 public:
  enum { Size = 48 };

 public:
  AdminMergeResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AdminTransferLeaseResponse {
 public:
  enum { Size = 48 };

 public:
  AdminTransferLeaseResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AdminChangeReplicasResponse {
 public:
  enum { Size = 48 };

 public:
  AdminChangeReplicasResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class HeartbeatTxnResponse {
 public:
  enum { Size = 48 };

 public:
  HeartbeatTxnResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class GCResponse {
 public:
  enum { Size = 48 };

 public:
  GCResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class PushTxnResponse {
 public:
  enum { Size = 256 };

 public:
  PushTxnResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  roachpb::Transaction pushee_txn() const {
    return Type<roachpb::Transaction>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class RangeLookupResponse {
 public:
  enum { Size = 96 };

 public:
  RangeLookupResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<roachpb::RangeDescriptor> ranges() const {
    return Type<Slice<roachpb::RangeDescriptor>>::get(data_ + 48);
  }
  Slice<roachpb::RangeDescriptor> prefetched_ranges() const {
    return Type<Slice<roachpb::RangeDescriptor>>::get(data_ + 72);
  }

 private:
  const uint8_t *data_;
};

class ResolveIntentResponse {
 public:
  enum { Size = 48 };

 public:
  ResolveIntentResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ResolveIntentRangeResponse {
 public:
  enum { Size = 48 };

 public:
  ResolveIntentRangeResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class MergeResponse {
 public:
  enum { Size = 48 };

 public:
  MergeResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class TruncateLogResponse {
 public:
  enum { Size = 48 };

 public:
  TruncateLogResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class RequestLeaseResponse {
 public:
  enum { Size = 48 };

 public:
  RequestLeaseResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ReverseScanResponse {
 public:
  enum { Size = 72 };

 public:
  ReverseScanResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<roachpb::KeyValue> rows() const {
    return Type<Slice<roachpb::KeyValue>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class ComputeChecksumResponse {
 public:
  enum { Size = 48 };

 public:
  ComputeChecksumResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class DeprecatedVerifyChecksumResponse {
 public:
  enum { Size = 48 };

 public:
  DeprecatedVerifyChecksumResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class CheckConsistencyResponse {
 public:
  enum { Size = 48 };

 public:
  CheckConsistencyResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class NoopResponse {
 public:
  enum { Size = 0 };

 public:
  NoopResponse(const void *data) {
  }
};

class InitPutResponse {
 public:
  enum { Size = 48 };

 public:
  InitPutResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class LeaseInfoResponse {
 public:
  enum { Size = 128 };

 public:
  LeaseInfoResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  roachpb::Lease lease() const {
    return Type<roachpb::Lease>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class WriteBatchResponse {
 public:
  enum { Size = 48 };

 public:
  WriteBatchResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class BulkOpSummary {
 public:
  enum { Size = 32 };

 public:
  BulkOpSummary(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  int64_t data_size() const {
    return Type<int64_t>::get(data_ + 0);
  }
  int64_t rows() const {
    return Type<int64_t>::get(data_ + 8);
  }
  int64_t index_entries() const {
    return Type<int64_t>::get(data_ + 16);
  }
  int64_t system_records() const {
    return Type<int64_t>::get(data_ + 24);
  }

 private:
  const uint8_t *data_;
};

class ExportResponse_File {
 public:
  enum { Size = 120 };

 public:
  ExportResponse_File(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }
  String path() const {
    return Type<String>::get(data_ + 48);
  }
  Slice<uint8_t> sha512() const {
    return Type<Slice<uint8_t>>::get(data_ + 64);
  }
  roachpb::BulkOpSummary exported() const {
    return Type<roachpb::BulkOpSummary>::get(data_ + 88);
  }

 private:
  const uint8_t *data_;
};

class ExportResponse {
 public:
  enum { Size = 72 };

 public:
  ExportResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<roachpb::ExportResponse_File> files() const {
    return Type<Slice<roachpb::ExportResponse_File>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class ImportResponse {
 public:
  enum { Size = 80 };

 public:
  ImportResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  roachpb::BulkOpSummary imported() const {
    return Type<roachpb::BulkOpSummary>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class QueryTxnResponse {
 public:
  enum { Size = 280 };

 public:
  QueryTxnResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  roachpb::Transaction queried_txn() const {
    return Type<roachpb::Transaction>::get(data_ + 48);
  }
  Slice<uuid::UUID> waiting_txns() const {
    return Type<Slice<uuid::UUID>>::get(data_ + 256);
  }

 private:
  const uint8_t *data_;
};

class AdminScatterResponse_Range {
 public:
  enum { Size = 48 };

 public:
  AdminScatterResponse_Range(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::Span span() const {
    return Type<roachpb::Span>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class AdminScatterResponse {
 public:
  enum { Size = 72 };

 public:
  AdminScatterResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }
  Slice<roachpb::AdminScatterResponse_Range> ranges() const {
    return Type<Slice<roachpb::AdminScatterResponse_Range>>::get(data_ + 48);
  }

 private:
  const uint8_t *data_;
};

class AddSSTableResponse {
 public:
  enum { Size = 48 };

 public:
  AddSSTableResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::ResponseHeader response_header() const {
    return Type<roachpb::ResponseHeader>::get(data_ + 0);
  }

 private:
  const uint8_t *data_;
};

class ResponseUnion {
 public:
  enum { Size = 280 };

 public:
  ResponseUnion(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  Pointer<roachpb::GetResponse> get() const {
    return Type<Pointer<roachpb::GetResponse>>::get(data_ + 0);
  }
  Pointer<roachpb::PutResponse> put() const {
    return Type<Pointer<roachpb::PutResponse>>::get(data_ + 8);
  }
  Pointer<roachpb::ConditionalPutResponse> conditional_put() const {
    return Type<Pointer<roachpb::ConditionalPutResponse>>::get(data_ + 16);
  }
  Pointer<roachpb::IncrementResponse> increment() const {
    return Type<Pointer<roachpb::IncrementResponse>>::get(data_ + 24);
  }
  Pointer<roachpb::DeleteResponse> delete_() const {
    return Type<Pointer<roachpb::DeleteResponse>>::get(data_ + 32);
  }
  Pointer<roachpb::DeleteRangeResponse> delete_range() const {
    return Type<Pointer<roachpb::DeleteRangeResponse>>::get(data_ + 40);
  }
  Pointer<roachpb::ScanResponse> scan() const {
    return Type<Pointer<roachpb::ScanResponse>>::get(data_ + 48);
  }
  Pointer<roachpb::BeginTransactionResponse> begin_transaction() const {
    return Type<Pointer<roachpb::BeginTransactionResponse>>::get(data_ + 56);
  }
  Pointer<roachpb::EndTransactionResponse> end_transaction() const {
    return Type<Pointer<roachpb::EndTransactionResponse>>::get(data_ + 64);
  }
  Pointer<roachpb::AdminSplitResponse> admin_split() const {
    return Type<Pointer<roachpb::AdminSplitResponse>>::get(data_ + 72);
  }
  Pointer<roachpb::AdminMergeResponse> admin_merge() const {
    return Type<Pointer<roachpb::AdminMergeResponse>>::get(data_ + 80);
  }
  Pointer<roachpb::AdminTransferLeaseResponse> admin_transfer_lease() const {
    return Type<Pointer<roachpb::AdminTransferLeaseResponse>>::get(data_ + 88);
  }
  Pointer<roachpb::AdminChangeReplicasResponse> admin_change_replicas() const {
    return Type<Pointer<roachpb::AdminChangeReplicasResponse>>::get(data_ + 96);
  }
  Pointer<roachpb::HeartbeatTxnResponse> heartbeat_txn() const {
    return Type<Pointer<roachpb::HeartbeatTxnResponse>>::get(data_ + 104);
  }
  Pointer<roachpb::GCResponse> gc() const {
    return Type<Pointer<roachpb::GCResponse>>::get(data_ + 112);
  }
  Pointer<roachpb::PushTxnResponse> push_txn() const {
    return Type<Pointer<roachpb::PushTxnResponse>>::get(data_ + 120);
  }
  Pointer<roachpb::RangeLookupResponse> range_lookup() const {
    return Type<Pointer<roachpb::RangeLookupResponse>>::get(data_ + 128);
  }
  Pointer<roachpb::ResolveIntentResponse> resolve_intent() const {
    return Type<Pointer<roachpb::ResolveIntentResponse>>::get(data_ + 136);
  }
  Pointer<roachpb::ResolveIntentRangeResponse> resolve_intent_range() const {
    return Type<Pointer<roachpb::ResolveIntentRangeResponse>>::get(data_ + 144);
  }
  Pointer<roachpb::MergeResponse> merge() const {
    return Type<Pointer<roachpb::MergeResponse>>::get(data_ + 152);
  }
  Pointer<roachpb::TruncateLogResponse> truncate_log() const {
    return Type<Pointer<roachpb::TruncateLogResponse>>::get(data_ + 160);
  }
  Pointer<roachpb::RequestLeaseResponse> request_lease() const {
    return Type<Pointer<roachpb::RequestLeaseResponse>>::get(data_ + 168);
  }
  Pointer<roachpb::ReverseScanResponse> reverse_scan() const {
    return Type<Pointer<roachpb::ReverseScanResponse>>::get(data_ + 176);
  }
  Pointer<roachpb::ComputeChecksumResponse> compute_checksum() const {
    return Type<Pointer<roachpb::ComputeChecksumResponse>>::get(data_ + 184);
  }
  Pointer<roachpb::DeprecatedVerifyChecksumResponse> deprecated_verify_checksum() const {
    return Type<Pointer<roachpb::DeprecatedVerifyChecksumResponse>>::get(data_ + 192);
  }
  Pointer<roachpb::CheckConsistencyResponse> check_consistency() const {
    return Type<Pointer<roachpb::CheckConsistencyResponse>>::get(data_ + 200);
  }
  Pointer<roachpb::NoopResponse> noop() const {
    return Type<Pointer<roachpb::NoopResponse>>::get(data_ + 208);
  }
  Pointer<roachpb::InitPutResponse> init_put() const {
    return Type<Pointer<roachpb::InitPutResponse>>::get(data_ + 216);
  }
  Pointer<roachpb::LeaseInfoResponse> lease_info() const {
    return Type<Pointer<roachpb::LeaseInfoResponse>>::get(data_ + 224);
  }
  Pointer<roachpb::WriteBatchResponse> write_batch() const {
    return Type<Pointer<roachpb::WriteBatchResponse>>::get(data_ + 232);
  }
  Pointer<roachpb::ExportResponse> export_() const {
    return Type<Pointer<roachpb::ExportResponse>>::get(data_ + 240);
  }
  Pointer<roachpb::ImportResponse> import() const {
    return Type<Pointer<roachpb::ImportResponse>>::get(data_ + 248);
  }
  Pointer<roachpb::QueryTxnResponse> query_txn() const {
    return Type<Pointer<roachpb::QueryTxnResponse>>::get(data_ + 256);
  }
  Pointer<roachpb::AdminScatterResponse> admin_scatter() const {
    return Type<Pointer<roachpb::AdminScatterResponse>>::get(data_ + 264);
  }
  Pointer<roachpb::AddSSTableResponse> add_sstable() const {
    return Type<Pointer<roachpb::AddSSTableResponse>>::get(data_ + 272);
  }

 private:
  const uint8_t *data_;
};

class BatchResponse {
 public:
  enum { Size = 96 };

 public:
  BatchResponse(const void *data)
    : data_(reinterpret_cast<const uint8_t*>(data)) {
  }

  roachpb::BatchResponse_Header batch_response__header() const {
    return Type<roachpb::BatchResponse_Header>::get(data_ + 0);
  }
  Slice<roachpb::ResponseUnion> responses() const {
    return Type<Slice<roachpb::ResponseUnion>>::get(data_ + 72);
  }

 private:
  const uint8_t *data_;
};

} // namespace roachpb

} // namespace go

#endif  // CPPGO_H
