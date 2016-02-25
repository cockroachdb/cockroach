// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#include <map>
#include <memory>
#ifndef _SHARED_PTR_H
#include <google/protobuf/stubs/shared_ptr.h>
#endif
#include <sstream>

#include <google/protobuf/stubs/casts.h>
#include <google/protobuf/stubs/logging.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/scoped_ptr.h>
#include <google/protobuf/stubs/stringprintf.h>
#include <google/protobuf/testing/file.h>
#include <google/protobuf/arena_test_util.h>
#include <google/protobuf/map_proto2_unittest.pb.h>
#include <google/protobuf/map_unittest.pb.h>
#include <google/protobuf/map_test_util.h>
#include <google/protobuf/test_util.h>
#include <google/protobuf/unittest.pb.h>
#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/map.h>
#include <google/protobuf/map_field_inl.h>
#include <google/protobuf/message.h>
#include <google/protobuf/reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/wire_format.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/tokenizer.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/stubs/strutil.h>
#include <google/protobuf/stubs/substitute.h>
#include <google/protobuf/testing/googletest.h>
#include <gtest/gtest.h>

namespace google {

using google::protobuf::unittest::ForeignMessage;
using google::protobuf::unittest::TestAllTypes;
using google::protobuf::unittest::TestMap;
using google::protobuf::unittest::TestRecursiveMapMessage;

namespace protobuf {
namespace internal {

// Map API Test =====================================================

class MapImplTest : public ::testing::Test {
 protected:
  MapImplTest()
      : map_ptr_(new Map<int32, int32>),
        map_(*map_ptr_),
        const_map_(*map_ptr_) {
    EXPECT_TRUE(map_.empty());
    EXPECT_EQ(0, map_.size());
  }
  ~MapImplTest() {}

  void ExpectSingleElement(int32 key, int32 value) {
    EXPECT_FALSE(map_.empty());
    EXPECT_EQ(1, map_.size());
    ExpectElement(key, value);
  }

  void ExpectElements(const std::map<int32, int32>& map) {
    EXPECT_FALSE(map_.empty());
    EXPECT_EQ(map.size(), map_.size());
    for (std::map<int32, int32>::const_iterator it = map.begin();
         it != map.end(); ++it) {
      ExpectElement(it->first, it->second);
    }
  }

  void ExpectElement(int32 key, int32 value) {
    // Test map size is correct.
    EXPECT_EQ(value, map_[key]);
    EXPECT_EQ(1, map_.count(key));

    // Check mutable at and find work correctly.
    EXPECT_EQ(value, map_.at(key));
    Map<int32, int32>::iterator it = map_.find(key);

    // interator dereferenceable
    EXPECT_EQ(key,   (*it).first);
    EXPECT_EQ(value, (*it).second);
    EXPECT_EQ(key,   it->first);
    EXPECT_EQ(value, it->second);

    // iterator mutable
    ((*it).second) = value + 1;
    EXPECT_EQ(value + 1, map_[key]);
    ((*it).second) = value;
    EXPECT_EQ(value, map_[key]);

    it->second = value + 1;
    EXPECT_EQ(value + 1, map_[key]);
    it->second = value;
    EXPECT_EQ(value, map_[key]);

    // copy constructor
    Map<int32, int32>::iterator it_copy = it;
    EXPECT_EQ(key, it_copy->first);
    EXPECT_EQ(value, it_copy->second);

    // Immutable API ================================================

    // Check immutable at and find work correctly.
    EXPECT_EQ(value, const_map_.at(key));
    Map<int32, int32>::const_iterator const_it = const_map_.find(key);

    // interator dereferenceable
    EXPECT_EQ(key, (*const_it).first);
    EXPECT_EQ(value, (*const_it).second);
    EXPECT_EQ(key, const_it->first);
    EXPECT_EQ(value, const_it->second);

    // copy constructor
    Map<int32, int32>::const_iterator const_it_copy = const_it;
    EXPECT_EQ(key, const_it_copy->first);
    EXPECT_EQ(value, const_it_copy->second);
  }

  google::protobuf::scoped_ptr<Map<int32, int32> > map_ptr_;
  Map<int32, int32>& map_;
  const Map<int32, int32>& const_map_;
};

TEST_F(MapImplTest, OperatorBracket) {
  int32 key = 0;
  int32 value1 = 100;
  int32 value2 = 101;

  EXPECT_EQ(0, map_[key]);

  map_[key] = value1;
  ExpectSingleElement(key, value1);

  map_[key] = value2;
  ExpectSingleElement(key, value2);
}

TEST_F(MapImplTest, OperatorBracketNonExist) {
  int32 key = 0;
  int32 default_value = 0;

  EXPECT_EQ(default_value, map_[key]);
  ExpectSingleElement(key, default_value);
}

TEST_F(MapImplTest, MutableAt) {
  int32 key = 0;
  int32 value1 = 100;
  int32 value2 = 101;

  map_[key] = value1;
  ExpectSingleElement(key, value1);

  map_.at(key) = value2;
  ExpectSingleElement(key, value2);
}

#ifdef PROTOBUF_HAS_DEATH_TEST

TEST_F(MapImplTest, MutableAtNonExistDeathTest) {
  EXPECT_DEATH(map_.at(0), "");
}

TEST_F(MapImplTest, ImmutableAtNonExistDeathTest) {
  EXPECT_DEATH(const_map_.at(0), "");
}

TEST_F(MapImplTest, UsageErrors) {
  MapKey key;
  key.SetInt64Value(1);
  EXPECT_DEATH(key.GetUInt64Value(),
               "Protocol Buffer map usage error:\n"
               "MapKey::GetUInt64Value type does not match\n"
               "  Expected : uint64\n"
               "  Actual   : int64");

  MapValueRef value;
  EXPECT_DEATH(value.SetFloatValue(0.1),
               "Protocol Buffer map usage error:\n"
               "MapValueRef::type MapValueRef is not initialized.");
}

#endif  // PROTOBUF_HAS_DEATH_TEST

TEST_F(MapImplTest, CountNonExist) {
  EXPECT_EQ(0, map_.count(0));
}

TEST_F(MapImplTest, MutableFindNonExist) {
  EXPECT_TRUE(map_.end() == map_.find(0));
}

TEST_F(MapImplTest, ImmutableFindNonExist) {
  EXPECT_TRUE(const_map_.end() == const_map_.find(0));
}

TEST_F(MapImplTest, ConstEnd) {
  EXPECT_TRUE(const_map_.end() == const_map_.cend());
}

TEST_F(MapImplTest, GetReferenceFromIterator) {
  for (int i = 0; i < 10; i++) {
    map_[i] = i;
  }

  for (Map<int32, int32>::const_iterator it = map_.cbegin();
       it != map_.cend();) {
    Map<int32, int32>::const_reference entry = *it++;
    EXPECT_EQ(entry.first, entry.second);
  }

  for (Map<int32, int32>::const_iterator it = const_map_.begin();
       it != const_map_.end();) {
    Map<int32, int32>::const_reference entry = *it++;
    EXPECT_EQ(entry.first, entry.second);
  }

  for (Map<int32, int32>::iterator it = map_.begin(); it != map_.end();) {
    Map<int32, int32>::reference entry = *it++;
    EXPECT_EQ(entry.first + 1, ++entry.second);
  }
}

TEST_F(MapImplTest, IteratorBasic) {
  map_[0] = 0;

  // Default constructible (per forward iterator requirements).
  Map<int, int>::const_iterator cit;
  Map<int, int>::iterator it;

  it = map_.begin();
  cit = it;  // Converts to const_iterator

  // Can compare between them.
  EXPECT_TRUE(it == cit);
  EXPECT_FALSE(cit != it);

  // Pre increment.
  EXPECT_FALSE(it == ++cit);

  // Post increment.
  EXPECT_FALSE(it++ == cit);
  EXPECT_TRUE(it == cit);
}

template <typename T>
bool IsConstHelper(T& /*t*/) {  // NOLINT. We want to catch non-const refs here.
  return false;
}
template <typename T>
bool IsConstHelper(const T& /*t*/) {
  return true;
}

TEST_F(MapImplTest, IteratorConstness) {
  map_[0] = 0;
  EXPECT_TRUE(IsConstHelper(*map_.cbegin()));
  EXPECT_TRUE(IsConstHelper(*const_map_.begin()));
  EXPECT_FALSE(IsConstHelper(*map_.begin()));
}

bool IsForwardIteratorHelper(std::forward_iterator_tag /*tag*/) { return true; }
template <typename T>
bool IsForwardIteratorHelper(T /*t*/) {
  return false;
}

TEST_F(MapImplTest, IteratorCategory) {
  EXPECT_TRUE(IsForwardIteratorHelper(
      std::iterator_traits<Map<int, int>::iterator>::iterator_category()));
  EXPECT_TRUE(IsForwardIteratorHelper(std::iterator_traits<
      Map<int, int>::const_iterator>::iterator_category()));
}

TEST_F(MapImplTest, InsertSingle) {
  int32 key = 0;
  int32 value1 = 100;
  int32 value2 = 101;

  // Insert a non-existed key.
  std::pair<Map<int32, int32>::iterator, bool> result1 =
      map_.insert(Map<int32, int32>::value_type(key, value1));
  ExpectSingleElement(key, value1);

  Map<int32, int32>::iterator it1 = result1.first;
  EXPECT_EQ(key, it1->first);
  EXPECT_EQ(value1, it1->second);
  EXPECT_TRUE(result1.second);

  // Insert an existed key.
  std::pair<Map<int32, int32>::iterator, bool> result2 =
      map_.insert(Map<int32, int32>::value_type(key, value2));
  ExpectSingleElement(key, value1);

  Map<int32, int32>::iterator it2 = result2.first;
  EXPECT_TRUE(it1 == it2);
  EXPECT_FALSE(result2.second);
}

TEST_F(MapImplTest, InsertByIterator) {
  int32 key1 = 0;
  int32 key2 = 1;
  int32 value1a = 100;
  int32 value1b = 101;
  int32 value2a = 200;
  int32 value2b = 201;

  std::map<int32, int32> map1;
  map1[key1] = value1a;
  map1[key2] = value2a;

  map_.insert(map1.begin(), map1.end());
  ExpectElements(map1);

  std::map<int32, int32> map2;
  map2[key1] = value1b;
  map2[key2] = value2b;

  map_.insert(map2.begin(), map2.end());
  ExpectElements(map1);
}

TEST_F(MapImplTest, EraseSingleByKey) {
  int32 key = 0;
  int32 value = 100;

  map_[key] = value;
  ExpectSingleElement(key, value);

  // Erase an existing key.
  EXPECT_EQ(1, map_.erase(key));
  EXPECT_TRUE(map_.empty());
  EXPECT_EQ(0, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(key));
  EXPECT_TRUE(map_.begin() == map_.end());

  // Erase a non-existing key.
  EXPECT_EQ(0, map_.erase(key));
}

TEST_F(MapImplTest, EraseMutipleByKey) {
  // erase in one specific order to trigger corner cases
  for (int i = 0; i < 5; i++) {
    map_[i] = i;
  }

  map_.erase(0);
  EXPECT_EQ(4, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(0));

  map_.erase(1);
  EXPECT_EQ(3, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(1));

  map_.erase(3);
  EXPECT_EQ(2, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(3));

  map_.erase(4);
  EXPECT_EQ(1, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(4));

  map_.erase(2);
  EXPECT_EQ(0, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(2));
}

TEST_F(MapImplTest, EraseSingleByIterator) {
  int32 key = 0;
  int32 value = 100;

  map_[key] = value;
  ExpectSingleElement(key, value);

  Map<int32, int32>::iterator it = map_.find(key);
  map_.erase(it);
  EXPECT_TRUE(map_.empty());
  EXPECT_EQ(0, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(key));
  EXPECT_TRUE(map_.begin() == map_.end());
}

TEST_F(MapImplTest, ValidIteratorAfterErase) {
  for (int i = 0; i < 10; i++) {
    map_[i] = i;
  }

  int count = 0;

  for (Map<int32, int32>::iterator it = map_.begin(); it != map_.end();) {
    count++;
    if (it->first % 2 == 1) {
      map_.erase(it++);
    } else {
      ++it;
    }
  }

  EXPECT_EQ(10, count);
  EXPECT_EQ(5, map_.size());
}

TEST_F(MapImplTest, EraseByIterator) {
  int32 key1 = 0;
  int32 key2 = 1;
  int32 value1 = 100;
  int32 value2 = 101;

  std::map<int32, int32> map;
  map[key1] = value1;
  map[key2] = value2;

  map_.insert(map.begin(), map.end());
  ExpectElements(map);

  map_.erase(map_.begin(), map_.end());
  EXPECT_TRUE(map_.empty());
  EXPECT_EQ(0, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(key1));
  EXPECT_TRUE(map_.end() == map_.find(key2));
  EXPECT_TRUE(map_.begin() == map_.end());
}

TEST_F(MapImplTest, Clear) {
  int32 key = 0;
  int32 value = 100;

  map_[key] = value;
  ExpectSingleElement(key, value);

  map_.clear();

  EXPECT_TRUE(map_.empty());
  EXPECT_EQ(0, map_.size());
  EXPECT_TRUE(map_.end() == map_.find(key));
  EXPECT_TRUE(map_.begin() == map_.end());
}

TEST_F(MapImplTest, CopyConstructor) {
  int32 key1 = 0;
  int32 key2 = 1;
  int32 value1 = 100;
  int32 value2 = 101;

  std::map<int32, int32> map;
  map[key1] = value1;
  map[key2] = value2;

  map_.insert(map.begin(), map.end());

  Map<int32, int32> other(map_);

  EXPECT_EQ(2, other.size());
  EXPECT_EQ(value1, other.at(key1));
  EXPECT_EQ(value2, other.at(key2));
}

TEST_F(MapImplTest, IterConstructor) {
  int32 key1 = 0;
  int32 key2 = 1;
  int32 value1 = 100;
  int32 value2 = 101;

  std::map<int32, int32> map;
  map[key1] = value1;
  map[key2] = value2;

  Map<int32, int32> new_map(map.begin(), map.end());

  EXPECT_EQ(2, new_map.size());
  EXPECT_EQ(value1, new_map.at(key1));
  EXPECT_EQ(value2, new_map.at(key2));
}

TEST_F(MapImplTest, Assigner) {
  int32 key1 = 0;
  int32 key2 = 1;
  int32 value1 = 100;
  int32 value2 = 101;

  std::map<int32, int32> map;
  map[key1] = value1;
  map[key2] = value2;

  map_.insert(map.begin(), map.end());

  Map<int32, int32> other;
  int32 key_other = 123;
  int32 value_other = 321;
  other[key_other] = value_other;
  EXPECT_EQ(1, other.size());

  other = map_;

  EXPECT_EQ(2, other.size());
  EXPECT_EQ(value1, other.at(key1));
  EXPECT_EQ(value2, other.at(key2));
  EXPECT_TRUE(other.find(key_other) == other.end());

  // Self assign
  other = other;
  EXPECT_EQ(2, other.size());
  EXPECT_EQ(value1, other.at(key1));
  EXPECT_EQ(value2, other.at(key2));
}

TEST_F(MapImplTest, Rehash) {
  const int test_size = 50;
  std::map<int32, int32> reference_map;
  for (int i = 0; i < test_size; i++) {
    reference_map[i] = i;
  }
  for (int i = 0; i < test_size; i++) {
    map_[i] = reference_map[i];
    EXPECT_EQ(reference_map[i], map_[i]);
  }
  for (int i = 0; i < test_size; i++) {
    map_.erase(i);
    EXPECT_TRUE(map_.end() == map_.find(i));
  }
  EXPECT_TRUE(map_.empty());
}

TEST_F(MapImplTest, EqualRange) {
  int key = 100, key_missing = 101;
  map_[key] = 100;

  std::pair<google::protobuf::Map<int32, int32>::iterator,
            google::protobuf::Map<int32, int32>::iterator> range = map_.equal_range(key);
  EXPECT_TRUE(map_.find(key) == range.first);
  EXPECT_TRUE(++map_.find(key) == range.second);

  range = map_.equal_range(key_missing);
  EXPECT_TRUE(map_.end() == range.first);
  EXPECT_TRUE(map_.end() == range.second);

  std::pair<google::protobuf::Map<int32, int32>::const_iterator,
            google::protobuf::Map<int32, int32>::const_iterator> const_range =
      const_map_.equal_range(key);
  EXPECT_TRUE(const_map_.find(key) == const_range.first);
  EXPECT_TRUE(++const_map_.find(key) == const_range.second);

  const_range = const_map_.equal_range(key_missing);
  EXPECT_TRUE(const_map_.end() == const_range.first);
  EXPECT_TRUE(const_map_.end() == const_range.second);
}

TEST_F(MapImplTest, ConvertToStdMap) {
  map_[100] = 101;
  std::map<int32, int32> std_map(map_.begin(), map_.end());
  EXPECT_EQ(1, std_map.size());
  EXPECT_EQ(101, std_map[100]);
}

TEST_F(MapImplTest, ConvertToStdVectorOfPairs) {
  map_[100] = 101;
  std::vector<std::pair<int32, int32> > std_vec(map_.begin(), map_.end());
  EXPECT_EQ(1, std_vec.size());
  EXPECT_EQ(100, std_vec[0].first);
  EXPECT_EQ(101, std_vec[0].second);
}

// Map Field Reflection Test ========================================

static int Func(int i, int j) {
  return i * j;
}

static string StrFunc(int i, int j) {
  string str;
  SStringPrintf(&str, "%d", Func(i, j));
  return str;
}

static int Int(const string& value) {
  int result = 0;
  std::istringstream(value) >> result;
  return result;
}

class MapFieldReflectionTest : public testing::Test {
 protected:
  typedef FieldDescriptor FD;
};

TEST_F(MapFieldReflectionTest, RegularFields) {
  TestMap message;
  const Reflection* refl = message.GetReflection();
  const Descriptor* desc = message.GetDescriptor();

  Map<int32, int32>* map_int32_int32 = message.mutable_map_int32_int32();
  Map<int32, double>* map_int32_double = message.mutable_map_int32_double();
  Map<string, string>* map_string_string = message.mutable_map_string_string();
  Map<int32, ForeignMessage>* map_int32_foreign_message =
      message.mutable_map_int32_foreign_message();

  for (int i = 0; i < 10; ++i) {
    (*map_int32_int32)[i] = Func(i, 1);
    (*map_int32_double)[i] = Func(i, 2);
    (*map_string_string)[StrFunc(i, 1)] = StrFunc(i, 5);
    (*map_int32_foreign_message)[i].set_c(Func(i, 6));
  }

  // Get FieldDescriptors for all the fields of interest.
  const FieldDescriptor* fd_map_int32_int32 =
      desc->FindFieldByName("map_int32_int32");
  const FieldDescriptor* fd_map_int32_double =
      desc->FindFieldByName("map_int32_double");
  const FieldDescriptor* fd_map_string_string =
      desc->FindFieldByName("map_string_string");
  const FieldDescriptor* fd_map_int32_foreign_message =
      desc->FindFieldByName("map_int32_foreign_message");

  const FieldDescriptor* fd_map_int32_in32_key =
      fd_map_int32_int32->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_in32_value =
      fd_map_int32_int32->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_int32_double_key =
      fd_map_int32_double->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_double_value =
      fd_map_int32_double->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_string_string_key =
      fd_map_string_string->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_string_string_value =
      fd_map_string_string->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_int32_foreign_message_key =
      fd_map_int32_foreign_message->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_foreign_message_value =
      fd_map_int32_foreign_message->message_type()->FindFieldByName("value");

  // Get RepeatedPtrField objects for all fields of interest.
  const RepeatedPtrField<Message>& mf_int32_int32 =
      refl->GetRepeatedPtrField<Message>(message, fd_map_int32_int32);
  const RepeatedPtrField<Message>& mf_int32_double =
      refl->GetRepeatedPtrField<Message>(message, fd_map_int32_double);
  const RepeatedPtrField<Message>& mf_string_string =
      refl->GetRepeatedPtrField<Message>(message, fd_map_string_string);
  const RepeatedPtrField<Message>&
      mf_int32_foreign_message =
          refl->GetRepeatedPtrField<Message>(
              message, fd_map_int32_foreign_message);

  // Get mutable RepeatedPtrField objects for all fields of interest.
  RepeatedPtrField<Message>* mmf_int32_int32 =
      refl->MutableRepeatedPtrField<Message>(&message, fd_map_int32_int32);
  RepeatedPtrField<Message>* mmf_int32_double =
      refl->MutableRepeatedPtrField<Message>(&message, fd_map_int32_double);
  RepeatedPtrField<Message>* mmf_string_string =
      refl->MutableRepeatedPtrField<Message>(&message, fd_map_string_string);
  RepeatedPtrField<Message>* mmf_int32_foreign_message =
      refl->MutableRepeatedPtrField<Message>(
          &message, fd_map_int32_foreign_message);

  // Make sure we can do gets through the RepeatedPtrField objects.
  for (int i = 0; i < 10; ++i) {
    {
      // Check gets through const objects.
      const Message& message_int32_int32 = mf_int32_int32.Get(i);
      int32 key_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_key);
      int32 value_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_value);
      EXPECT_EQ(value_int32_int32, Func(key_int32_int32, 1));

      const Message& message_int32_double = mf_int32_double.Get(i);
      int32 key_int32_double = message_int32_double.GetReflection()->GetInt32(
          message_int32_double, fd_map_int32_double_key);
      double value_int32_double =
          message_int32_double.GetReflection()->GetDouble(
              message_int32_double, fd_map_int32_double_value);
      EXPECT_EQ(value_int32_double, Func(key_int32_double, 2));

      const Message& message_string_string = mf_string_string.Get(i);
      string key_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_key);
      string value_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_value);
      EXPECT_EQ(value_string_string, StrFunc(Int(key_string_string), 5));

      const Message& message_int32_message = mf_int32_foreign_message.Get(i);
      int32 key_int32_message = message_int32_message.GetReflection()->GetInt32(
          message_int32_message, fd_map_int32_foreign_message_key);
      const ForeignMessage& value_int32_message =
          down_cast<const ForeignMessage&>(
              message_int32_message.GetReflection()
                  ->GetMessage(message_int32_message,
                               fd_map_int32_foreign_message_value));
      EXPECT_EQ(value_int32_message.c(), Func(key_int32_message, 6));
    }

    {
      // Check gets through mutable objects.
      const Message& message_int32_int32 = mmf_int32_int32->Get(i);
      int32 key_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_key);
      int32 value_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_value);
      EXPECT_EQ(value_int32_int32, Func(key_int32_int32, 1));

      const Message& message_int32_double = mmf_int32_double->Get(i);
      int32 key_int32_double = message_int32_double.GetReflection()->GetInt32(
          message_int32_double, fd_map_int32_double_key);
      double value_int32_double =
          message_int32_double.GetReflection()->GetDouble(
              message_int32_double, fd_map_int32_double_value);
      EXPECT_EQ(value_int32_double, Func(key_int32_double, 2));

      const Message& message_string_string = mmf_string_string->Get(i);
      string key_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_key);
      string value_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_value);
      EXPECT_EQ(value_string_string, StrFunc(Int(key_string_string), 5));

      const Message& message_int32_message = mmf_int32_foreign_message->Get(i);
      int32 key_int32_message = message_int32_message.GetReflection()->GetInt32(
          message_int32_message, fd_map_int32_foreign_message_key);
      const ForeignMessage& value_int32_message =
          down_cast<const ForeignMessage&>(
              message_int32_message.GetReflection()
                  ->GetMessage(message_int32_message,
                               fd_map_int32_foreign_message_value));
      EXPECT_EQ(value_int32_message.c(), Func(key_int32_message, 6));
    }
  }

  // Do sets through the RepeatedPtrField objects.
  for (int i = 0; i < 10; i++) {
    {
      Message* message_int32_int32 = mmf_int32_int32->Mutable(i);
      int32 key_int32_int32 = message_int32_int32->GetReflection()->GetInt32(
          *message_int32_int32, fd_map_int32_in32_key);
      message_int32_int32->GetReflection()->SetInt32(message_int32_int32,
                                                     fd_map_int32_in32_value,
                                                     Func(key_int32_int32, -1));

      Message* message_int32_double = mmf_int32_double->Mutable(i);
      int32 key_int32_double = message_int32_double->GetReflection()->GetInt32(
          *message_int32_double, fd_map_int32_double_key);
      message_int32_double->GetReflection()->SetDouble(
          message_int32_double, fd_map_int32_double_value,
          Func(key_int32_double, -2));

      Message* message_string_string = mmf_string_string->Mutable(i);
      string key_string_string =
          message_string_string->GetReflection()->GetString(
              *message_string_string, fd_map_string_string_key);
      message_string_string->GetReflection()->SetString(
          message_string_string, fd_map_string_string_value,
          StrFunc(Int(key_string_string), -5));

      Message* message_int32_message = mmf_int32_foreign_message->Mutable(i);
      int32 key_int32_message =
          message_int32_message->GetReflection()->GetInt32(
              *message_int32_message, fd_map_int32_foreign_message_key);
      ForeignMessage* value_int32_message = down_cast<ForeignMessage*>(
          message_int32_message->GetReflection()
              ->MutableMessage(message_int32_message,
                               fd_map_int32_foreign_message_value));
      value_int32_message->set_c(Func(key_int32_message, -6));
    }
  }

  // Check gets through mutable objects.
  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(Func(i, -1), message.map_int32_int32().at(i));
    EXPECT_EQ(Func(i, -2), message.map_int32_double().at(i));
    EXPECT_EQ(StrFunc(i, -5), message.map_string_string().at(StrFunc(i, 1)));
    EXPECT_EQ(Func(i, -6), message.map_int32_foreign_message().at(i).c());
  }
}

TEST_F(MapFieldReflectionTest, RepeatedFieldRefForRegularFields) {
  TestMap message;
  const Reflection* refl = message.GetReflection();
  const Descriptor* desc = message.GetDescriptor();

  Map<int32, int32>* map_int32_int32 = message.mutable_map_int32_int32();
  Map<int32, double>* map_int32_double = message.mutable_map_int32_double();
  Map<string, string>* map_string_string = message.mutable_map_string_string();
  Map<int32, ForeignMessage>* map_int32_foreign_message =
      message.mutable_map_int32_foreign_message();

  for (int i = 0; i < 10; ++i) {
    (*map_int32_int32)[i] = Func(i, 1);
    (*map_int32_double)[i] = Func(i, 2);
    (*map_string_string)[StrFunc(i, 1)] = StrFunc(i, 5);
    (*map_int32_foreign_message)[i].set_c(Func(i, 6));
  }

  // Get FieldDescriptors for all the fields of interest.
  const FieldDescriptor* fd_map_int32_int32 =
      desc->FindFieldByName("map_int32_int32");
  const FieldDescriptor* fd_map_int32_double =
      desc->FindFieldByName("map_int32_double");
  const FieldDescriptor* fd_map_string_string =
      desc->FindFieldByName("map_string_string");
  const FieldDescriptor* fd_map_int32_foreign_message =
      desc->FindFieldByName("map_int32_foreign_message");

  const FieldDescriptor* fd_map_int32_in32_key =
      fd_map_int32_int32->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_in32_value =
      fd_map_int32_int32->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_int32_double_key =
      fd_map_int32_double->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_double_value =
      fd_map_int32_double->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_string_string_key =
      fd_map_string_string->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_string_string_value =
      fd_map_string_string->message_type()->FindFieldByName("value");
  const FieldDescriptor* fd_map_int32_foreign_message_key =
      fd_map_int32_foreign_message->message_type()->FindFieldByName("key");
  const FieldDescriptor* fd_map_int32_foreign_message_value =
      fd_map_int32_foreign_message->message_type()->FindFieldByName("value");

  // Get RepeatedFieldRef objects for all fields of interest.
  const RepeatedFieldRef<Message> mf_int32_int32 =
      refl->GetRepeatedFieldRef<Message>(message, fd_map_int32_int32);
  const RepeatedFieldRef<Message> mf_int32_double =
      refl->GetRepeatedFieldRef<Message>(message, fd_map_int32_double);
  const RepeatedFieldRef<Message> mf_string_string =
      refl->GetRepeatedFieldRef<Message>(message, fd_map_string_string);
  const RepeatedFieldRef<Message> mf_int32_foreign_message =
      refl->GetRepeatedFieldRef<Message>(message, fd_map_int32_foreign_message);

  // Get mutable RepeatedFieldRef objects for all fields of interest.
  const MutableRepeatedFieldRef<Message> mmf_int32_int32 =
      refl->GetMutableRepeatedFieldRef<Message>(&message, fd_map_int32_int32);
  const MutableRepeatedFieldRef<Message> mmf_int32_double =
      refl->GetMutableRepeatedFieldRef<Message>(&message, fd_map_int32_double);
  const MutableRepeatedFieldRef<Message> mmf_string_string =
      refl->GetMutableRepeatedFieldRef<Message>(&message, fd_map_string_string);
  const MutableRepeatedFieldRef<Message>
      mmf_int32_foreign_message =
          refl->GetMutableRepeatedFieldRef<Message>(
              &message, fd_map_int32_foreign_message);

  // Get entry default instances
  google::protobuf::scoped_ptr<Message> entry_int32_int32(
      MessageFactory::generated_factory()
          ->GetPrototype(fd_map_int32_int32->message_type())
          ->New());
  google::protobuf::scoped_ptr<Message> entry_int32_double(
      MessageFactory::generated_factory()
          ->GetPrototype(fd_map_int32_double->message_type())
          ->New());
  google::protobuf::scoped_ptr<Message> entry_string_string(
      MessageFactory::generated_factory()
          ->GetPrototype(fd_map_string_string->message_type())
          ->New());
  google::protobuf::scoped_ptr<Message> entry_int32_foreign_message(
      MessageFactory::generated_factory()
          ->GetPrototype(fd_map_int32_foreign_message->message_type())
          ->New());

  EXPECT_EQ(10, mf_int32_int32.size());
  EXPECT_EQ(10, mmf_int32_int32.size());
  EXPECT_EQ(10, mf_int32_double.size());
  EXPECT_EQ(10, mmf_int32_double.size());
  EXPECT_EQ(10, mf_string_string.size());
  EXPECT_EQ(10, mmf_string_string.size());
  EXPECT_EQ(10, mf_int32_foreign_message.size());
  EXPECT_EQ(10, mmf_int32_foreign_message.size());

  EXPECT_FALSE(mf_int32_int32.empty());
  EXPECT_FALSE(mmf_int32_int32.empty());
  EXPECT_FALSE(mf_int32_double.empty());
  EXPECT_FALSE(mmf_int32_double.empty());
  EXPECT_FALSE(mf_string_string.empty());
  EXPECT_FALSE(mmf_string_string.empty());
  EXPECT_FALSE(mf_int32_foreign_message.empty());
  EXPECT_FALSE(mmf_int32_foreign_message.empty());

  // Make sure we can do gets through the RepeatedFieldRef objects.
  for (int i = 0; i < 10; ++i) {
    {
      // Check gets through const objects.
      const Message& message_int32_int32 =
          mf_int32_int32.Get(i, entry_int32_int32.get());
      int32 key_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_key);
      int32 value_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_value);
      EXPECT_EQ(value_int32_int32, Func(key_int32_int32, 1));

      const Message& message_int32_double =
          mf_int32_double.Get(i, entry_int32_double.get());
      int32 key_int32_double = message_int32_double.GetReflection()->GetInt32(
          message_int32_double, fd_map_int32_double_key);
      double value_int32_double =
          message_int32_double.GetReflection()->GetDouble(
              message_int32_double, fd_map_int32_double_value);
      EXPECT_EQ(value_int32_double, Func(key_int32_double, 2));

      const Message& message_string_string =
          mf_string_string.Get(i, entry_string_string.get());
      string key_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_key);
      string value_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_value);
      EXPECT_EQ(value_string_string, StrFunc(Int(key_string_string), 5));

      const Message& message_int32_message =
          mf_int32_foreign_message.Get(i, entry_int32_foreign_message.get());
      int32 key_int32_message = message_int32_message.GetReflection()->GetInt32(
          message_int32_message, fd_map_int32_foreign_message_key);
      const ForeignMessage& value_int32_message =
          down_cast<const ForeignMessage&>(
              message_int32_message.GetReflection()
                  ->GetMessage(message_int32_message,
                               fd_map_int32_foreign_message_value));
      EXPECT_EQ(value_int32_message.c(), Func(key_int32_message, 6));
    }

    {
      // Check gets through mutable objects.
      const Message& message_int32_int32 =
          mmf_int32_int32.Get(i, entry_int32_int32.get());
      int32 key_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_key);
      int32 value_int32_int32 = message_int32_int32.GetReflection()->GetInt32(
          message_int32_int32, fd_map_int32_in32_value);
      EXPECT_EQ(value_int32_int32, Func(key_int32_int32, 1));

      const Message& message_int32_double =
          mmf_int32_double.Get(i, entry_int32_double.get());
      int32 key_int32_double = message_int32_double.GetReflection()->GetInt32(
          message_int32_double, fd_map_int32_double_key);
      double value_int32_double =
          message_int32_double.GetReflection()->GetDouble(
              message_int32_double, fd_map_int32_double_value);
      EXPECT_EQ(value_int32_double, Func(key_int32_double, 2));

      const Message& message_string_string =
          mmf_string_string.Get(i, entry_string_string.get());
      string key_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_key);
      string value_string_string =
          message_string_string.GetReflection()->GetString(
              message_string_string, fd_map_string_string_value);
      EXPECT_EQ(value_string_string, StrFunc(Int(key_string_string), 5));

      const Message& message_int32_message =
          mmf_int32_foreign_message.Get(i, entry_int32_foreign_message.get());
      int32 key_int32_message = message_int32_message.GetReflection()->GetInt32(
          message_int32_message, fd_map_int32_foreign_message_key);
      const ForeignMessage& value_int32_message =
          down_cast<const ForeignMessage&>(
              message_int32_message.GetReflection()
                  ->GetMessage(message_int32_message,
                               fd_map_int32_foreign_message_value));
      EXPECT_EQ(value_int32_message.c(), Func(key_int32_message, 6));
    }
  }

  // Make sure we can do sets through the RepeatedFieldRef objects.
  for (int i = 0; i < 10; i++) {
    const Message& message_int32_int32 =
        mmf_int32_int32.Get(i, entry_int32_int32.get());
    int key = message_int32_int32.GetReflection()->GetInt32(
        message_int32_int32, fd_map_int32_in32_key);

    entry_int32_int32->GetReflection()->SetInt32(
        entry_int32_int32.get(), fd_map_int32_int32->message_type()->field(0),
        key);
    entry_int32_int32->GetReflection()->SetInt32(
        entry_int32_int32.get(), fd_map_int32_int32->message_type()->field(1),
        Func(key, -1));
    entry_int32_double->GetReflection()->SetInt32(
        entry_int32_double.get(), fd_map_int32_double->message_type()->field(0),
        key);
    entry_int32_double->GetReflection()->SetDouble(
        entry_int32_double.get(), fd_map_int32_double->message_type()->field(1),
        Func(key, -2));
    entry_string_string->GetReflection()->SetString(
        entry_string_string.get(),
        fd_map_string_string->message_type()->field(0), StrFunc(key, 1));
    entry_string_string->GetReflection()->SetString(
        entry_string_string.get(),
        fd_map_string_string->message_type()->field(1), StrFunc(key, -5));
    entry_int32_foreign_message->GetReflection()->SetInt32(
        entry_int32_foreign_message.get(),
        fd_map_int32_foreign_message->message_type()->field(0), key);
    Message* value_message =
        entry_int32_foreign_message->GetReflection()->MutableMessage(
            entry_int32_foreign_message.get(),
            fd_map_int32_foreign_message->message_type()->field(1));
    value_message->GetReflection()->SetInt32(
        value_message, value_message->GetDescriptor()->FindFieldByName("c"),
        Func(key, -6));

    mmf_int32_int32.Set(i, *entry_int32_int32);
    mmf_int32_double.Set(i, *entry_int32_double);
    mmf_string_string.Set(i, *entry_string_string);
    mmf_int32_foreign_message.Set(i, *entry_int32_foreign_message);
  }

  for (int i = 0; i < 10; i++) {
    EXPECT_EQ(Func(i, -1), message.map_int32_int32().at(i));
    EXPECT_EQ(Func(i, -2), message.map_int32_double().at(i));
    EXPECT_EQ(StrFunc(i, -5), message.map_string_string().at(StrFunc(i, 1)));
    EXPECT_EQ(Func(i, -6), message.map_int32_foreign_message().at(i).c());
  }

  // Test iterators.
  {
    int index = 0;
    hash_map<int32, int32> result;
    for (RepeatedFieldRef<Message>::iterator it = mf_int32_int32.begin();
         it != mf_int32_int32.end(); ++it) {
      const Message& message = *it;
      int32 key =
          message.GetReflection()->GetInt32(message, fd_map_int32_in32_key);
      int32 value =
          message.GetReflection()->GetInt32(message, fd_map_int32_in32_value);
      result[key] = value;
      ++index;
    }
    EXPECT_EQ(10, index);
    for (hash_map<int32, int32>::const_iterator it = result.begin();
         it != result.end(); ++it) {
      EXPECT_EQ(message.map_int32_int32().at(it->first), it->second);
    }
  }

  {
    int index = 0;
    hash_map<int32, double> result;
    for (RepeatedFieldRef<Message>::iterator it = mf_int32_double.begin();
         it != mf_int32_double.end(); ++it) {
      const Message& message = *it;
      int32 key =
          message.GetReflection()->GetInt32(message, fd_map_int32_double_key);
      double value = message.GetReflection()->GetDouble(
          message, fd_map_int32_double_value);
      result[key] = value;
      ++index;
    }
    EXPECT_EQ(10, index);
    for (hash_map<int32, double>::const_iterator it = result.begin();
         it != result.end(); ++it) {
      EXPECT_EQ(message.map_int32_double().at(it->first), it->second);
    }
  }

  {
    int index = 0;
    hash_map<string, string> result;
    for (RepeatedFieldRef<Message>::iterator it = mf_string_string.begin();
         it != mf_string_string.end(); ++it) {
      const Message& message = *it;
      string key =
          message.GetReflection()->GetString(message, fd_map_string_string_key);
      string value = message.GetReflection()->GetString(
          message, fd_map_string_string_value);
      result[key] = value;
      ++index;
    }
    EXPECT_EQ(10, index);
    for (hash_map<string, string>::const_iterator it = result.begin();
         it != result.end(); ++it) {
      EXPECT_EQ(message.map_string_string().at(it->first), it->second);
    }
  }

  {
    int index = 0;
    std::map<int32, ForeignMessage> result;
    for (RepeatedFieldRef<Message>::iterator it =
             mf_int32_foreign_message.begin();
         it != mf_int32_foreign_message.end(); ++it) {
      const Message& message = *it;
      int32 key = message.GetReflection()->GetInt32(
          message, fd_map_int32_foreign_message_key);
      const ForeignMessage& sub_message = down_cast<const ForeignMessage&>(
          message.GetReflection()
              ->GetMessage(message, fd_map_int32_foreign_message_value));
      result[key].MergeFrom(sub_message);
      ++index;
    }
    EXPECT_EQ(10, index);
    for (std::map<int32, ForeignMessage>::const_iterator it = result.begin();
         it != result.end(); ++it) {
      EXPECT_EQ(message.map_int32_foreign_message().at(it->first).c(),
                it->second.c());
    }
  }

  // Test MutableRepeatedFieldRef::Add()
  entry_int32_int32->GetReflection()->SetInt32(
      entry_int32_int32.get(), fd_map_int32_int32->message_type()->field(0),
      4321);
  entry_int32_int32->GetReflection()->SetInt32(
      entry_int32_int32.get(), fd_map_int32_int32->message_type()->field(1),
      1234);
  mmf_int32_int32.Add(*entry_int32_int32);
  EXPECT_EQ(1234, message.map_int32_int32().at(4321));

  entry_int32_double->GetReflection()->SetInt32(
      entry_int32_double.get(), fd_map_int32_double->message_type()->field(0),
      4321);
  entry_int32_double->GetReflection()->SetDouble(
      entry_int32_double.get(), fd_map_int32_double->message_type()->field(1),
      1234.0);
  mmf_int32_double.Add(*entry_int32_double);
  EXPECT_EQ(1234.0, message.map_int32_double().at(4321));

  entry_string_string->GetReflection()->SetString(
      entry_string_string.get(),
      fd_map_string_string->message_type()->field(0), "4321");
  entry_string_string->GetReflection()->SetString(
      entry_string_string.get(), fd_map_string_string->message_type()->field(1),
      "1234");
  mmf_string_string.Add(*entry_string_string);
  EXPECT_EQ("1234", message.map_string_string().at("4321"));

  entry_int32_foreign_message->GetReflection()->SetInt32(
      entry_int32_foreign_message.get(),
      fd_map_int32_foreign_message->message_type()->field(0), 4321);
  Message* value_message =
      entry_int32_foreign_message->GetReflection()->MutableMessage(
          entry_int32_foreign_message.get(),
          fd_map_int32_foreign_message->message_type()->field(1));
  ForeignMessage foreign_message;
  foreign_message.set_c(1234);
  value_message->CopyFrom(foreign_message);

  mmf_int32_foreign_message.Add(*entry_int32_foreign_message);
  EXPECT_EQ(1234, message.map_int32_foreign_message().at(4321).c());

  // Test Reflection::AddAllocatedMessage
  Message* free_entry_string_string = MessageFactory::generated_factory()
      ->GetPrototype(fd_map_string_string->message_type())
      ->New();
  entry_string_string->GetReflection()->SetString(
      free_entry_string_string,
      fd_map_string_string->message_type()->field(0), "4321");
  entry_string_string->GetReflection()->SetString(
      free_entry_string_string, fd_map_string_string->message_type()->field(1),
      "1234");
  refl->AddAllocatedMessage(&message, fd_map_string_string,
                            free_entry_string_string);

  // Test MutableRepeatedFieldRef::RemoveLast()
  mmf_int32_int32.RemoveLast();
  mmf_int32_double.RemoveLast();
  mmf_string_string.RemoveLast();
  mmf_int32_foreign_message.RemoveLast();
  EXPECT_EQ(10, message.map_int32_int32().size());
  EXPECT_EQ(10, message.map_int32_double().size());
  EXPECT_EQ(11, message.map_string_string().size());
  EXPECT_EQ(10, message.map_int32_foreign_message().size());

  // Test MutableRepeatedFieldRef::SwapElements()
  {
    const Message& message0a = mmf_int32_int32.Get(0, entry_int32_int32.get());
    int32 int32_value0a =
        message0a.GetReflection()->GetInt32(message0a, fd_map_int32_in32_value);
    const Message& message9a = mmf_int32_int32.Get(9, entry_int32_int32.get());
    int32 int32_value9a =
        message9a.GetReflection()->GetInt32(message9a, fd_map_int32_in32_value);

    mmf_int32_int32.SwapElements(0, 9);

    const Message& message0b = mmf_int32_int32.Get(0, entry_int32_int32.get());
    int32 int32_value0b =
        message0b.GetReflection()->GetInt32(message0b, fd_map_int32_in32_value);
    const Message& message9b = mmf_int32_int32.Get(9, entry_int32_int32.get());
    int32 int32_value9b =
        message9b.GetReflection()->GetInt32(message9b, fd_map_int32_in32_value);

    EXPECT_EQ(int32_value9a, int32_value0b);
    EXPECT_EQ(int32_value0a, int32_value9b);
  }

  {
    const Message& message0a =
        mmf_int32_double.Get(0, entry_int32_double.get());
    double double_value0a = message0a.GetReflection()->GetDouble(
        message0a, fd_map_int32_double_value);
    const Message& message9a =
        mmf_int32_double.Get(9, entry_int32_double.get());
    double double_value9a = message9a.GetReflection()->GetDouble(
        message9a, fd_map_int32_double_value);

    mmf_int32_double.SwapElements(0, 9);

    const Message& message0b =
        mmf_int32_double.Get(0, entry_int32_double.get());
    double double_value0b = message0b.GetReflection()->GetDouble(
        message0b, fd_map_int32_double_value);
    const Message& message9b =
        mmf_int32_double.Get(9, entry_int32_double.get());
    double double_value9b = message9b.GetReflection()->GetDouble(
        message9b, fd_map_int32_double_value);

    EXPECT_EQ(double_value9a, double_value0b);
    EXPECT_EQ(double_value0a, double_value9b);
  }

  {
    const Message& message0a =
        mmf_string_string.Get(0, entry_string_string.get());
    string string_value0a = message0a.GetReflection()->GetString(
        message0a, fd_map_string_string_value);
    const Message& message9a =
        mmf_string_string.Get(9, entry_string_string.get());
    string string_value9a = message9a.GetReflection()->GetString(
        message9a, fd_map_string_string_value);

    mmf_string_string.SwapElements(0, 9);

    const Message& message0b =
        mmf_string_string.Get(0, entry_string_string.get());
    string string_value0b = message0b.GetReflection()->GetString(
        message0b, fd_map_string_string_value);
    const Message& message9b =
        mmf_string_string.Get(9, entry_string_string.get());
    string string_value9b = message9b.GetReflection()->GetString(
        message9b, fd_map_string_string_value);

    EXPECT_EQ(string_value9a, string_value0b);
    EXPECT_EQ(string_value0a, string_value9b);
  }

  {
    const Message& message0a =
        mmf_int32_foreign_message.Get(0, entry_int32_foreign_message.get());
    const ForeignMessage& sub_message0a = down_cast<const ForeignMessage&>(
        message0a.GetReflection()
            ->GetMessage(message0a, fd_map_int32_foreign_message_value));
    int32 int32_value0a = sub_message0a.c();
    const Message& message9a =
        mmf_int32_foreign_message.Get(9, entry_int32_foreign_message.get());
    const ForeignMessage& sub_message9a = down_cast<const ForeignMessage&>(
        message9a.GetReflection()
            ->GetMessage(message9a, fd_map_int32_foreign_message_value));
    int32 int32_value9a = sub_message9a.c();

    mmf_int32_foreign_message.SwapElements(0, 9);

    const Message& message0b =
        mmf_int32_foreign_message.Get(0, entry_int32_foreign_message.get());
    const ForeignMessage& sub_message0b = down_cast<const ForeignMessage&>(
        message0b.GetReflection()
            ->GetMessage(message0b, fd_map_int32_foreign_message_value));
    int32 int32_value0b = sub_message0b.c();
    const Message& message9b =
        mmf_int32_foreign_message.Get(9, entry_int32_foreign_message.get());
    const ForeignMessage& sub_message9b = down_cast<const ForeignMessage&>(
        message9b.GetReflection()
            ->GetMessage(message9b, fd_map_int32_foreign_message_value));
    int32 int32_value9b = sub_message9b.c();

    EXPECT_EQ(int32_value9a, int32_value0b);
    EXPECT_EQ(int32_value0a, int32_value9b);
  }
}

TEST_F(MapFieldReflectionTest, RepeatedFieldRefMergeFromAndSwap) {
  // Set-up message content.
  TestMap m0, m1, m2;
  for (int i = 0; i < 10; ++i) {
    (*m0.mutable_map_int32_int32())[i] = Func(i, 1);
    (*m0.mutable_map_int32_double())[i] = Func(i, 2);
    (*m0.mutable_map_string_string())[StrFunc(i, 1)] = StrFunc(i, 5);
    (*m0.mutable_map_int32_foreign_message())[i].set_c(Func(i, 6));
    (*m1.mutable_map_int32_int32())[i + 10] = Func(i, 11);
    (*m1.mutable_map_int32_double())[i + 10] = Func(i, 12);
    (*m1.mutable_map_string_string())[StrFunc(i + 10, 1)] = StrFunc(i, 15);
    (*m1.mutable_map_int32_foreign_message())[i + 10].set_c(Func(i, 16));
    (*m2.mutable_map_int32_int32())[i + 20] = Func(i, 21);
    (*m2.mutable_map_int32_double())[i + 20] = Func(i, 22);
    (*m2.mutable_map_string_string())[StrFunc(i + 20, 1)] = StrFunc(i, 25);
    (*m2.mutable_map_int32_foreign_message())[i + 20].set_c(Func(i, 26));
  }

  const Reflection* refl = m0.GetReflection();
  const Descriptor* desc = m0.GetDescriptor();

  // Get FieldDescriptors for all the fields of interest.
  const FieldDescriptor* fd_map_int32_int32 =
      desc->FindFieldByName("map_int32_int32");
  const FieldDescriptor* fd_map_int32_double =
      desc->FindFieldByName("map_int32_double");
  const FieldDescriptor* fd_map_string_string =
      desc->FindFieldByName("map_string_string");
  const FieldDescriptor* fd_map_int32_foreign_message =
      desc->FindFieldByName("map_int32_foreign_message");

    // Get MutableRepeatedFieldRef objects for all fields of interest.
  const MutableRepeatedFieldRef<Message> mmf_int32_int32 =
      refl->GetMutableRepeatedFieldRef<Message>(
          &m0, fd_map_int32_int32);
  const MutableRepeatedFieldRef<Message> mmf_int32_double =
      refl->GetMutableRepeatedFieldRef<Message>(
          &m0, fd_map_int32_double);
  const MutableRepeatedFieldRef<Message> mmf_string_string =
      refl->GetMutableRepeatedFieldRef<Message>(
          &m0, fd_map_string_string);
  const MutableRepeatedFieldRef<Message>
      mmf_int32_foreign_message =
          refl->GetMutableRepeatedFieldRef<Message>(
              &m0, fd_map_int32_foreign_message);

  // Test MutableRepeatedRef::CopyFrom
  mmf_int32_int32.CopyFrom(
      refl->GetRepeatedFieldRef<Message>(
          m1, fd_map_int32_int32));
  mmf_int32_double.CopyFrom(
      refl->GetRepeatedFieldRef<Message>(
          m1, fd_map_int32_double));
  mmf_string_string.CopyFrom(
      refl->GetRepeatedFieldRef<Message>(
          m1, fd_map_string_string));
  mmf_int32_foreign_message.CopyFrom(
      refl->GetRepeatedFieldRef<Message>(
          m1, fd_map_int32_foreign_message));

  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Func(i, 11), m0.map_int32_int32().at(i + 10));
    EXPECT_EQ(Func(i, 12), m0.map_int32_double().at(i + 10));
    EXPECT_EQ(StrFunc(i, 15), m0.map_string_string().at(StrFunc(i + 10, 1)));
    EXPECT_EQ(Func(i, 16), m0.map_int32_foreign_message().at(i + 10).c());
  }

  // Test MutableRepeatedRef::MergeFrom
  mmf_int32_int32.MergeFrom(
      refl->GetRepeatedFieldRef<Message>(
          m2, fd_map_int32_int32));
  mmf_int32_double.MergeFrom(
      refl->GetRepeatedFieldRef<Message>(
          m2, fd_map_int32_double));
  mmf_string_string.MergeFrom(
      refl->GetRepeatedFieldRef<Message>(
          m2, fd_map_string_string));
  mmf_int32_foreign_message.MergeFrom(
      refl->GetRepeatedFieldRef<Message>(
          m2, fd_map_int32_foreign_message));
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(Func(i, 21), m0.map_int32_int32().at(i + 20));
    EXPECT_EQ(Func(i, 22), m0.map_int32_double().at(i + 20));
    EXPECT_EQ(StrFunc(i, 25), m0.map_string_string().at(StrFunc(i + 20, 1)));
    EXPECT_EQ(Func(i, 26), m0.map_int32_foreign_message().at(i + 20).c());
  }

  // Test MutableRepeatedRef::Swap
  // Swap between m0 and m2.
  mmf_int32_int32.Swap(
      refl->GetMutableRepeatedFieldRef<Message>(
          &m2, fd_map_int32_int32));
  mmf_int32_double.Swap(
      refl->GetMutableRepeatedFieldRef<Message>(
          &m2, fd_map_int32_double));
  mmf_string_string.Swap(
      refl->GetMutableRepeatedFieldRef<Message>(
          &m2, fd_map_string_string));
  mmf_int32_foreign_message.Swap(
      refl->GetMutableRepeatedFieldRef<Message>(
          &m2, fd_map_int32_foreign_message));
  for (int i = 0; i < 10; ++i) {
    // Check the content of m0.
    EXPECT_EQ(Func(i, 21), m0.map_int32_int32().at(i + 20));
    EXPECT_EQ(Func(i, 22), m0.map_int32_double().at(i + 20));
    EXPECT_EQ(StrFunc(i, 25), m0.map_string_string().at(StrFunc(i + 20, 1)));
    EXPECT_EQ(Func(i, 26), m0.map_int32_foreign_message().at(i + 20).c());

    // Check the content of m2.
    EXPECT_EQ(Func(i, 11), m2.map_int32_int32().at(i + 10));
    EXPECT_EQ(Func(i, 12), m2.map_int32_double().at(i + 10));
    EXPECT_EQ(StrFunc(i, 15), m2.map_string_string().at(StrFunc(i + 10, 1)));
    EXPECT_EQ(Func(i, 16), m2.map_int32_foreign_message().at(i + 10).c());
    EXPECT_EQ(Func(i, 21), m2.map_int32_int32().at(i + 20));
    EXPECT_EQ(Func(i, 22), m2.map_int32_double().at(i + 20));
    EXPECT_EQ(StrFunc(i, 25), m2.map_string_string().at(StrFunc(i + 20, 1)));
    EXPECT_EQ(Func(i, 26), m2.map_int32_foreign_message().at(i + 20).c());
  }

  // TODO(teboring): add test for duplicated key
}

// Generated Message Test ===========================================

TEST(GeneratedMapFieldTest, Accessors) {
  unittest::TestMap message;

  MapTestUtil::SetMapFields(&message);
  MapTestUtil::ExpectMapFieldsSet(message);

  MapTestUtil::ModifyMapFields(&message);
  MapTestUtil::ExpectMapFieldsModified(message);
}

TEST(GeneratedMapFieldTest, SetMapFieldsInitialized) {
  unittest::TestMap message;

  MapTestUtil::SetMapFieldsInitialized(&message);
  MapTestUtil::ExpectMapFieldsSetInitialized(message);
}

TEST(GeneratedMapFieldTest, Proto2SetMapFieldsInitialized) {
  unittest::TestEnumMap message;
  EXPECT_EQ(unittest::PROTO2_MAP_ENUM_FOO,
            (*message.mutable_known_map_field())[0]);
}

TEST(GeneratedMapFieldTest, Clear) {
  unittest::TestMap message;

  MapTestUtil::SetMapFields(&message);
  message.Clear();
  MapTestUtil::ExpectClear(message);
}

TEST(GeneratedMapFieldTest, ClearMessageMap) {
  unittest::TestMessageMap message;

  // Creates a TestAllTypes with default value
  TestUtil::ExpectClear((*message.mutable_map_int32_message())[0]);
}

TEST(GeneratedMapFieldTest, CopyFrom) {
  unittest::TestMap message1, message2;

  MapTestUtil::SetMapFields(&message1);
  message2.CopyFrom(message1);
  MapTestUtil::ExpectMapFieldsSet(message2);

  // Copying from self should be a no-op.
  message2.CopyFrom(message2);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldTest, CopyFromMessageMap) {
  unittest::TestMessageMap message1, message2;

  (*message1.mutable_map_int32_message())[0].add_repeated_int32(100);
  (*message2.mutable_map_int32_message())[0].add_repeated_int32(101);

  message1.CopyFrom(message2);

  // Checks repeated field is overwritten.
  EXPECT_EQ(1, message1.map_int32_message().at(0).repeated_int32_size());
  EXPECT_EQ(101, message1.map_int32_message().at(0).repeated_int32(0));
}

TEST(GeneratedMapFieldTest, SwapWithEmpty) {
  unittest::TestMap message1, message2;

  MapTestUtil::SetMapFields(&message1);
  MapTestUtil::ExpectMapFieldsSet(message1);
  MapTestUtil::ExpectClear(message2);

  message1.Swap(&message2);
  MapTestUtil::ExpectMapFieldsSet(message2);
  MapTestUtil::ExpectClear(message1);
}

TEST(GeneratedMapFieldTest, SwapWithSelf) {
  unittest::TestMap message;

  MapTestUtil::SetMapFields(&message);
  MapTestUtil::ExpectMapFieldsSet(message);

  message.Swap(&message);
  MapTestUtil::ExpectMapFieldsSet(message);
}

TEST(GeneratedMapFieldTest, SwapWithOther) {
  unittest::TestMap message1, message2;

  MapTestUtil::SetMapFields(&message1);
  MapTestUtil::SetMapFields(&message2);
  MapTestUtil::ModifyMapFields(&message2);

  message1.Swap(&message2);
  MapTestUtil::ExpectMapFieldsModified(message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldTest, CopyConstructor) {
  unittest::TestMap message1;
  MapTestUtil::SetMapFields(&message1);

  unittest::TestMap message2(message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldTest, CopyAssignmentOperator) {
  unittest::TestMap message1;
  MapTestUtil::SetMapFields(&message1);

  unittest::TestMap message2;
  message2 = message1;
  MapTestUtil::ExpectMapFieldsSet(message2);

  // Make sure that self-assignment does something sane.
  message2.operator=(message2);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

#if !defined(PROTOBUF_TEST_NO_DESCRIPTORS) || \
        !defined(GOOGLE_PROTOBUF_NO_RTTI)
TEST(GeneratedMapFieldTest, UpcastCopyFrom) {
  // Test the CopyFrom method that takes in the generic const Message&
  // parameter.
  unittest::TestMap message1, message2;

  MapTestUtil::SetMapFields(&message1);

  const Message* source = implicit_cast<const Message*>(&message1);
  message2.CopyFrom(*source);

  MapTestUtil::ExpectMapFieldsSet(message2);
}
#endif

#ifndef PROTOBUF_TEST_NO_DESCRIPTORS

TEST(GeneratedMapFieldTest, CopyFromDynamicMessage) {
  // Test copying from a DynamicMessage, which must fall back to using
  // reflection.
  unittest::TestMap message2;

  // Construct a new version of the dynamic message via the factory.
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<Message> message1;
  message1.reset(
      factory.GetPrototype(unittest::TestMap::descriptor())->New());
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  reflection_tester.SetMapFieldsViaReflection(message1.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message1);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(message1.get());
  message2.CopyFrom(*message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldTest, CopyFromDynamicMessageMapReflection) {
  unittest::TestMap message2;

  // Construct a new version of the dynamic message via the factory.
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<Message> message1;
  message1.reset(
      factory.GetPrototype(unittest::TestMap::descriptor())->New());
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  reflection_tester.SetMapFieldsViaMapReflection(message1.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message1);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(message1.get());
  message2.CopyFrom(*message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldTest, DynamicMessageCopyFrom) {
  // Test copying to a DynamicMessage, which must fall back to using reflection.
  unittest::TestMap message2;
  MapTestUtil::SetMapFields(&message2);

  // Construct a new version of the dynamic message via the factory.
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<Message> message1;
  message1.reset(
      factory.GetPrototype(unittest::TestMap::descriptor())->New());

  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  message1->MergeFrom(message2);
  reflection_tester.ExpectMapFieldsSetViaReflection(*message1);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(message1.get());
}

TEST(GeneratedMapFieldTest, DynamicMessageCopyFromMapReflection) {
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  unittest::TestMap message2;
  reflection_tester.SetMapFieldsViaMapReflection(&message2);

  // Construct a dynamic message via the factory.
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<Message> message1;
  message1.reset(
      factory.GetPrototype(unittest::TestMap::descriptor())->New());

  message1->MergeFrom(message2);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(message1.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message1);
}

TEST(GeneratedMapFieldTest, SyncDynamicMapWithRepeatedField) {
  // Construct a dynamic message via the factory.
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  DynamicMessageFactory factory;
  google::protobuf::scoped_ptr<Message> message;
  message.reset(
      factory.GetPrototype(unittest::TestMap::descriptor())->New());
  reflection_tester.SetMapFieldsViaReflection(message.get());
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(message.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message);
}

#endif  // !PROTOBUF_TEST_NO_DESCRIPTORS

TEST(GeneratedMapFieldTest, NonEmptyMergeFrom) {
  unittest::TestMap message1, message2;

  MapTestUtil::SetMapFields(&message1);

  // This field will test merging into an empty spot.
  (*message2.mutable_map_int32_int32())[1] = 1;
  message1.mutable_map_int32_int32()->erase(1);

  // This tests overwriting.
  (*message2.mutable_map_int32_double())[1] = 1;
  (*message1.mutable_map_int32_double())[1] = 2;

  message1.MergeFrom(message2);
  MapTestUtil::ExpectMapFieldsSet(message1);
}

TEST(GeneratedMapFieldTest, MergeFromMessageMap) {
  unittest::TestMessageMap message1, message2;

  (*message1.mutable_map_int32_message())[0].add_repeated_int32(100);
  (*message2.mutable_map_int32_message())[0].add_repeated_int32(101);

  message1.MergeFrom(message2);

  // Checks repeated field is overwritten.
  EXPECT_EQ(1, message1.map_int32_message().at(0).repeated_int32_size());
  EXPECT_EQ(101, message1.map_int32_message().at(0).repeated_int32(0));
}

// Test the generated SerializeWithCachedSizesToArray()
TEST(GeneratedMapFieldTest, SerializationToArray) {
  unittest::TestMap message1, message2;
  string data;
  MapTestUtil::SetMapFields(&message1);
  int size = message1.ByteSize();
  data.resize(size);
  uint8* start = reinterpret_cast<uint8*>(string_as_array(&data));
  uint8* end = message1.SerializeWithCachedSizesToArray(start);
  EXPECT_EQ(size, end - start);
  EXPECT_TRUE(message2.ParseFromString(data));
  MapTestUtil::ExpectMapFieldsSet(message2);
}

// Test the generated SerializeWithCachedSizes()
TEST(GeneratedMapFieldTest, SerializationToStream) {
  unittest::TestMap message1, message2;
  MapTestUtil::SetMapFields(&message1);
  int size = message1.ByteSize();
  string data;
  data.resize(size);
  {
    // Allow the output stream to buffer only one byte at a time.
    io::ArrayOutputStream array_stream(string_as_array(&data), size, 1);
    io::CodedOutputStream output_stream(&array_stream);
    message1.SerializeWithCachedSizes(&output_stream);
    EXPECT_FALSE(output_stream.HadError());
    EXPECT_EQ(size, output_stream.ByteCount());
  }
  EXPECT_TRUE(message2.ParseFromString(data));
  MapTestUtil::ExpectMapFieldsSet(message2);
}


TEST(GeneratedMapFieldTest, SameTypeMaps) {
  const Descriptor* map1 = unittest::TestSameTypeMap::descriptor()
                               ->FindFieldByName("map1")
                               ->message_type();
  const Descriptor* map2 = unittest::TestSameTypeMap::descriptor()
                               ->FindFieldByName("map2")
                               ->message_type();

  const Message* map1_entry =
      MessageFactory::generated_factory()->GetPrototype(map1);
  const Message* map2_entry =
      MessageFactory::generated_factory()->GetPrototype(map2);

  EXPECT_EQ(map1, map1_entry->GetDescriptor());
  EXPECT_EQ(map2, map2_entry->GetDescriptor());
}

TEST(GeneratedMapFieldTest, Proto2UnknownEnum) {
  unittest::TestEnumMapPlusExtra from;
  (*from.mutable_known_map_field())[0] = unittest::E_PROTO2_MAP_ENUM_FOO;
  (*from.mutable_unknown_map_field())[0] = unittest::E_PROTO2_MAP_ENUM_EXTRA;
  string data;
  from.SerializeToString(&data);

  unittest::TestEnumMap to;
  EXPECT_TRUE(to.ParseFromString(data));
  EXPECT_EQ(0, to.unknown_map_field().size());
  const UnknownFieldSet& unknown_field_set =
      to.GetReflection()->GetUnknownFields(to);
  EXPECT_EQ(1, unknown_field_set.field_count());
  EXPECT_EQ(1, to.known_map_field().size());
  EXPECT_EQ(unittest::PROTO2_MAP_ENUM_FOO, to.known_map_field().at(0));

  data.clear();
  from.Clear();
  to.SerializeToString(&data);
  EXPECT_TRUE(from.ParseFromString(data));
  EXPECT_EQ(0, from.GetReflection()->GetUnknownFields(from).field_count());
  EXPECT_EQ(1, from.known_map_field().size());
  EXPECT_EQ(unittest::E_PROTO2_MAP_ENUM_FOO, from.known_map_field().at(0));
  EXPECT_EQ(1, from.unknown_map_field().size());
  EXPECT_EQ(unittest::E_PROTO2_MAP_ENUM_EXTRA, from.unknown_map_field().at(0));
}

TEST(GeneratedMapFieldTest, StandardWireFormat) {
  unittest::TestMap message;
  string data = "\x0A\x04\x08\x01\x10\x01";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(1, message.map_int32_int32().at(1));
}

TEST(GeneratedMapFieldTest, UnorderedWireFormat) {
  unittest::TestMap message;

  // put value before key in wire format
  string data = "\x0A\x04\x10\x01\x08\x02";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(1, message.map_int32_int32().at(2));
}

TEST(GeneratedMapFieldTest, DuplicatedKeyWireFormat) {
  unittest::TestMap message;

  // Two key fields in wire format
  string data = "\x0A\x06\x08\x01\x08\x02\x10\x01";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(1, message.map_int32_int32().at(2));
}

TEST(GeneratedMapFieldTest, DuplicatedValueWireFormat) {
  unittest::TestMap message;

  // Two value fields in wire format
  string data = "\x0A\x06\x08\x01\x10\x01\x10\x02";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(2, message.map_int32_int32().at(1));
}

TEST(GeneratedMapFieldTest, MissedKeyWireFormat) {
  unittest::TestMap message;

  // No key field in wire format
  string data = "\x0A\x02\x10\x01";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(1, message.map_int32_int32().at(0));
}

TEST(GeneratedMapFieldTest, MissedValueWireFormat) {
  unittest::TestMap message;

  // No value field in wire format
  string data = "\x0A\x02\x08\x01";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(0, message.map_int32_int32().at(1));
}

TEST(GeneratedMapFieldTest, MissedValueTextFormat) {
  unittest::TestMap message;

  // No value field in text format
  string text =
      "map_int32_foreign_message {\n"
      "  key: 1234567890\n"
      "}";

  EXPECT_TRUE(google::protobuf::TextFormat::ParseFromString(text, &message));
  EXPECT_EQ(1, message.map_int32_foreign_message().size());
  EXPECT_EQ(11, message.ByteSize());
}

TEST(GeneratedMapFieldTest, UnknownFieldWireFormat) {
  unittest::TestMap message;

  // Unknown field in wire format
  string data = "\x0A\x06\x08\x02\x10\x03\x18\x01";

  EXPECT_TRUE(message.ParseFromString(data));
  EXPECT_EQ(1, message.map_int32_int32().size());
  EXPECT_EQ(3, message.map_int32_int32().at(2));
}

TEST(GeneratedMapFieldTest, CorruptedWireFormat) {
  unittest::TestMap message;

  // corrupted data in wire format
  string data = "\x0A\x06\x08\x02\x11\x03";

  EXPECT_FALSE(message.ParseFromString(data));
}

TEST(GeneratedMapFieldTest, IsInitialized) {
  unittest::TestRequiredMessageMap map_message;

  // Add an uninitialized message.
  (*map_message.mutable_map_field())[0];
  EXPECT_FALSE(map_message.IsInitialized());

  // Initialize uninitialized message
  (*map_message.mutable_map_field())[0].set_a(0);
  (*map_message.mutable_map_field())[0].set_b(0);
  (*map_message.mutable_map_field())[0].set_c(0);
  EXPECT_TRUE(map_message.IsInitialized());
}

// Generated Message Reflection Test ================================

TEST(GeneratedMapFieldReflectionTest, SpaceUsed) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
    unittest::TestMap::descriptor());
  reflection_tester.SetMapFieldsViaReflection(&message);

  EXPECT_LT(0, message.GetReflection()->SpaceUsed(message));
}

TEST(GeneratedMapFieldReflectionTest, Accessors) {
  // Set every field to a unique value then go back and check all those
  // values.
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
    unittest::TestMap::descriptor());
  reflection_tester.SetMapFieldsViaReflection(&message);
  MapTestUtil::ExpectMapFieldsSet(message);
  reflection_tester.ExpectMapFieldsSetViaReflection(message);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(&message);

  reflection_tester.ModifyMapFieldsViaReflection(&message);
  MapTestUtil::ExpectMapFieldsModified(message);
}

TEST(GeneratedMapFieldReflectionTest, Swap) {
  unittest::TestMap message1;
  unittest::TestMap message2;

  MapTestUtil::SetMapFields(&message1);

  const Reflection* reflection = message1.GetReflection();
  reflection->Swap(&message1, &message2);

  MapTestUtil::ExpectClear(message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldReflectionTest, SwapWithBothSet) {
  unittest::TestMap message1;
  unittest::TestMap message2;

  MapTestUtil::SetMapFields(&message1);
  MapTestUtil::SetMapFields(&message2);
  MapTestUtil::ModifyMapFields(&message2);

  const Reflection* reflection = message1.GetReflection();
  reflection->Swap(&message1, &message2);

  MapTestUtil::ExpectMapFieldsModified(message1);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(GeneratedMapFieldReflectionTest, SwapFields) {
  unittest::TestMap message1;
  unittest::TestMap message2;

  MapTestUtil::SetMapFields(&message2);

  vector<const FieldDescriptor*> fields;
  const Reflection* reflection = message1.GetReflection();
  reflection->ListFields(message2, &fields);
  reflection->SwapFields(&message1, &message2, fields);

  MapTestUtil::ExpectMapFieldsSet(message1);
  MapTestUtil::ExpectClear(message2);
}

TEST(GeneratedMapFieldReflectionTest, ClearField) {
  unittest::TestMap message;
  MapTestUtil::SetMapFields(&message);
  MapTestUtil::ExpectMapFieldsSet(message);

  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  reflection_tester.ClearMapFieldsViaReflection(&message);
  reflection_tester.ExpectClearViaReflection(message);
  reflection_tester.ExpectClearViaReflectionIterator(&message);
}

TEST(GeneratedMapFieldReflectionTest, RemoveLast) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());

  MapTestUtil::SetMapFields(&message);
  MapTestUtil::ExpectMapsSize(message, 2);
  std::vector<const Message*> expected_entries =
      MapTestUtil::GetMapEntries(message, 0);

  reflection_tester.RemoveLastMapsViaReflection(&message);

  MapTestUtil::ExpectMapsSize(message, 1);
  std::vector<const Message*> remained_entries =
      MapTestUtil::GetMapEntries(message, 0);
  EXPECT_TRUE(expected_entries == remained_entries);
}

TEST(GeneratedMapFieldReflectionTest, ReleaseLast) {
  unittest::TestMap message;
  const Descriptor* descriptor = message.GetDescriptor();
  MapReflectionTester reflection_tester(descriptor);

  MapTestUtil::SetMapFields(&message);

  MapTestUtil::ExpectMapsSize(message, 2);

  reflection_tester.ReleaseLastMapsViaReflection(&message);

  MapTestUtil::ExpectMapsSize(message, 1);

  // Now test that we actually release the right message.
  message.Clear();
  MapTestUtil::SetMapFields(&message);

  MapTestUtil::ExpectMapsSize(message, 2);
  std::vector<const Message*> expect_last =
      MapTestUtil::GetMapEntries(message, 1);
  std::vector<const Message*> release_last =
      MapTestUtil::GetMapEntriesFromRelease(&message);
  MapTestUtil::ExpectMapsSize(message, 1);
  EXPECT_TRUE(expect_last == release_last);
  for (std::vector<const Message*>::iterator it = release_last.begin();
       it != release_last.end(); ++it) {
    delete *it;
  }
}

TEST(GeneratedMapFieldReflectionTest, SwapElements) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
    unittest::TestMap::descriptor());

  MapTestUtil::SetMapFields(&message);

  // Get pointers of map entries at their original position
  std::vector<const Message*> entries0 = MapTestUtil::GetMapEntries(message, 0);
  std::vector<const Message*> entries1 = MapTestUtil::GetMapEntries(message, 1);

  // Swap the first time.
  reflection_tester.SwapMapsViaReflection(&message);

  // Get pointer of map entry after swap once.
  std::vector<const Message*> entries0_once =
      MapTestUtil::GetMapEntries(message, 0);
  std::vector<const Message*> entries1_once =
      MapTestUtil::GetMapEntries(message, 1);

  // Test map entries are swapped.
  MapTestUtil::ExpectMapsSize(message, 2);
  EXPECT_TRUE(entries0 == entries1_once);
  EXPECT_TRUE(entries1 == entries0_once);

  // Swap the second time.
  reflection_tester.SwapMapsViaReflection(&message);

  // Get pointer of map entry after swap once.
  std::vector<const Message*> entries0_twice =
      MapTestUtil::GetMapEntries(message, 0);
  std::vector<const Message*> entries1_twice =
      MapTestUtil::GetMapEntries(message, 1);

  // Test map entries are swapped back.
  MapTestUtil::ExpectMapsSize(message, 2);
  EXPECT_TRUE(entries0 == entries0_twice);
  EXPECT_TRUE(entries1 == entries1_twice);
}

TEST(GeneratedMapFieldReflectionTest, MutableUnknownFields) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
    unittest::TestMap::descriptor());
  reflection_tester.MutableUnknownFieldsOfMapFieldsViaReflection(&message);
}

TEST(GeneratedMapFieldReflectionTest, EmbedProto2Message) {
  unittest::TestMessageMap message;

  const FieldDescriptor* map_field =
      unittest::TestMessageMap::descriptor()->FindFieldByName(
          "map_int32_message");
  const FieldDescriptor* value =
      map_field->message_type()->FindFieldByName("value");

  Message* entry_message =
      message.GetReflection()->AddMessage(&message, map_field);
  EXPECT_EQ(
      &entry_message->GetReflection()->GetMessage(*entry_message, value),
      reinterpret_cast<const Message*>(&TestAllTypes::default_instance()));

  Message* proto2_message =
      entry_message->GetReflection()->MutableMessage(entry_message, value);
  EXPECT_EQ(unittest::TestAllTypes::descriptor(),
            proto2_message->GetDescriptor());
  ASSERT_EQ(1, message.map_int32_message().size());
}

TEST(GeneratedMapFieldReflectionTest, MergeFromClearMapEntry) {
  unittest::TestMap message;
  const FieldDescriptor* map_field =
      unittest::TestMap::descriptor()->FindFieldByName("map_int32_int32");
  const FieldDescriptor* key =
      map_field->message_type()->FindFieldByName("key");
  const FieldDescriptor* value =
      map_field->message_type()->FindFieldByName("value");

  Message* entry_message1 =
      message.GetReflection()->AddMessage(&message, map_field);
  EXPECT_FALSE(entry_message1->GetReflection()->HasField(*entry_message1, key));
  EXPECT_FALSE(
      entry_message1->GetReflection()->HasField(*entry_message1, value));

  Message* entry_message2 =
      message.GetReflection()->AddMessage(&message, map_field);
  EXPECT_FALSE(entry_message2->GetReflection()->HasField(*entry_message2, key));
  EXPECT_FALSE(
      entry_message2->GetReflection()->HasField(*entry_message2, value));

  entry_message1->MergeFrom(*entry_message2);
  EXPECT_FALSE(entry_message1->GetReflection()->HasField(*entry_message1, key));
  EXPECT_FALSE(
      entry_message1->GetReflection()->HasField(*entry_message1, value));
}

TEST(GeneratedMapFieldReflectionTest, MapEntryClear) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
    unittest::TestMap::descriptor());
  reflection_tester.MutableUnknownFieldsOfMapFieldsViaReflection(&message);
}

TEST(GeneratedMapFieldReflectionTest, Proto2MapEntryClear) {
  unittest::TestEnumMap message;
  const Descriptor* descriptor = message.GetDescriptor();
  const FieldDescriptor* field_descriptor =
      descriptor->FindFieldByName("known_map_field");
  const FieldDescriptor* value_descriptor =
      field_descriptor->message_type()->FindFieldByName("value");
  Message* sub_message =
      message.GetReflection()->AddMessage(&message, field_descriptor);
  EXPECT_EQ(0, sub_message->GetReflection()->GetEnumValue(*sub_message,
                                                          value_descriptor));
}

// Map Reflection API Test =========================================

TEST(GeneratedMapFieldReflectionTest, SetViaMapReflection) {
  unittest::TestMap message;
  MapReflectionTester reflection_tester(
      unittest::TestMap::descriptor());
  reflection_tester.SetMapFieldsViaMapReflection(&message);
  reflection_tester.ExpectMapFieldsSetViaReflection(message);
  reflection_tester.ExpectMapFieldsSetViaReflectionIterator(&message);
}

// Dynamic Message Test =============================================

class MapFieldInDynamicMessageTest : public testing::Test {
 protected:
  const DescriptorPool* pool_;
  DynamicMessageFactory factory_;
  const Descriptor* map_descriptor_;
  const Descriptor* recursive_map_descriptor_;
  const Message* map_prototype_;

  MapFieldInDynamicMessageTest()
      : pool_(DescriptorPool::generated_pool()), factory_(pool_) {}

  virtual void SetUp() {
    map_descriptor_ =
      pool_->FindMessageTypeByName("protobuf_unittest.TestMap");
    recursive_map_descriptor_ =
        pool_->FindMessageTypeByName("protobuf_unittest.TestRecursiveMapMessage");
    ASSERT_TRUE(map_descriptor_ != NULL);
    ASSERT_TRUE(recursive_map_descriptor_ != NULL);
    map_prototype_ = factory_.GetPrototype(map_descriptor_);
  }
};

TEST_F(MapFieldInDynamicMessageTest, MapIndependentOffsets) {
  // Check that all fields have independent offsets by setting each
  // one to a unique value then checking that they all still have those
  // unique values (i.e. they don't stomp each other).
  google::protobuf::scoped_ptr<Message> message(map_prototype_->New());
  MapReflectionTester reflection_tester(map_descriptor_);

  reflection_tester.SetMapFieldsViaReflection(message.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message);
}

TEST_F(MapFieldInDynamicMessageTest, DynamicMapReflection) {
  // Check that map fields work properly.
  google::protobuf::scoped_ptr<Message> message(map_prototype_->New());

  // Check set functions.
  MapReflectionTester reflection_tester(map_descriptor_);
  reflection_tester.SetMapFieldsViaMapReflection(message.get());
  reflection_tester.ExpectMapFieldsSetViaReflection(*message);
}

TEST_F(MapFieldInDynamicMessageTest, MapSpaceUsed) {
  // Test that SpaceUsed() works properly

  // Since we share the implementation with generated messages, we don't need
  // to test very much here.  Just make sure it appears to be working.

  google::protobuf::scoped_ptr<Message> message(map_prototype_->New());
  MapReflectionTester reflection_tester(map_descriptor_);

  int initial_space_used = message->SpaceUsed();

  reflection_tester.SetMapFieldsViaReflection(message.get());
  EXPECT_LT(initial_space_used, message->SpaceUsed());
}

TEST_F(MapFieldInDynamicMessageTest, RecursiveMap) {
  TestRecursiveMapMessage from;
  (*from.mutable_a())[""];
  string data = from.SerializeAsString();
  google::protobuf::scoped_ptr<Message> to(
      factory_.GetPrototype(recursive_map_descriptor_)->New());
  ASSERT_TRUE(to->ParseFromString(data));
}

// ReflectionOps Test ===============================================

TEST(ReflectionOpsForMapFieldTest, MapSanityCheck) {
  unittest::TestMap message;

  MapTestUtil::SetMapFields(&message);
  MapTestUtil::ExpectMapFieldsSet(message);
}

TEST(ReflectionOpsForMapFieldTest, MapCopy) {
  unittest::TestMap message, message2;

  MapTestUtil::SetMapFields(&message);

  ReflectionOps::Copy(message, &message2);

  MapTestUtil::ExpectMapFieldsSet(message2);

  // Copying from self should be a no-op.
  ReflectionOps::Copy(message2, &message2);
  MapTestUtil::ExpectMapFieldsSet(message2);
}

TEST(ReflectionOpsForMapFieldTest, MergeMap) {
  // Note:  Copy is implemented in terms of Merge() so technically the Copy
  //   test already tested most of this.

  unittest::TestMap message, message2;

  MapTestUtil::SetMapFields(&message);

  ReflectionOps::Merge(message2, &message);

  MapTestUtil::ExpectMapFieldsSet(message);
}

TEST(ReflectionOpsForMapFieldTest, ClearMap) {
  unittest::TestMap message;

  MapTestUtil::SetMapFields(&message);

  ReflectionOps::Clear(&message);

  MapTestUtil::ExpectClear(message);
}

TEST(ReflectionOpsForMapFieldTest, MapDiscardUnknownFields) {
  unittest::TestMap message;
  MapTestUtil::SetMapFields(&message);

  // Set some unknown fields in message.
  message.GetReflection()->MutableUnknownFields(&message)->
      AddVarint(123456, 654321);

  // Discard them.
  ReflectionOps::DiscardUnknownFields(&message);
  MapTestUtil::ExpectMapFieldsSet(message);

  EXPECT_EQ(0, message.GetReflection()->
      GetUnknownFields(message).field_count());
}

// Wire Format Test =================================================

TEST(WireFormatForMapFieldTest, ParseMap) {
  unittest::TestMap source, dest;
  string data;

  // Serialize using the generated code.
  MapTestUtil::SetMapFields(&source);
  source.SerializeToString(&data);

  // Parse using WireFormat.
  io::ArrayInputStream raw_input(data.data(), data.size());
  io::CodedInputStream input(&raw_input);
  WireFormat::ParseAndMergePartial(&input, &dest);

  // Check.
  MapTestUtil::ExpectMapFieldsSet(dest);
}

TEST(WireFormatForMapFieldTest, MapByteSize) {
  unittest::TestMap message;
  MapTestUtil::SetMapFields(&message);

  EXPECT_EQ(message.ByteSize(), WireFormat::ByteSize(message));
  message.Clear();
  EXPECT_EQ(0, message.ByteSize());
  EXPECT_EQ(0, WireFormat::ByteSize(message));
}

TEST(WireFormatForMapFieldTest, SerializeMap) {
  unittest::TestMap message;
  string generated_data;
  string dynamic_data;

  MapTestUtil::SetMapFields(&message);

  // Serialize using the generated code.
  {
    message.ByteSize();
    io::StringOutputStream raw_output(&generated_data);
    io::CodedOutputStream output(&raw_output);
    message.SerializeWithCachedSizes(&output);
    ASSERT_FALSE(output.HadError());
  }

  // Serialize using WireFormat.
  {
    io::StringOutputStream raw_output(&dynamic_data);
    io::CodedOutputStream output(&raw_output);
    int size = WireFormat::ByteSize(message);
    WireFormat::SerializeWithCachedSizes(message, size, &output);
    ASSERT_FALSE(output.HadError());
  }

  // Should be the same.
  // Don't use EXPECT_EQ here because we're comparing raw binary data and
  // we really don't want it dumped to stdout on failure.
  EXPECT_TRUE(dynamic_data == generated_data);
}

TEST(WireFormatForMapFieldTest, MapParseHelpers) {
  string data;

  {
    // Set up.
    protobuf_unittest::TestMap message;
    MapTestUtil::SetMapFields(&message);
    message.SerializeToString(&data);
  }

  {
    // Test ParseFromString.
    protobuf_unittest::TestMap message;
    EXPECT_TRUE(message.ParseFromString(data));
    MapTestUtil::ExpectMapFieldsSet(message);
  }

  {
    // Test ParseFromIstream.
    protobuf_unittest::TestMap message;
    stringstream stream(data);
    EXPECT_TRUE(message.ParseFromIstream(&stream));
    EXPECT_TRUE(stream.eof());
    MapTestUtil::ExpectMapFieldsSet(message);
  }

  {
    // Test ParseFromBoundedZeroCopyStream.
    string data_with_junk(data);
    data_with_junk.append("some junk on the end");
    io::ArrayInputStream stream(data_with_junk.data(), data_with_junk.size());
    protobuf_unittest::TestMap message;
    EXPECT_TRUE(message.ParseFromBoundedZeroCopyStream(&stream, data.size()));
    MapTestUtil::ExpectMapFieldsSet(message);
  }

  {
    // Test that ParseFromBoundedZeroCopyStream fails (but doesn't crash) if
    // EOF is reached before the expected number of bytes.
    io::ArrayInputStream stream(data.data(), data.size());
    protobuf_unittest::TestAllTypes message;
    EXPECT_FALSE(
      message.ParseFromBoundedZeroCopyStream(&stream, data.size() + 1));
  }
}

// Text Format Test =================================================

TEST(TextFormatMapTest, SerializeAndParse) {
  unittest::TestMap source;
  unittest::TestMap dest;
  MapTestUtil::SetMapFields(&source);
  string output;

  // Test compact ASCII
  TextFormat::Printer printer;
  printer.PrintToString(source, &output);
  TextFormat::Parser parser;
  EXPECT_TRUE(parser.ParseFromString(output, &dest));
  MapTestUtil::ExpectMapFieldsSet(dest);
}

TEST(TextFormatMapTest, Sorted) {
  unittest::TestMap message;
  MapReflectionTester tester(message.GetDescriptor());
  tester.SetMapFieldsViaReflection(&message);

  string expected_text;
  GOOGLE_CHECK_OK(File::GetContents(
      TestSourceDir() +
          "/google/protobuf/"
          "testdata/map_test_data.txt",
      &expected_text, true));

  EXPECT_EQ(message.DebugString(), expected_text);

  // Test again on the reverse order.
  unittest::TestMap message2;
  tester.SetMapFieldsViaReflection(&message2);
  tester.SwapMapsViaReflection(&message2);
  EXPECT_EQ(message2.DebugString(), expected_text);
}


// arena support =================================================
TEST(ArenaTest, ParsingAndSerializingNoHeapAllocation) {
  // Allocate a large initial block to avoid mallocs during hooked test.
  std::vector<char> arena_block(128 * 1024);
  ArenaOptions options;
  options.initial_block = &arena_block[0];
  options.initial_block_size = arena_block.size();
  Arena arena(options);
  string data;
  data.reserve(128 * 1024);

  {
    // TODO(teboring): Enable no heap check when ArenaStringPtr is used in map.
    // NoHeapChecker no_heap;

    unittest::TestArenaMap* from =
        Arena::CreateMessage<unittest::TestArenaMap>(&arena);
    MapTestUtil::SetArenaMapFields(from);
    from->SerializeToString(&data);

    unittest::TestArenaMap* to =
        Arena::CreateMessage<unittest::TestArenaMap>(&arena);
    to->ParseFromString(data);
    MapTestUtil::ExpectArenaMapFieldsSet(*to);
  }
}

// Use text format parsing and serializing to test reflection api.
TEST(ArenaTest, RelfectionInTextFormat) {
  Arena arena;
  string data;

  TextFormat::Printer printer;
  TextFormat::Parser parser;

  unittest::TestArenaMap* from =
      Arena::CreateMessage<unittest::TestArenaMap>(&arena);
  unittest::TestArenaMap* to =
      Arena::CreateMessage<unittest::TestArenaMap>(&arena);

  MapTestUtil::SetArenaMapFields(from);
  printer.PrintToString(*from, &data);

  EXPECT_TRUE(parser.ParseFromString(data, to));
  MapTestUtil::ExpectArenaMapFieldsSet(*to);
}

// Make sure the memory allocated for string in map is deallocated.
TEST(ArenaTest, StringMapNoLeak) {
  Arena arena;
  unittest::TestArenaMap* message =
      Arena::CreateMessage<unittest::TestArenaMap>(&arena);
  string data;
  // String with length less than 16 will not be allocated from heap.
  int original_capacity = data.capacity();
  while (data.capacity() <= original_capacity) {
    data.append("a");
  }
  (*message->mutable_map_string_string())[data] = data;
  // We rely on heap checkers to detect memory leak for us.
  ASSERT_FALSE(message == NULL);
}

}  // namespace internal
}  // namespace protobuf
}  // namespace google
