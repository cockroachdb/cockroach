// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#pragma once

#include <libroach.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>

namespace cockroach {

// Getter defines an interface for retrieving a value from either an
// iterator or an engine. It is used by ProcessDeltaKey to abstract
// whether the "base" layer is an iterator or an engine.
struct Getter {
  virtual DBStatus Get(DBString* value) = 0;
};

// IteratorGetter is an implementation of the Getter interface which
// retrieves the value currently pointed to by the supplied
// iterator. It is ok for the supplied iterator to be NULL in which
// case no value will be retrieved.
struct IteratorGetter : public Getter {
  rocksdb::Iterator* const base;

  IteratorGetter(rocksdb::Iterator* iter) : base(iter) {}

  virtual DBStatus Get(DBString* value);
};

// DBGetter is an implementation of the Getter interface which
// retrieves the value for the supplied key from a rocksdb::DB.
struct DBGetter : public Getter {
  rocksdb::DB* const rep;
  rocksdb::ReadOptions const options;
  std::string const key;

  DBGetter(rocksdb::DB* const r, rocksdb::ReadOptions opts, std::string&& k)
      : rep(r), options(opts), key(std::move(k)) {}

  virtual DBStatus Get(DBString* value);
};

}  // namespace cockroach
