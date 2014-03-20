// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"encoding/gob"
	"net"
	"strings"
)

// init pre-registers all value types.
func init() {
	gob.Register(Int64Value(0))
	gob.Register(Float64Value(0))
	gob.Register(StringValue(""))
}

// Comparable is satisfied by objects used within an InfoArray
// to support sorting.
// TODO(spencer): maybe better just to use direct basic types
//   and the sort package's facility for sorting ints, floats and strings.
type Comparable interface {
	Less(b Comparable) bool
}

// Int64Value is an int64 that satisfies the Comparable interface.
type Int64Value int64

// Less interface for sorting Int64Value arrays.
func (a Int64Value) Less(b Comparable) bool {
	return a < b.(Int64Value)
}

// Float64Value is a float64 that satisfies the Comparable interface.
type Float64Value float64

// Less interface for sorting Float64Value arrays.
func (a Float64Value) Less(b Comparable) bool {
	return a < b.(Float64Value)
}

// StringValue is a string that satisfies the Comparable interface.
type StringValue string

// Less interface for sorting StringValue arrays.
func (a StringValue) Less(b Comparable) bool {
	return a < b.(StringValue)
}

// info is the basic unit of information traded over the gossip
// network.
type info struct {
	Key       string     // Info key
	Val       Comparable // Comparable info value
	Timestamp int64      // Wall time at origination (Unix-nanos)
	TTLStamp  int64      // Wall time before info is discarded (Unix-nanos)
	Hops      uint32     // Number of hops from originator
	NodeAddr  net.Addr   // Originating node in "host:port" format
	peerAddr  net.Addr   // Proximate peer which passed us the info
	seq       int64      // Sequence number for incremental updates
}

// infoPrefix returns the text preceding the last period within
// the given key.
func infoPrefix(key string) string {
	if index := strings.LastIndex(key, "."); index != -1 {
		return key[:index]
	}
	return ""
}

// expired returns true if the node's time to live (TTL) has expired.
func (i *info) expired(now int64) bool {
	return i.TTLStamp <= now
}

// isFresh returns true if the info has a sequence number newer
// than seq and wasn't either passed directly or originated from
// the same address as addr.
func (i *info) isFresh(addr net.Addr, seq int64) bool {
	if i.seq <= seq {
		return false
	}
	if i.NodeAddr.String() == addr.String() {
		return false
	}
	if i.peerAddr.String() == addr.String() {
		return false
	}
	return true
}

// infoMap is a map of keys to info object pointers.
type infoMap map[string]*info

// infoArray is a slice of Info object pointers.
type infoArray []*info

// Implement sort.Interface for infoArray.
func (a infoArray) Len() int           { return len(a) }
func (a infoArray) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a infoArray) Less(i, j int) bool { return a[i].Val.Less(a[j].Val) }
