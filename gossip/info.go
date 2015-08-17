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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

import (
	"strings"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

func (i *Info) setValue(v interface{}) {
	// TODO(thschroeter): avoid duplication.
	// supported types are listed below and
	// in ValueUnion in gossip.proto
	var nv interface{}
	switch t := v.(type) {
	case int64:
		nv = &t
	case float64:
		nv = &t
	case string:
		nv = &t
	case proto.RangeDescriptor:
		nv = &t
	case proto.NodeDescriptor:
		nv = &t
	case proto.StoreDescriptor:
		nv = &t
	default:
		nv = t
	}
	if !i.Val.SetValue(nv) {
		log.Fatalf("unsupported type %T", nv)
	}
}

func (i *Info) value() interface{} {
	v := i.Val.GetValue()
	switch t := v.(type) {
	case *int64:
		return *t
	case *float64:
		return *t
	case *string:
		return *t
	case *proto.RangeDescriptor:
		return *t
	case *proto.StoreDescriptor:
		return *t
	case *proto.NodeDescriptor:
		return *t
	}
	return v
}

// infoPrefix returns the text preceding the last period within
// the given key.
func infoPrefix(key string) string {
	if index := strings.LastIndex(key, "."); index != -1 {
		return key[:index]
	}
	return ""
}

// less returns true if i's value is less than b's value. i's and
// b's types must match.
func (i *Info) less(b *Info) bool {
	v := i.Val.GetValue()
	bv := b.Val.GetValue()
	switch t := v.(type) {
	case *int64:
		return *t < *bv.(*int64)
	case *float64:
		return *t < *bv.(*float64)
	case *string:
		return *t < *bv.(*string)
	default:
		if ord, ok := v.(util.Ordered); ok {
			return ord.Less(bv.(util.Ordered))
		}
		log.Fatalf("unhandled info value type: %s", t)
	}
	return false
}

// expired returns true if the node's time to live (TTL) has expired.
func (i *Info) expired(now int64) bool {
	return i.TTLStamp <= now
}

// isFresh returns true if the info has a sequence number newer
// than seq and wasn't either passed directly or originated from
// the same node.
func (i *Info) isFresh(nodeID proto.NodeID, seq int64) bool {
	if i.Seq <= seq {
		return false
	}
	if nodeID != 0 && i.NodeID == nodeID {
		return false
	}
	if nodeID != 0 && i.PeerID == nodeID {
		return false
	}
	return true
}

// infoMap is a map of keys to info object pointers.
type infoMap map[string]*Info

// infoSlice is a slice of Info object pointers.
type infoSlice []*Info

// Implement sort.Interface for infoSlice.
func (a infoSlice) Len() int           { return len(a) }
func (a infoSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a infoSlice) Less(i, j int) bool { return a[i].less(a[j]) }
