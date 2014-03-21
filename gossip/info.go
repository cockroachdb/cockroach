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
	"log"
	"net"
	"strings"
)

// info is the basic unit of information traded over the gossip
// network.
type info struct {
	Key       string      // Info key
	Val       interface{} // Info value: must be one of {int64, float64, string}
	Timestamp int64       // Wall time at origination (Unix-nanos)
	TTLStamp  int64       // Wall time before info is discarded (Unix-nanos)
	Hops      uint32      // Number of hops from originator
	NodeAddr  net.Addr    // Originating node in "host:port" format
	peerAddr  net.Addr    // Proximate peer which passed us the info
	seq       int64       // Sequence number for incremental updates
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
func (i *info) less(b *info) bool {
	switch t := i.Val.(type) {
	case int64:
		return t < b.Val.(int64)
	case float64:
		return t < b.Val.(float64)
	case string:
		return t < b.Val.(string)
	default:
		log.Fatalf("unhandled info value type: %s", t)
	}
	return false
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
func (a infoArray) Less(i, j int) bool { return a[i].less(a[j]) }
