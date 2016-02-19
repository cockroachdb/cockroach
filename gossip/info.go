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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package gossip

// expired returns true if the node's time to live (TTL) has expired.
func (i *Info) expired(now int64) bool {
	return i.TTLStamp <= now
}

// isFresh returns false if the info has an originating timestamp
// earlier than the latest seen by this node.
func (i *Info) isFresh(highWaterStamp int64) bool {
	return i.OrigStamp > highWaterStamp
}

// infoMap is a map of keys to info object pointers.
type infoMap map[string]*Info
