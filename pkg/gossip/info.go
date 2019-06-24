// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gossip

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
