// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
