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

package client

import (
	"math/rand"

	"github.com/cockroachdb/cockroach/proto"
)

// A Call is a pending database API call.
type Call struct {
	Method string         // The name of the database command (see api.proto)
	Args   proto.Request  // The argument to the command
	Reply  proto.Response // The reply from the command
}

// resetClientCmdID sets the client command ID if the call is for a
// read-write method. The client command ID provides idempotency
// protection in conjunction with the server.
func (c *Call) resetClientCmdID(clock Clock) {
	// On mutating commands, set a client command ID. This prevents
	// mutations from being run multiple times on retries.
	if proto.IsReadWrite(c.Method) {
		c.Args.Header().CmdID = proto.ClientCmdID{
			WallTime: clock.Now(),
			Random:   rand.Int63(),
		}
	}
}
