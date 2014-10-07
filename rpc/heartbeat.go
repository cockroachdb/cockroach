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

package rpc

import "github.com/cockroachdb/cockroach/util/hlc"

// A PingRequest specifies the string to echo in response.
type PingRequest struct {
	Ping string // Echo this string with PingResponse.
}

// A PingResponse contains the echoed ping request string.
type PingResponse struct {
	Pong       string // An echo of value sent with PingRequest.
	ServerTime int64
}

// A HeartbeatService exposes a method to echo its request params.
type HeartbeatService struct {
	// Provides the nanosecond unix epoch timestamp of the processor.
	clock *hlc.Clock
}

// Ping echos the contents of the request to the response.
func (hs *HeartbeatService) Ping(args *PingRequest, reply *PingResponse) error {
	reply.Pong = args.Ping
	reply.ServerTime = hs.clock.Now().WallTime
	return nil
}

// A ManualHeartbeatService allows manual control of when heartbeats occur, to
// facilitate testing.
type ManualHeartbeatService struct {
	clock *hlc.Clock
	ready chan bool // Heartbeats are processed when a value is sent here.
}

// Ping waits until the heartbeat service is ready to respond to a Heartbeat.
func (mhs *ManualHeartbeatService) Ping(args *PingRequest, reply *PingResponse) error {
	<-mhs.ready
	reply.Pong = args.Ping
	reply.ServerTime = mhs.clock.Now().WallTime
	return nil
}
