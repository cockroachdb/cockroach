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

package rpc

// A PingRequest specifies the string to echo in response.
type PingRequest struct {
	Ping string // Echo this string with PingResponse.
}

// A PingResponse contains the echoed ping request string.
type PingResponse struct {
	Pong string // An echo of value sent with PingRequest.
}

type HeartbeatService struct{}

// Ping echos the contents of the request to the response.
func (hs *HeartbeatService) Ping(args *PingRequest, reply *PingResponse) error {
	reply.Pong = args.Ping
	return nil
}
