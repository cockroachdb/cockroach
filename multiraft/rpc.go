// Copyright 2015 The Cockroach Authors.
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
// Author: Tamir Duberstein (tamird@gmail.com)

package multiraft

import "github.com/cockroachdb/cockroach/security"

var _ security.RequestWithUser = &RaftMessageRequest{}
var _ security.RequestWithUser = &MultiPackageMessageRequest{}

// GetUser implements security.RequestWithUser.
// Raft messages are always sent by the node user.
func (*RaftMessageRequest) GetUser() string {
	return security.NodeUser
}

// GetUser implements security.RequestWithUser.
// Multi-Package messages are always sent by the node user.
func (*MultiPackageMessageRequest) GetUser() string {
	return security.NodeUser
}
