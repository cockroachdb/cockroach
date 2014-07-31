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

package storage

import "fmt"

// A NotLeaderError indicates that the current range is not the
// leader. If the leader is known, its Replica is set in the error.
type NotLeaderError struct {
	leader Replica
}

// Error formats error.
func (e *NotLeaderError) Error() string {
	return fmt.Sprintf("range not leader; leader is %+v", e.leader)
}

// A NotInRangeError indicates that a command's key or key range falls
// either completely or partially outside the target range's key
// range.
type NotInRangeError struct {
	start, end Key
}

// Error formats error.
func (e *NotInRangeError) Error() string {
	return fmt.Sprintf("key range outside of range bounds %q-%q", string(e.start), string(e.end))
}
