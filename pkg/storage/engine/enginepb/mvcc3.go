// Copyright 2017 The Cockroach Authors.
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

package enginepb

import proto "github.com/gogo/protobuf/proto"

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCStatsDelta) ToStats() MVCCStats {
	return MVCCStats(*ms)
}

// ToStatsDelta converts the receiver to an MVCCStatsDelta.
func (ms *MVCCStats) ToStatsDelta() MVCCStatsDelta {
	return MVCCStatsDelta(*ms)
}

// ToStats converts the receiver to an MVCCStats.
func (ms *MVCCPersistentStats) ToStats() MVCCStats {
	return MVCCStats(*ms)
}

// ToPersistentStats converts the receiver to an MVCCPersistentStats.
func (ms *MVCCStats) ToPersistentStats() MVCCPersistentStats {
	return MVCCPersistentStats(*ms)
}

var isolationTypeLowerCase = map[int32]string{
	0: "serializable",
	1: "snapshot",
}

// ToLowerCaseString returns the lower case version of String().
// Asking for lowercase is common enough (pg_setting / SHOW in SQL)
// that we don't want to call strings.ToLower(x.String()) all the time.
func (x IsolationType) ToLowerCaseString() string {
	return proto.EnumName(isolationTypeLowerCase, int32(x))
}
