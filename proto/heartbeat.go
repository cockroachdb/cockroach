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
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package proto

import "math"

// InfiniteOffset is the offset value used if we fail to detect a heartbeat.
var InfiniteOffset = RemoteOffset{
	Offset: math.MaxInt64,
	Error:  0,
}

// Equal is a equality comparison between remote offsets.
func (r RemoteOffset) Equal(o RemoteOffset) bool {
	return r.Offset == o.Offset && r.Error == o.Error && r.MeasuredAt == o.MeasuredAt
}
