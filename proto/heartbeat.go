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

import (
	"fmt"
	"time"
)

// Equal is a equality comparison between remote offsets.
func (r RemoteOffset) Equal(o RemoteOffset) bool {
	return r.Offset == o.Offset && r.Uncertainty == o.Uncertainty && r.MeasuredAt == o.MeasuredAt
}

// String formats the RemoteOffset for human readability.
func (r RemoteOffset) String() string {
	t := time.Unix(r.MeasuredAt/1E9, 0).UTC()
	return fmt.Sprintf("off=%.9fs, err=%.9fs, at=%s", float64(r.Offset)/1E9, float64(r.Uncertainty)/1E9, t)
}
