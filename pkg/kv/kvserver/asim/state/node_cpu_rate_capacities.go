// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package state

import (
	"fmt"
	"strings"
	"time"
)

type NodeCPURateCapacities []uint64

func (sl NodeCPURateCapacities) String() string {
	if len(sl) == 0 {
		return "no cpus"
	}
	var buf strings.Builder
	if len(sl) > 1 {
		buf.WriteString("(")
	}
	for i, cc := range sl {
		if i > 0 {
			buf.WriteString(" ")
		}
		fmt.Fprintf(&buf, "%d", cc/uint64(time.Second.Nanoseconds()))
	}
	if len(sl) > 1 {
		buf.WriteString(")")
	}
	buf.WriteString(" cpu-sec/sec")
	return buf.String()
}
