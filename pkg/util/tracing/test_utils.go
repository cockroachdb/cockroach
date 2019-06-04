// Copyright 2018 The Cockroach Authors.
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

package tracing

import (
	"fmt"
	"strings"
)

// FindMsgInRecording returns the index of the first span containing msg in its
// logs, or -1 if no span is found.
func FindMsgInRecording(recording []RecordedSpan, msg string) int {
	for i, recSp := range recording {
		spMsg := ""
		for _, l := range recSp.Logs {
			for _, f := range l.Fields {
				spMsg = spMsg + fmt.Sprintf("  %s: %v", f.Key, f.Value)
			}
		}
		if strings.Contains(spMsg, msg) {
			return i
		}
	}
	return -1
}
