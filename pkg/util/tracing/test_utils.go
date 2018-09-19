// Copyright 2018 The Cockroach Authors.
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
