// Copyright 2020 The Cockroach Authors.
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

package markers

import "regexp"

// Internal constants.
const (
	Start       = '‹'
	StartS      = string(Start)
	End         = '›'
	EndS        = string(End)
	EscapeMark  = '?'
	EscapeMarkS = string(EscapeMark)
	RedactedS   = StartS + "×" + EndS
)

// Internal variables.
var (
	StartBytes       = []byte(StartS)
	EndBytes         = []byte(EndS)
	EscapeMarkBytes  = []byte(EscapeMarkS)
	RedactedBytes    = []byte(RedactedS)
	ReStripSensitive = regexp.MustCompile(StartS + "[^" + StartS + EndS + "]*" + EndS)
	ReStripMarkers   = regexp.MustCompile("[" + StartS + EndS + "]")
)
