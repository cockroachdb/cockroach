// Copyright 2016 The Cockroach Authors.
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
//
// Author: Peter Mattis (petermattis@gmail.com)

package tracing

// static void annotateTrace() {
// }
import "C"
import "github.com/cockroachdb/cockroach/util/envutil"

var annotationEnabled = envutil.EnvOrDefaultBool("annotate_traces", false)

// AnnotateTrace adds an annotation to the golang executation tracer by calling
// a no-op cgo function.
func AnnotateTrace() {
	if annotationEnabled {
		C.annotateTrace()
	}
}
