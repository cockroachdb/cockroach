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

package distsqlrun

import (
	"github.com/pkg/errors"
)

func newRegisteredProcessor(
	flowCtx *FlowCtx,
	spec *RegisteredProcessorSpec,
	inputs []RowSource,
	outputs []RowReceiver,
) (Processor, error) {
	fn := registeredProcessorTypes[spec.Name]
	if fn == nil {
		return nil, errors.Errorf("no registered processor: %s", spec.Name)
	}
	return fn(flowCtx, inputs, outputs, spec.Args)
}

type RegisteredProcessorFunc func(flowCtx *FlowCtx, inputs []RowSource, outputs []RowReceiver, args []byte) (Processor, error)

var registeredProcessorTypes = map[string]RegisteredProcessorFunc{}

func RegisterProcessor(name string, fn RegisteredProcessorFunc) {
	registeredProcessorTypes[name] = fn
}
