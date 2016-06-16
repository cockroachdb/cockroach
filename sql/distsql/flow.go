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
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"sync"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util"
)

// Flow represents a flow which consists of processors and streams.
type Flow struct {
	evalCtx            *parser.EvalContext
	txn                *client.Txn
	simpleFlowConsumer rowReceiver
	waitGroup          sync.WaitGroup
	processors         []processor
}

func (f *Flow) setupMailbox(sp *MailboxSpec) (rowReceiver, error) {
	// TODO(radu): for now we only support the simple flow mailbox.
	if !sp.SimpleResponse {
		return nil, util.Errorf("mailbox spec %s not supported", sp)
	}
	return f.simpleFlowConsumer, nil
}

func (f *Flow) setupStreamOut(spec StreamEndpointSpec) (rowReceiver, error) {
	if spec.LocalStreamID != nil {
		return nil, util.Errorf("local endpoints not supported")
	}
	if spec.Mailbox == nil {
		return nil, util.Errorf("empty endpoint spec")
	}
	return f.setupMailbox(spec.Mailbox)
}

func (f *Flow) setupRouter(spec OutputRouterSpec) (rowReceiver, error) {
	streams := make([]rowReceiver, len(spec.Streams))
	for i := range spec.Streams {
		var err error
		streams[i], err = f.setupStreamOut(spec.Streams[i])
		if err != nil {
			return nil, err
		}
	}
	return makeRouter(spec.Type, streams)
}

// TODO(radu): this should return a general processor interface, not
// a TableReader.
func (f *Flow) setupProcessor(ps *ProcessorSpec) (*tableReader, error) {
	if ps.Core.TableReader == nil {
		return nil, util.Errorf("unsupported processor %s", ps)
	}
	if len(ps.Output) != 1 {
		return nil, util.Errorf("only single-output processors supported")
	}
	out, err := f.setupRouter(ps.Output[0])
	if err != nil {
		return nil, err
	}
	tr, err := newTableReader(ps.Core.TableReader, f.txn, out, f.evalCtx)
	if err != nil {
		return nil, err
	}
	f.processors = append(f.processors, tr)
	return tr, nil
}

// Start starts the flow (each processor runs in their own goroutine).
func (f *Flow) Start() {
	f.waitGroup.Add(len(f.processors))
	for _, p := range f.processors {
		go p.Run(&f.waitGroup)
	}
}

// Wait waits for all the goroutines for this flow to exit.
func (f *Flow) Wait() {
	f.waitGroup.Wait()
}

// RunSync runs the processors in the flow in order (serially), in the same
// context (no goroutines are spawned).
func (f *Flow) RunSync() {
	for _, p := range f.processors {
		p.Run(nil)
	}
}
