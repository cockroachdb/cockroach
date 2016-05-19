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

type flow struct {
	evalCtx           parser.EvalContext
	txn               *client.Txn
	simpleFlowMailbox *outbox
	waitGroup         sync.WaitGroup
}

func (f *flow) setupMailbox(sp *MailboxSpec) (rowReceiver, error) {
	// TODO(radu): for now we only support the simple flow mailbox.
	if !sp.SimpleResponse {
		return nil, util.Errorf("mailbox spec %s not supported", sp)
	}
	return f.simpleFlowMailbox, nil
}

func (f *flow) setupStreamOut(spec StreamEndpointSpec) (rowReceiver, error) {
	if spec.LocalStreamID != nil {
		return nil, util.Errorf("local endpoints not supported")
	}
	if spec.Mailbox == nil {
		return nil, util.Errorf("empty endpoint spec")
	}
	return f.setupMailbox(spec.Mailbox)
}

func (f *flow) setupRouter(spec OutputRouterSpec) (rowReceiver, error) {
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
func (f *flow) setupProcessor(ps *ProcessorSpec) (*tableReader, error) {
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
	return newTableReader(ps.Core.TableReader, f.txn, out, f.evalCtx)

}
