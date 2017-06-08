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
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

func (p *planner) PauseJob(ctx context.Context, n *parser.PauseJob) (planNode, error) {
	return nil, pgerror.Unimplemented("pause-job", "unimplemented")
}

func (p *planner) ResumeJob(ctx context.Context, n *parser.ResumeJob) (planNode, error) {
	return nil, pgerror.Unimplemented("resume-job", "unimplemented")
}

func (p *planner) CancelJob(ctx context.Context, n *parser.CancelJob) (planNode, error) {
	return nil, pgerror.Unimplemented("cancel-job", "unimplemented")
}

func (p *planner) CancelQuery(ctx context.Context, n *parser.CancelQuery) (planNode, error) {
	return nil, pgerror.Unimplemented("cancel-query", "unimplemented")
}
