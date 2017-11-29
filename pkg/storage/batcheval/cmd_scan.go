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
// permissions and limitations under the License.

package batcheval

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// A Command is the implementation of a single request within a BatchRequest.
type Command struct {
	// DeclareKeys adds all keys this command touches to the given spanSet.
	DeclareKeys func(roachpb.RangeDescriptor, roachpb.Header, roachpb.Request, *spanset.SpanSet)

	// Eval evaluates a command on the given engine. It should populate
	// the supplied response (always a non-nil pointer to the correct
	// type) and return special side effects (if any) in the Result.
	// If it writes to the engine it should also update
	// *CommandArgs.Stats.
	Eval func(context.Context, engine.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error)
}

var cmds = make(map[roachpb.Method]Command)

// RegisterCommand makes a command available for execution. It must only be
// called before any evaluation takes place.
//
// For testing purposes, calling RegisterCommand with `nil` for its function
// arguments removes the specified command.
func RegisterCommand(
	method roachpb.Method,
	declare func(roachpb.RangeDescriptor, roachpb.Header, roachpb.Request, *spanset.SpanSet),
	impl func(context.Context, engine.ReadWriter, CommandArgs, roachpb.Response) (result.Result, error),
) {
	if declare == nil && impl == nil {
		delete(cmds, method)
		return
	}
	if _, ok := cmds[method]; ok {
		log.Fatalf(context.TODO(), "cannot overwrite previously registered method %v", method)
	}
	cmds[method] = Command{
		DeclareKeys: declare,
		Eval:        impl,
	}
}

// LookupCommand returns the command for the given method, with the boolean
// indicating success or failure.
func LookupCommand(method roachpb.Method) (Command, bool) {
	cmd, ok := cmds[method]
	return cmd, ok
}

func init() {
	RegisterCommand(roachpb.Scan, DefaultDeclareKeys, Scan)
}

// Scan scans the key range specified by start key through end key
// in ascending order up to some maximum number of results. maxKeys
// stores the number of scan results remaining for this batch
// (MaxInt64 for no limit).
func Scan(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ScanRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.ScanResponse)

	rows, resumeSpan, intents, err := engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
		cArgs.MaxKeys, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	if err != nil {
		return result.Result{}, err
	}

	reply.NumKeys = int64(len(rows))
	if resumeSpan != nil {
		reply.ResumeSpan = resumeSpan
		reply.ResumeReason = roachpb.RESUME_KEY_LIMIT
	}
	reply.Rows = rows
	if args.ReturnIntents {
		reply.IntentRows, err = CollectIntentRows(ctx, batch, cArgs, intents)
	}
	return result.FromIntents(intents, args, true /* alwaysReturn */), err
}
