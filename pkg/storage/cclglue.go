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

package storage

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/pkg/errors"
)

// The variables/methods here are initialized/called at init() time, often from
// the ccl packages.

func makeUnimplementedCommand(method roachpb.Method) Command {
	return Command{
		DeclareKeys: DefaultDeclareKeys,
		Eval: func(
			_ context.Context, _ engine.ReadWriter, _ CommandArgs, _ roachpb.Response,
		) (EvalResult, error) {
			return EvalResult{}, errors.Errorf("unimplemented command: %s", method.String())
		}}
}

var writeBatchCmd = makeUnimplementedCommand(roachpb.WriteBatch)
var addSSTableCmd = makeUnimplementedCommand(roachpb.AddSSTable)
var exportCmd = makeUnimplementedCommand(roachpb.Export)
var importCmdFn ImportCmdFunc = func(context.Context, CommandArgs) (*roachpb.ImportResponse, error) {
	return &roachpb.ImportResponse{}, errors.Errorf("unimplemented command: %s", roachpb.Import)
}

// SetWriteBatchCmd allows setting the function that will be called as the
// implementation of the WriteBatch command. Only allowed to be called by Init.
func SetWriteBatchCmd(cmd Command) {
	// This is safe if SetWriteBatchCmd is only called at init time.
	commands[roachpb.WriteBatch] = cmd
}

// SetAddSSTableCmd allows setting the function that will be called as the
// implementation of the AddSSTable command. Only allowed to be called by Init.
func SetAddSSTableCmd(cmd Command) {
	// This is safe if SetAddSSTableCmd is only called at init time.
	commands[roachpb.AddSSTable] = cmd
}

// SetExportCmd allows setting the function that will be called as the
// implementation of the Export command. Only allowed to be called by Init.
func SetExportCmd(cmd Command) {
	// This is safe if SetExportCmd is only called at init time.
	commands[roachpb.Export] = cmd
}

// ImportCmdFunc is the type of the function that will be called as the
// implementation of the Import command.
type ImportCmdFunc func(context.Context, CommandArgs) (*roachpb.ImportResponse, error)

// SetImportCmd allows setting the function that will be called as the
// implementation of the Import command. Only allowed to be called by Init.
func SetImportCmd(fn ImportCmdFunc) {
	// This is safe if SetImportCmd is only called at init time.
	importCmdFn = fn
}
