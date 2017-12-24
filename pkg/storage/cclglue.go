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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval"
	"github.com/pkg/errors"
)

var importCmdFn ImportCmdFunc = func(context.Context, batcheval.CommandArgs) (*roachpb.ImportResponse, error) {
	return &roachpb.ImportResponse{}, errors.Errorf("unimplemented command: %s", roachpb.Import)
}

// ImportCmdFunc is the type of the function that will be called as the
// implementation of the Import command.
type ImportCmdFunc func(context.Context, batcheval.CommandArgs) (*roachpb.ImportResponse, error)

// SetImportCmd allows setting the function that will be called as the
// implementation of the Import command. Only allowed to be called by Init.
func SetImportCmd(fn ImportCmdFunc) {
	// This is safe if SetImportCmd is only called at init time.
	importCmdFn = fn
}
