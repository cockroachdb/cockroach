// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package bufmoduleprotoparse

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/bufbuild/buf/private/bufpkg/bufmodule"
	"github.com/bufbuild/buf/private/gen/data/datawkt"
	"github.com/bufbuild/buf/private/pkg/storage"
	"go.uber.org/multierr"
)

type parserAccessorHandler struct {
	ctx                  context.Context
	module               bufmodule.Module
	pathToExternalPath   map[string]string
	nonImportPaths       map[string]struct{}
	pathToModuleIdentity map[string]bufmodule.ModuleIdentity
	pathToCommit         map[string]string
	lock                 sync.RWMutex
}

func newParserAccessorHandler(
	ctx context.Context,
	module bufmodule.Module,
) *parserAccessorHandler {
	return &parserAccessorHandler{
		ctx:                  ctx,
		module:               module,
		pathToExternalPath:   make(map[string]string),
		nonImportPaths:       make(map[string]struct{}),
		pathToModuleIdentity: make(map[string]bufmodule.ModuleIdentity),
		pathToCommit:         make(map[string]string),
	}
}

func (p *parserAccessorHandler) Open(path string) (_ io.ReadCloser, retErr error) {
	moduleFile, moduleErr := p.module.GetModuleFile(p.ctx, path)
	if moduleErr != nil {
		if !storage.IsNotExist(moduleErr) {
			return nil, moduleErr
		}
		if wktModuleFile, wktErr := datawkt.ReadBucket.Get(p.ctx, path); wktErr == nil {
			if wktModuleFile.Path() != path {
				// this should never happen, but just in case
				return nil, fmt.Errorf("parser accessor requested path %q but got %q", path, wktModuleFile.Path())
			}
			if err := p.addPath(path, path, true, nil, ""); err != nil {
				return nil, err
			}
			return wktModuleFile, nil
		}
		return nil, moduleErr
	}
	defer func() {
		if retErr != nil {
			retErr = multierr.Append(retErr, moduleFile.Close())
		}
	}()
	if moduleFile.Path() != path {
		// this should never happen, but just in case
		return nil, fmt.Errorf("parser accessor requested path %q but got %q", path, moduleFile.Path())
	}
	if err := p.addPath(
		path,
		moduleFile.ExternalPath(),
		moduleFile.IsImport(),
		moduleFile.ModuleIdentity(),
		moduleFile.Commit(),
	); err != nil {
		return nil, err
	}
	return moduleFile, nil
}

func (p *parserAccessorHandler) ExternalPath(path string) string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	if externalPath := p.pathToExternalPath[path]; externalPath != "" {
		return externalPath
	}
	return path
}

func (p *parserAccessorHandler) IsImport(path string) bool {
	p.lock.RLock()
	defer p.lock.RUnlock()
	_, isNotImport := p.nonImportPaths[path]
	return !isNotImport
}

func (p *parserAccessorHandler) ModuleIdentity(path string) bufmodule.ModuleIdentity {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pathToModuleIdentity[path] // nil is a valid value.
}

func (p *parserAccessorHandler) Commit(path string) string {
	p.lock.RLock()
	defer p.lock.RUnlock()
	return p.pathToCommit[path] // empty is a valid value.
}

func (p *parserAccessorHandler) addPath(
	path string,
	externalPath string,
	isImport bool,
	moduleIdentity bufmodule.ModuleIdentity,
	commit string,
) error {
	p.lock.Lock()
	defer p.lock.Unlock()
	existingExternalPath, ok := p.pathToExternalPath[path]
	if ok {
		if existingExternalPath != externalPath {
			return fmt.Errorf("parser accessor had external paths %q and %q for path %q", existingExternalPath, externalPath, path)
		}
	} else {
		p.pathToExternalPath[path] = externalPath
	}
	if !isImport {
		p.nonImportPaths[path] = struct{}{}
	}
	if moduleIdentity != nil {
		p.pathToModuleIdentity[path] = moduleIdentity
	}
	if commit != "" {
		p.pathToCommit[path] = commit
	}
	return nil
}
