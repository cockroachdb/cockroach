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

package appproto

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/normalpath"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

type responseWriter struct {
	container             app.StderrContainer
	fileNames             map[string]struct{}
	files                 []*pluginpb.CodeGeneratorResponse_File
	errorMessages         []string
	featureProto3Optional bool
	lock                  sync.RWMutex
}

func newResponseWriter(container app.StderrContainer) *responseWriter {
	return &responseWriter{
		container: container,
		fileNames: make(map[string]struct{}),
	}
}

func (r *responseWriter) AddFile(file *pluginpb.CodeGeneratorResponse_File) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if file == nil {
		return errors.New("add CodeGeneratorResponse.File is nil")
	}
	name := file.GetName()
	if name == "" {
		return errors.New("add CodeGeneratorResponse.File.Name is empty")
	}
	// name must be relative, to-slashed, and not contain "." or ".." per the documentation
	// this is what normalize does
	normalizedName, err := normalpath.NormalizeAndValidate(name)
	if err != nil {
		// we need names to be normalized for the appproto.Generator to properly put them in buckets
		// so we have to error here if it is not validated
		return newUnvalidatedNameError(name)
	}
	if normalizedName != name {
		if err := r.warnUnnormalizedName(name); err != nil {
			return err
		}
		// we need names to be normalized for the appproto.Generator to properly put
		// them in buckets, so we will coerce this into a normalized name if it is
		// validated, ie if it does not container ".." and is absolute, we can still
		// continue, assuming we validate here
		name = normalizedName
		file.Name = proto.String(name)
	}
	if _, ok := r.fileNames[name]; ok {
		if err := r.warnDuplicateName(name); err != nil {
			return err
		}
	} else {
		// we drop the file if it is duplicated, and only put in the map and files slice
		// if it does not exist
		r.fileNames[name] = struct{}{}
		r.files = append(r.files, file)
	}
	return nil
}

func (r *responseWriter) AddError(message string) {
	r.lock.Lock()
	defer r.lock.Unlock()
	if message == "" {
		// default to an error message to make sure we pass an error
		// if this function was called
		message = "error"
	}
	r.errorMessages = append(r.errorMessages, message)
}

func (r *responseWriter) SetFeatureProto3Optional() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.featureProto3Optional = true
}

// ToResponse turns the response writer into a Protobuf CodeGeneratorResponse.
// It should be run after all writing to the response has finished.
func (r *responseWriter) ToResponse() *pluginpb.CodeGeneratorResponse {
	r.lock.RLock()
	defer r.lock.RUnlock()
	response := &pluginpb.CodeGeneratorResponse{
		File: r.files,
	}
	if len(r.errorMessages) > 0 {
		response.Error = proto.String(strings.Join(r.errorMessages, "\n"))
	}
	if r.featureProto3Optional {
		response.SupportedFeatures = proto.Uint64(uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL))
	}
	return response
}

func (r *responseWriter) warnUnnormalizedName(name string) error {
	_, err := r.container.Stderr().Write([]byte(fmt.Sprintf(
		`Warning: Generated file name %q does not conform to the Protobuf generation specification. Note that the file name must be relative, use "/" instead of "\", and not use "." or ".." as part of the file name. Buf will continue without error here, but please raise an issue with the maintainer of the plugin and reference https://github.com/protocolbuffers/protobuf/blob/95e6c5b4746dd7474d540ce4fb375e3f79a086f8/src/google/protobuf/compiler/plugin.proto#L122
`,
		name,
	)))
	return err
}

func (r *responseWriter) warnDuplicateName(name string) error {
	_, err := r.container.Stderr().Write([]byte(fmt.Sprintf(
		`Warning: Duplicate generated file name %q. Buf will continue without error here and drop the second occurrence of this file, but please raise an issue with the maintainer of the plugin.
`,
		name,
	)))
	return err
}

func newUnvalidatedNameError(name string) error {
	return fmt.Errorf(
		`Generated file name %q does not conform to the Protobuf generation specification. Note that the file name must be relative, and not use "..". Please raise an issue with the maintainer of the plugin and reference https://github.com/protocolbuffers/protobuf/blob/95e6c5b4746dd7474d540ce4fb375e3f79a086f8/src/google/protobuf/compiler/plugin.proto#L122
`,
		name,
	)
}
