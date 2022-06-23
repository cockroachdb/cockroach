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

package appprotoexec

import (
	"bytes"
	"context"
	"os/exec"
	"path/filepath"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appproto"
	"github.com/bufbuild/buf/private/pkg/protoencoding"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/pluginpb"
)

type binaryHandler struct {
	logger     *zap.Logger
	pluginPath string
}

func newBinaryHandler(
	logger *zap.Logger,
	pluginPath string,
) *binaryHandler {
	return &binaryHandler{
		logger:     logger.Named("appprotoexec"),
		pluginPath: pluginPath,
	}
}

func (h *binaryHandler) Handle(
	ctx context.Context,
	container app.EnvStderrContainer,
	responseWriter appproto.ResponseWriter,
	request *pluginpb.CodeGeneratorRequest,
) error {
	ctx, span := trace.StartSpan(ctx, "plugin_proxy")
	span.AddAttributes(trace.StringAttribute("plugin", filepath.Base(h.pluginPath)))
	defer span.End()
	unsetRequestVersion := false
	if request.CompilerVersion == nil {
		unsetRequestVersion = true
		request.CompilerVersion = DefaultVersion
	}
	requestData, err := protoencoding.NewWireMarshaler().Marshal(request)
	if unsetRequestVersion {
		request.CompilerVersion = nil
	}
	if err != nil {
		return err
	}
	responseBuffer := bytes.NewBuffer(nil)
	cmd := exec.CommandContext(ctx, h.pluginPath)
	cmd.Env = app.Environ(container)
	cmd.Stdin = bytes.NewReader(requestData)
	cmd.Stdout = responseBuffer
	cmd.Stderr = container.Stderr()
	if err := cmd.Run(); err != nil {
		// TODO: strip binary path as well?
		return err
	}
	response := &pluginpb.CodeGeneratorResponse{}
	if err := protoencoding.NewWireUnmarshaler(nil).Unmarshal(responseBuffer.Bytes(), response); err != nil {
		return err
	}
	response, err = normalizeCodeGeneratorResponse(response)
	if err != nil {
		return err
	}
	if response.GetSupportedFeatures()&uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL) != 0 {
		responseWriter.SetFeatureProto3Optional()
	}
	for _, file := range response.File {
		if err := responseWriter.AddFile(file); err != nil {
			return err
		}
	}
	// plugin.proto specifies that only non-empty errors are considered errors.
	// This is also consistent with protoc's behaviour.
	// Ref: https://github.com/protocolbuffers/protobuf/blob/069f989b483e63005f87ab309de130677718bbec/src/google/protobuf/compiler/plugin.proto#L100-L108.
	if response.GetError() != "" {
		responseWriter.AddError(response.GetError())
	}
	return nil
}
