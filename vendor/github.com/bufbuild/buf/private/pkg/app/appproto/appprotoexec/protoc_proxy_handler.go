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
	"fmt"
	"io"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/app/appproto"
	"github.com/bufbuild/buf/private/pkg/ioextended"
	"github.com/bufbuild/buf/private/pkg/protoencoding"
	"github.com/bufbuild/buf/private/pkg/storage"
	"github.com/bufbuild/buf/private/pkg/storage/storageos"
	"github.com/bufbuild/buf/private/pkg/tmp"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

type protocProxyHandler struct {
	logger            *zap.Logger
	storageosProvider storageos.Provider
	protocPath        string
	pluginName        string
}

func newProtocProxyHandler(
	logger *zap.Logger,
	storageosProvider storageos.Provider,
	protocPath string,
	pluginName string,
) *protocProxyHandler {
	return &protocProxyHandler{
		logger:            logger.Named("appprotoexec"),
		storageosProvider: storageosProvider,
		protocPath:        protocPath,
		pluginName:        pluginName,
	}
}

func (h *protocProxyHandler) Handle(
	ctx context.Context,
	container app.EnvStderrContainer,
	responseWriter appproto.ResponseWriter,
	request *pluginpb.CodeGeneratorRequest,
) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "protoc_proxy")
	span.AddAttributes(trace.StringAttribute("plugin", filepath.Base(h.pluginName)))
	defer span.End()
	protocVersion, err := h.getProtocVersion(ctx, container)
	if err != nil {
		return err
	}
	if h.pluginName == "kotlin" && !getKotlinSupported(protocVersion) {
		return fmt.Errorf("kotlin is not supported for protoc version %s", versionString(protocVersion))
	}
	fileDescriptorSet := &descriptorpb.FileDescriptorSet{
		File: request.ProtoFile,
	}
	fileDescriptorSetData, err := protoencoding.NewWireMarshaler().Marshal(fileDescriptorSet)
	if err != nil {
		return err
	}
	descriptorFilePath := app.DevStdinFilePath
	var tmpFile tmp.File
	if descriptorFilePath == "" {
		// since we have no stdin file (i.e. Windows), we're going to have to use a temporary file
		tmpFile, err = tmp.NewFileWithData(fileDescriptorSetData)
		if err != nil {
			return err
		}
		defer func() {
			retErr = multierr.Append(retErr, tmpFile.Close())
		}()
		descriptorFilePath = tmpFile.AbsPath()
	}
	tmpDir, err := tmp.NewDir()
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, tmpDir.Close())
	}()
	args := []string{
		fmt.Sprintf("--descriptor_set_in=%s", descriptorFilePath),
		fmt.Sprintf("--%s_out=%s", h.pluginName, tmpDir.AbsPath()),
	}
	featureProto3Optional := getFeatureProto3Optional(protocVersion)
	if featureProto3Optional {
		args = append(
			args,
			// this flag may be deleted someday
			// in that case, we will have to check that minor < certain_value
			"--experimental_allow_proto3_optional",
		)
	}
	if parameter := request.GetParameter(); parameter != "" {
		args = append(
			args,
			fmt.Sprintf("--%s_opt=%s", h.pluginName, parameter),
		)
	}
	args = append(
		args,
		request.FileToGenerate...,
	)
	cmd := exec.CommandContext(ctx, h.protocPath, args...)
	cmd.Env = app.Environ(container)
	if descriptorFilePath != "" && descriptorFilePath == app.DevStdinFilePath {
		cmd.Stdin = bytes.NewReader(fileDescriptorSetData)
	}
	cmd.Stdout = io.Discard
	cmd.Stderr = container.Stderr()
	if err := cmd.Run(); err != nil {
		// TODO: strip binary path as well?
		// We don't know if this is a system error or plugin error, so we assume system error
		return err
	}
	if featureProto3Optional {
		responseWriter.SetFeatureProto3Optional()
	}
	// no need for symlinks here, and don't want to support
	readWriteBucket, err := h.storageosProvider.NewReadWriteBucket(tmpDir.AbsPath())
	if err != nil {
		return err
	}
	return storage.WalkReadObjects(
		ctx,
		readWriteBucket,
		"",
		func(readObject storage.ReadObject) error {
			data, err := io.ReadAll(readObject)
			if err != nil {
				return err
			}
			return responseWriter.AddFile(
				&pluginpb.CodeGeneratorResponse_File{
					Name:    proto.String(readObject.Path()),
					Content: proto.String(string(data)),
				},
			)
		},
	)
}

func (h *protocProxyHandler) getProtocVersion(
	ctx context.Context,
	container app.EnvContainer,
) (*pluginpb.Version, error) {
	stdoutBuffer := bytes.NewBuffer(nil)
	stderrBuffer := bytes.NewBuffer(nil)
	cmd := exec.CommandContext(ctx, h.protocPath, "--version")
	cmd.Env = app.Environ(container)
	cmd.Stdin = ioextended.DiscardReader
	// do we want to do this?
	cmd.Stdout = stdoutBuffer
	cmd.Stderr = stderrBuffer
	if err := cmd.Run(); err != nil {
		// TODO: strip binary path as well?
		return nil, fmt.Errorf("%v\n%v", err, stderrBuffer.String())
	}
	return parseVersionForCLIVersion(strings.TrimSpace(stdoutBuffer.String()))
}
