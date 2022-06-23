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

package bufwire

import (
	"context"
	"fmt"

	"github.com/bufbuild/buf/private/buf/buffetch"
	"github.com/bufbuild/buf/private/bufpkg/bufimage"
	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/protoencoding"
	"go.opencensus.io/trace"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type imageWriter struct {
	logger      *zap.Logger
	fetchWriter buffetch.Writer
}

func newImageWriter(
	logger *zap.Logger,
	fetchWriter buffetch.Writer,
) *imageWriter {
	return &imageWriter{
		logger:      logger,
		fetchWriter: fetchWriter,
	}
}

func (i *imageWriter) PutImage(
	ctx context.Context,
	container app.EnvStdoutContainer,
	imageRef buffetch.ImageRef,
	image bufimage.Image,
	asFileDescriptorSet bool,
	excludeImports bool,
) (retErr error) {
	ctx, span := trace.StartSpan(ctx, "put_image")
	defer span.End()
	// stop short for performance
	if imageRef.IsNull() {
		return nil
	}
	writeImage := image
	if excludeImports {
		writeImage = bufimage.ImageWithoutImports(image)
	}
	var message proto.Message
	if asFileDescriptorSet {
		message = bufimage.ImageToFileDescriptorSet(writeImage)
	} else {
		message = bufimage.ImageToProtoImage(writeImage)
	}
	data, err := i.imageMarshal(ctx, message, image, imageRef.ImageEncoding())
	if err != nil {
		return err
	}
	writeCloser, err := i.fetchWriter.PutImageFile(ctx, container, imageRef)
	if err != nil {
		return err
	}
	defer func() {
		retErr = multierr.Append(retErr, writeCloser.Close())
	}()
	_, err = writeCloser.Write(data)
	return err
}

func (i *imageWriter) imageMarshal(
	ctx context.Context,
	message proto.Message,
	image bufimage.Image,
	imageEncoding buffetch.ImageEncoding,
) ([]byte, error) {
	_, span := trace.StartSpan(ctx, "image_marshal")
	defer span.End()
	switch imageEncoding {
	case buffetch.ImageEncodingBin:
		return protoencoding.NewWireMarshaler().Marshal(message)
	case buffetch.ImageEncodingJSON:
		// TODO: verify that image is complete
		resolver, err := protoencoding.NewResolver(
			bufimage.ImageToFileDescriptors(
				image,
			)...,
		)
		if err != nil {
			return nil, err
		}
		return protoencoding.NewJSONMarshaler(resolver).Marshal(message)
	default:
		return nil, fmt.Errorf("unknown image encoding: %v", imageEncoding)
	}
}
