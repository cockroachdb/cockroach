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

// Package appproto contains helper functionality for protoc plugins.
//
// Note this is currently implicitly tested through buf's protoc command.
// If this were split out into a separate package, testing would need to be moved to this package.
package appproto

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"unicode"
	"unicode/utf8"

	"github.com/bufbuild/buf/private/pkg/app"
	"github.com/bufbuild/buf/private/pkg/protodescriptor"
	"github.com/bufbuild/buf/private/pkg/protoencoding"
	"github.com/bufbuild/buf/private/pkg/storage"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/pluginpb"
)

const (
	// Our generated files in `private/gen/proto` are on average 15KB which isn't
	// an unreasonable amount of memory to reserve each time we process an insertion
	// point and will save a significant number of allocations.
	averageGeneratedFileSize = 15 * 1024
	// We don't use insertion points internally, but assume they are smaller than
	// entire generated files.
	averageInsertionPointSize = 1024
)

// ResponseWriter handles CodeGeneratorResponses.
type ResponseWriter interface {
	// Add adds the file to the response.
	//
	// Returns error if nil or the name is empty.
	// Warns to stderr if the name is already added or the name is not normalized.
	AddFile(*pluginpb.CodeGeneratorResponse_File) error
	// AddError adds the error message to the response.
	//
	// If there is an existing error message, this will be concatenated with a newline.
	// If message is empty, a message "error" will be added.
	AddError(message string)
	// SetFeatureProto3Optional sets the proto3 optional feature.
	SetFeatureProto3Optional()
	// ToResponse returns the resulting CodeGeneratorResponse. This must
	// only be called after all writing has been completed.
	ToResponse() *pluginpb.CodeGeneratorResponse
}

// Handler is a protoc plugin handler
type Handler interface {
	// Handle handles the plugin.
	//
	// This function can assume the request is valid.
	// This should only return error on system error.
	// Plugin generation errors should be added with AddError.
	// See https://github.com/protocolbuffers/protobuf/blob/95e6c5b4746dd7474d540ce4fb375e3f79a086f8/src/google/protobuf/compiler/plugin.proto#L100
	Handle(
		ctx context.Context,
		container app.EnvStderrContainer,
		responseWriter ResponseWriter,
		request *pluginpb.CodeGeneratorRequest,
	) error
}

// HandlerFunc is a handler function.
type HandlerFunc func(
	context.Context,
	app.EnvStderrContainer,
	ResponseWriter,
	*pluginpb.CodeGeneratorRequest,
) error

// Handle implements Handler.
func (h HandlerFunc) Handle(
	ctx context.Context,
	container app.EnvStderrContainer,
	responseWriter ResponseWriter,
	request *pluginpb.CodeGeneratorRequest,
) error {
	return h(ctx, container, responseWriter, request)
}

// Main runs the plugin using app.Main and the Handler.
func Main(ctx context.Context, handler Handler) {
	app.Main(ctx, newRunFunc(handler))
}

// Run runs the plugin using app.Main and the Handler.
//
// The exit code can be determined using app.GetExitCode.
func Run(ctx context.Context, container app.Container, handler Handler) error {
	return app.Run(ctx, container, newRunFunc(handler))
}

// Generator executes the Handler using protoc's plugin execution logic.
//
// This invokes a Handler and writes out the response to the output location,
// additionally accounting for insertion point logic.
//
// If multiple requests are specified, these are executed in parallel and the
// result is combined into one response that is written.
type Generator interface {
	// Generate generates to the bucket.
	Generate(
		ctx context.Context,
		container app.EnvStderrContainer,
		writeBucket storage.WriteBucket,
		requests []*pluginpb.CodeGeneratorRequest,
		options ...GenerateOption,
	) error
}

// GenerateOption is an option for Generate.
type GenerateOption func(*generateOptions)

// GenerateWithInsertionPointReadBucket returns a new GenerateOption that uses the given
// ReadBucket to read from for insertion points.
//
// If this is not specified, insertion points are not supported.
func GenerateWithInsertionPointReadBucket(
	insertionPointReadBucket storage.ReadBucket,
) GenerateOption {
	return func(generateOptions *generateOptions) {
		generateOptions.insertionPointReadBucket = insertionPointReadBucket
	}
}

// NewGenerator returns a new Generator.
func NewGenerator(
	logger *zap.Logger,
	handler Handler,
) Generator {
	return newGenerator(logger, handler)
}

// newRunFunc returns a new RunFunc for app.Main and app.Run.
func newRunFunc(handler Handler) func(context.Context, app.Container) error {
	return func(ctx context.Context, container app.Container) error {
		input, err := io.ReadAll(container.Stdin())
		if err != nil {
			return err
		}
		request := &pluginpb.CodeGeneratorRequest{}
		// We do not know the FileDescriptorSet before unmarshaling this
		if err := protoencoding.NewWireUnmarshaler(nil).Unmarshal(input, request); err != nil {
			return err
		}
		if err := protodescriptor.ValidateCodeGeneratorRequest(request); err != nil {
			return err
		}
		responseWriter := newResponseWriter(container)
		if err := handler.Handle(ctx, container, responseWriter, request); err != nil {
			return err
		}
		response := responseWriter.ToResponse()
		if err := protodescriptor.ValidateCodeGeneratorResponse(response); err != nil {
			return err
		}
		data, err := protoencoding.NewWireMarshaler().Marshal(response)
		if err != nil {
			return err
		}
		_, err = container.Stdout().Write(data)
		return err
	}
}

// NewReponseWriter returns a new ResponseWriter.
func NewResponseWriter(container app.StderrContainer) ResponseWriter {
	return newResponseWriter(container)
}

// ApplyInsertionPoint applies the insertion point defined in insertionPointFile
// to the targetFile and returns the result as []byte. The caller must ensure the
// provided targetFile matches the file requested in insertionPointFile.Name.
func ApplyInsertionPoint(
	ctx context.Context,
	insertionPointFile *pluginpb.CodeGeneratorResponse_File,
	targetFile io.Reader,
) (_ []byte, retErr error) {
	targetScanner := bufio.NewScanner(targetFile)
	match := []byte("@@protoc_insertion_point(" + insertionPointFile.GetInsertionPoint() + ")")
	postInsertionContent := bytes.NewBuffer(nil)
	postInsertionContent.Grow(averageGeneratedFileSize)
	// TODO: We should respect the line endings in the generated file. This would
	// require either targetFile being an io.ReadSeeker and in the worst case
	// doing 2 full scans of the file (if it is a single line), or implementing
	// bufio.Scanner.Scan() inline
	newline := []byte{'\n'}
	for targetScanner.Scan() {
		targetLine := targetScanner.Bytes()
		if !bytes.Contains(targetLine, match) {
			// these writes cannot fail, they will panic if they cannot
			// allocate
			_, _ = postInsertionContent.Write(targetLine)
			_, _ = postInsertionContent.Write(newline)
			continue
		}
		// For each line in then new content, apply the
		// same amount of whitespace. This is important
		// for specific languages, e.g. Python.
		whitespace := leadingWhitespace(targetLine)

		// Create another scanner so that we can seamlessly handle
		// newlines in a platform-agnostic manner.
		insertedContentScanner := bufio.NewScanner(bytes.NewBufferString(insertionPointFile.GetContent()))
		insertedContent := scanWithPrefixAndLineEnding(insertedContentScanner, whitespace, newline)
		// This write cannot fail, it will panic if it cannot
		// allocate
		_, _ = postInsertionContent.Write(insertedContent)

		// Code inserted at this point is placed immediately
		// above the line containing the insertion point, so
		// we include it last.
		// These writes cannot fail, they will panic if they cannot
		// allocate
		_, _ = postInsertionContent.Write(targetLine)
		_, _ = postInsertionContent.Write(newline)
	}

	if err := targetScanner.Err(); err != nil {
		return nil, err
	}

	// trim the trailing newline
	postInsertionBytes := postInsertionContent.Bytes()
	return postInsertionBytes[:len(postInsertionBytes)-1], nil
}

// leadingWhitespace iterates through the given string,
// and returns the leading whitespace substring, if any,
// respecting utf-8 encoding.
//
//  leadingWhitespace("\u205F   foo ") -> "\u205F   "
func leadingWhitespace(buf []byte) []byte {
	leadingSize := 0
	iterBuf := buf
	for len(iterBuf) > 0 {
		r, size := utf8.DecodeRune(iterBuf)
		// protobuf strings must always be valid UTF8
		// https://developers.google.com/protocol-buffers/docs/proto3#scalar
		// Additionally, utf8.RuneError is not a space so we'll terminate
		// and return the leading, valid, UTF8 whitespace sequence.
		if !unicode.IsSpace(r) {
			out := make([]byte, leadingSize)
			copy(out, buf)
			return out
		}
		leadingSize += size
		iterBuf = iterBuf[size:]
	}
	return buf
}

// scanWithPrefixAndLineEnding iterates over each of the given scanner's lines
// prepends prefix, and appends the newline sequence.
func scanWithPrefixAndLineEnding(scanner *bufio.Scanner, prefix []byte, newline []byte) []byte {
	result := bytes.NewBuffer(nil)
	result.Grow(averageInsertionPointSize)
	for scanner.Scan() {
		// These writes cannot fail, they will panic if they cannot
		// allocate
		_, _ = result.Write(prefix)
		_, _ = result.Write(scanner.Bytes())
		_, _ = result.Write(newline)
	}
	return result.Bytes()
}
