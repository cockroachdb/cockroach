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

// Package zaputil implements utilities for zap.
package zaputil

import (
	"io"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// NewLogger returns a new Logger.
func NewLogger(
	writer io.Writer,
	level zapcore.Level,
	encoder zapcore.Encoder,
) *zap.Logger {
	return zap.New(
		zapcore.NewCore(
			encoder,
			zapcore.Lock(zapcore.AddSync(writer)),
			zap.NewAtomicLevelAt(level),
		),
	)
}

// NewTextEncoder returns a new text Encoder.
func NewTextEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(textEncoderConfig)
}

// NewColortextEncoder returns a new colortext Encoder.
func NewColortextEncoder() zapcore.Encoder {
	return zapcore.NewConsoleEncoder(colortextEncoderConfig)
}

// NewJSONEncoder returns a new JSON encoder.
func NewJSONEncoder() zapcore.Encoder {
	return zapcore.NewJSONEncoder(jsonEncoderConfig)
}
