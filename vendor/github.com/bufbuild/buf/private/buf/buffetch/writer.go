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

package buffetch

import (
	"context"
	"io"

	"github.com/bufbuild/buf/private/buf/buffetch/internal"
	"github.com/bufbuild/buf/private/pkg/app"
	"go.uber.org/zap"
)

type writer struct {
	internalWriter internal.Writer
}

func newWriter(
	logger *zap.Logger,
) *writer {
	return &writer{
		internalWriter: internal.NewWriter(
			logger,
			internal.WithWriterLocal(),
			internal.WithWriterStdio(),
		),
	}
}

func (w *writer) PutImageFile(
	ctx context.Context,
	container app.EnvStdoutContainer,
	imageRef ImageRef,
) (io.WriteCloser, error) {
	return w.internalWriter.PutFile(ctx, container, imageRef.internalFileRef())
}
