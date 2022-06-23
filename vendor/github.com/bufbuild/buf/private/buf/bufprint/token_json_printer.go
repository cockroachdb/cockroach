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

package bufprint

import (
	"context"
	"io"

	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
)

type tokenJSONPrinter struct {
	writer io.Writer
}

func newTokenJSONPrinter(writer io.Writer) *tokenJSONPrinter {
	return &tokenJSONPrinter{
		writer: writer,
	}
}

func (p *tokenJSONPrinter) PrintTokens(
	ctx context.Context,
	tokens ...*registryv1alpha1.Token,
) error {
	for _, token := range tokens {
		if err := printProtoMessageJSON(p.writer, token); err != nil {
			return err
		}
	}
	return nil
}
