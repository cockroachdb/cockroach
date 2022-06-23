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
	"time"

	registryv1alpha1 "github.com/bufbuild/buf/private/gen/proto/go/buf/alpha/registry/v1alpha1"
)

type tokenTextPrinter struct {
	writer io.Writer
}

func newTokenTextPrinter(writer io.Writer) *tokenTextPrinter {
	return &tokenTextPrinter{
		writer: writer,
	}
}

func (p *tokenTextPrinter) PrintTokens(ctx context.Context, tokens ...*registryv1alpha1.Token) error {
	if len(tokens) == 0 {
		return nil
	}
	return WithTabWriter(
		p.writer,
		[]string{
			"ID",
			"Note",
			"Create time",
			"Expire time",
		},
		func(tabWriter TabWriter) error {
			for _, token := range tokens {
				if err := tabWriter.Write(
					token.Id,
					token.Note,
					token.CreateTime.AsTime().Format(time.RFC3339),
					token.ExpireTime.AsTime().Format(time.RFC3339),
				); err != nil {
					return err
				}
			}
			return nil
		},
	)
}
