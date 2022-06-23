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
	"io"
	"strings"
	"text/tabwriter"
)

type tabWriter struct {
	delegate *tabwriter.Writer
}

func newTabWriter(writer io.Writer) *tabWriter {
	return &tabWriter{
		delegate: tabwriter.NewWriter(writer, 0, 0, 2, ' ', 0),
	}
}

func (t *tabWriter) Write(values ...string) error {
	if len(values) == 0 {
		return nil
	}
	_, err := t.delegate.Write([]byte(strings.Join(values, "\t") + "\n"))
	return err
}

func (t *tabWriter) Flush() error {
	return t.delegate.Flush()
}
