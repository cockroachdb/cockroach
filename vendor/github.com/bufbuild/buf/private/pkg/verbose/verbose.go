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

package verbose

import (
	"fmt"
	"io"
	"strings"
)

var (
	// NopPrinter is a no-op printer.
	//
	// This generally aligns with the --verbose flag not being set.
	NopPrinter = nopPrinter{}
)

// Printer prints verbose messages.
type Printer interface {
	// Printf prints a new verbose message.
	//
	// Leading and trailing newlines are not respected.
	//
	// Callers should not rely on the print calls being reliable, i.e. errors to
	// a backing Writer will be ignored.
	Printf(format string, args ...interface{})
}

// NewWritePrinter returns a new Printer using the given Writer.
//
// The trimmed prefix is printed with a : before each line.
//
// This generally aligns with the --verbose flag being set and writer being stderr.
func NewWritePrinter(writer io.Writer, prefix string) Printer {
	return newWritePrinter(writer, prefix)
}

type nopPrinter struct{}

func (nopPrinter) Printf(string, ...interface{}) {}

type writePrinter struct {
	writer io.Writer
	prefix string
}

func newWritePrinter(writer io.Writer, prefix string) *writePrinter {
	prefix = strings.TrimSpace(prefix)
	if prefix != "" {
		prefix = prefix + ": "
	}
	return &writePrinter{
		writer: writer,
		prefix: prefix,
	}
}

func (w *writePrinter) Printf(format string, args ...interface{}) {
	if value := strings.TrimSpace(fmt.Sprintf(format, args...)); value != "" {
		// Errors are ignored per the interface spec.
		_, _ = w.writer.Write([]byte(w.prefix + value + "\n"))
	}
}
