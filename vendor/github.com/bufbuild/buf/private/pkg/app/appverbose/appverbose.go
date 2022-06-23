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

package appverbose

import (
	"io"

	"github.com/bufbuild/buf/private/pkg/verbose"
)

// Container is a container.
type Container interface {
	VerbosePrinter() verbose.Printer
}

// NewContainer returns a new Container.
func NewContainer(verbosePrinter verbose.Printer) Container {
	return newContainer(verbosePrinter)
}

// NewVerbosePrinter returns a new verbose.Printer depending on the value of verboseValue.
func NewVerbosePrinter(writer io.Writer, prefix string, verboseValue bool) verbose.Printer {
	if verboseValue {
		return verbose.NewWritePrinter(writer, prefix)
	}
	return verbose.NopPrinter
}
