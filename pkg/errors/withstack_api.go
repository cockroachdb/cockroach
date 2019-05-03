// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package errors

import "github.com/cockroachdb/cockroach/pkg/errors/withstack"

// WithStack forwards a definition.
var WithStack func(err error) error = withstack.WithStack

// WithStackDepth forwards a definition.
var WithStackDepth func(err error, depth int) error = withstack.WithStackDepth

// ReportableStackTrace forwards a definition.
type ReportableStackTrace = withstack.ReportableStackTrace

// GetOneLineSource forwards a definition.
var GetOneLineSource func(err error) (file string, line int, fn string, ok bool) = withstack.GetOneLineSource

// GetReportableStackTrace forwards a definition.
var GetReportableStackTrace func(err error) *ReportableStackTrace = withstack.GetReportableStackTrace
