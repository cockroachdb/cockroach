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

package issuelink

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/errors/errbase"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type unimplementedError struct {
	msg string
	IssueLink
}

func (w *unimplementedError) Error() string { return w.msg }
func (w *unimplementedError) SafeDetails() []string {
	return []string{w.IssueURL, w.Detail}
}

// ErrorHint implements the hintdetail.ErrorHinter interface.
func (w *unimplementedError) ErrorHint() string {
	var hintText bytes.Buffer
	hintText.WriteString(UnimplementedErrorHint)
	maybeAppendReferral(&hintText, w.IssueLink)
	return hintText.String()
}

// UnimplementedErrorHint is the hint emitted upon unimplemented errors.
const UnimplementedErrorHint = `You have attempted to use a feature not yet implemented in CockroachDB.`

func (w *unimplementedError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%s", w.msg)
			if w.IssueURL != "" {
				fmt.Fprintf(s, "\n-- issue: %s", w.IssueURL)
			}
			if w.Detail != "" {
				fmt.Fprintf(s, "\n-- detail: %s", w.Detail)
			}
			return
		}
		fallthrough
	case 's', 'q':
		fmt.Fprintf(s, fmt.Sprintf("%%%c", verb), w.msg)
	}
}

func decodeUnimplementedError(msg string, details []string, _ protoutil.SimpleMessage) error {
	var issueLink IssueLink
	if len(details) > 0 {
		issueLink.IssueURL = details[0]
	}
	if len(details) > 1 {
		issueLink.Detail = details[1]
	}
	return &unimplementedError{msg: msg, IssueLink: issueLink}
}

func init() {
	errbase.RegisterLeafDecoder(errbase.FullTypeName(&unimplementedError{}), decodeUnimplementedError)
}
