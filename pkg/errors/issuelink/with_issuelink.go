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
	"github.com/cockroachdb/cockroach/pkg/errors/stdstrings"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

type withIssueLink struct {
	cause error
	IssueLink
}

func (w *withIssueLink) Error() string { return w.cause.Error() }
func (w *withIssueLink) Cause() error  { return w.cause }
func (w *withIssueLink) Unwrap() error { return w.cause }
func (w *withIssueLink) SafeDetails() []string {
	return []string{w.IssueURL, w.Detail}
}

// ErrorHint implements the hintdetail.ErrorHinter interface.
func (w *withIssueLink) ErrorHint() string {
	var hintText bytes.Buffer
	maybeAppendReferral(&hintText, w.IssueLink)
	return hintText.String()
}

func maybeAppendReferral(buf *bytes.Buffer, link IssueLink) {
	if link.IssueURL != "" {
		// If there is a URL, refer to that.
		if buf.Len() > 0 {
			buf.WriteByte('\n')
		}
		fmt.Fprintf(buf, "See: %s", link.IssueURL)
	} else {
		// No URL: tell the user to send details.
		buf.WriteString(stdstrings.IssueReferral)
	}
}

func (w *withIssueLink) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "%+v", w.cause)
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
		errbase.FormatError(s, verb, w.cause)
	}
}

func decodeWithIssueLink(cause error, _ string, details []string, _ protoutil.SimpleMessage) error {
	var issueLink IssueLink
	if len(details) > 0 {
		issueLink.IssueURL = details[0]
	}
	if len(details) > 1 {
		issueLink.Detail = details[1]
	}
	return &withIssueLink{cause: cause, IssueLink: issueLink}
}

func init() {
	errbase.RegisterWrapperDecoder(errbase.FullTypeName(&withIssueLink{}), decodeWithIssueLink)
}
