// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"fmt"
	htmltemplate "html/template"
	"text/template"
)

// templateToText is helper function to execute a template using the text/template package
func templateToText(templateText string, args interface{}) (string, error) {
	templ, err := template.New("").Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("cannot parse template: %w", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %w", err)
	}
	return buf.String(), nil
}

// templateToHTML is helper function to execute a template using the html/template package
func templateToHTML(templateText string, args interface{}) (string, error) {
	templ, err := htmltemplate.New("").Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("cannot parse template: %w", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %w", err)
	}
	return buf.String(), nil
}
