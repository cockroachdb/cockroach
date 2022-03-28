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
func templateToText(args interface{}, templateText string) (string, error) {
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

// templateFilesToText is helper function to execute templates using the text/template package
func templateFilesToText(args interface{}, templateFiles ...string) (string, error) {
	templ := template.Must(template.ParseFiles(templateFiles...))
	var buf bytes.Buffer
	err := templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %w", err)
	}
	return buf.String(), nil
}

// templateToHTML is helper function to execute templates using the html/template package
func templateFilesToHTML(args interface{}, templateFiles ...string) (string, error) {
	templ := htmltemplate.Must(htmltemplate.ParseFiles(templateFiles...))
	var buf bytes.Buffer
	err := templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %w", err)
	}
	return buf.String(), nil
}
