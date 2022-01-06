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
		return "", fmt.Errorf("cannot parse template: %s", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %s", err)
	}
	return buf.String(), nil
}

// templateToHTML is helper function to execute a template using the html/template package
func templateToHTML(templateText string, args interface{}) (string, error) {
	templ, err := htmltemplate.New("").Parse(templateText)
	if err != nil {
		return "", fmt.Errorf("cannot parse template: %s", err)
	}

	var buf bytes.Buffer
	err = templ.Execute(&buf, args)
	if err != nil {
		return "", fmt.Errorf("cannot execute template: %s", err)
	}
	return buf.String(), nil
}
