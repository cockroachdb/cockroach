package gendoc

import (
	"bytes"
	"encoding/json"
	"errors"
	html_template "html/template"
	text_template "text/template"

	"github.com/Masterminds/sprig"
)

// RenderType is an "enum" for which type of renderer to use.
type RenderType int8

// Available render types.
const (
	_ RenderType = iota
	RenderTypeDocBook
	RenderTypeHTML
	RenderTypeJSON
	RenderTypeMarkdown
)

// NewRenderType creates a RenderType from the supplied string. If the type is not known, (0, error) is returned. It is
// assumed (by the plugin) that invalid render type simply means that the path to a custom template was supplied.
func NewRenderType(renderType string) (RenderType, error) {
	switch renderType {
	case "docbook":
		return RenderTypeDocBook, nil
	case "html":
		return RenderTypeHTML, nil
	case "json":
		return RenderTypeJSON, nil
	case "markdown":
		return RenderTypeMarkdown, nil
	}

	return 0, errors.New("Invalid render type")
}

func (rt RenderType) renderer() (Processor, error) {
	tmpl, err := rt.template()
	if err != nil {
		return nil, err
	}

	switch rt {
	case RenderTypeDocBook:
		return &textRenderer{string(tmpl)}, nil
	case RenderTypeHTML:
		return &htmlRenderer{string(tmpl)}, nil
	case RenderTypeJSON:
		return new(jsonRenderer), nil
	case RenderTypeMarkdown:
		return &htmlRenderer{string(tmpl)}, nil
	}

	return nil, errors.New("Unable to create a processor")
}

func (rt RenderType) template() ([]byte, error) {
	switch rt {
	case RenderTypeDocBook:
		return fetchResource("docbook.tmpl")
	case RenderTypeHTML:
		return fetchResource("html.tmpl")
	case RenderTypeJSON:
		return nil, nil
	case RenderTypeMarkdown:
		return fetchResource("markdown.tmpl")
	}

	return nil, errors.New("Couldn't find template for render type")
}

var funcMap = map[string]interface{}{
	"p":    PFilter,
	"para": ParaFilter,
	"nobr": NoBrFilter,
}

// Processor is an interface that is satisfied by all built-in processors (text, html, and json).
type Processor interface {
	Apply(template *Template) ([]byte, error)
}

// RenderTemplate renders the template based on the render type. It supports overriding the default input templates by
// supplying a non-empty string as the last parameter.
//
// Example: generating an HTML template (assuming you've got a Template object)
//     data, err := RenderTemplate(RenderTypeHTML, &template, "")
//
// Example: generating a custom template (assuming you've got a Template object)
//     data, err := RenderTemplate(RenderTypeHTML, &template, "{{range .Files}}{{.Name}}{{end}}")
func RenderTemplate(kind RenderType, template *Template, inputTemplate string) ([]byte, error) {
	if inputTemplate != "" {
		processor := &textRenderer{inputTemplate}
		return processor.Apply(template)
	}

	processor, err := kind.renderer()
	if err != nil {
		return nil, err
	}

	return processor.Apply(template)
}

type textRenderer struct {
	inputTemplate string
}

func (mr *textRenderer) Apply(template *Template) ([]byte, error) {
	tmpl, err := text_template.New("Text Template").Funcs(funcMap).Funcs(sprig.TxtFuncMap()).Parse(mr.inputTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, template); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type htmlRenderer struct {
	inputTemplate string
}

func (mr *htmlRenderer) Apply(template *Template) ([]byte, error) {
	tmpl, err := html_template.New("Text Template").Funcs(funcMap).Funcs(sprig.HtmlFuncMap()).Parse(mr.inputTemplate)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, template); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

type jsonRenderer struct{}

func (r *jsonRenderer) Apply(template *Template) ([]byte, error) {
	return json.MarshalIndent(template, "", "  ")
}
