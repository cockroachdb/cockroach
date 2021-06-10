// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build ignore

package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

type sinkInfo struct {
	Comment      string
	Name         string
	AnchorName   string
	Fields       []fieldInfo
	CommonFields []fieldInfo
}

type fieldInfo struct {
	Comment   string
	FieldType string
	FieldName string
	Inherited bool
}

func run() error {
	tmplFuncs := template.FuncMap{
		// error produces an error.
		"error": func(s string) string {
			panic(errors.Newf("template error: %s", s))
		},
		// tableCell formats strings for use in a table cell. For example, it converts \n\n into <br>.
		"tableCell": func(s string) string {
			s = strings.TrimSpace(s)
			if s == "" {
				return ""
			}
			s = strings.ReplaceAll(s, "\r", "")
			// Double newlines are paragraph breaks.
			s = strings.ReplaceAll(s, "\n\n", "<br><br>")
			// Other newlines are just width wrapping and should be converted to spaces.
			s = strings.ReplaceAll(s, "\n", " ")
			return s
		},
	}
	tmpl, err := template.New("sink docs").Funcs(tmplFuncs).Parse(tmplSrc)
	if err != nil {
		return err
	}

	// Read the input .proto file.
	info := map[string]*sinkInfo{}
	if err := readInput(info); err != nil {
		return err
	}

	var keys []string
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sortedSinkInfos []*sinkInfo
	for _, k := range keys {
		if k == "CommonSinkConfig" || strings.HasSuffix(k, "Defaults") {
			// We don't want the common configuration to appear as a sink in
			// the output doc.
			continue
		}
		sortedSinkInfos = append(sortedSinkInfos, info[k])
	}

	// Render the template.
	var src bytes.Buffer
	if err := tmpl.Execute(&src, struct {
		Sinks []*sinkInfo
	}{sortedSinkInfos}); err != nil {
		return err
	}

	// Write the output file.
	w := os.Stdout
	if _, err := w.Write(src.Bytes()); err != nil {
		return err
	}

	return nil
}

func readInput(infos map[string]*sinkInfo) error {
	fileData, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	inConfig := false
	comment := ""
	title := ""
	var curSink *sinkInfo
	for _, line := range strings.Split(string(fileData), "\n") {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "//") {
			thisLine := ""
			if line != "//" {
				// Naively we may want to use just
				// strings.TrimSpace(strings.TrimPrefix(line, "//"))
				// however we need to preserve the indentation of
				// preformatted config examples.
				thisLine = strings.TrimPrefix(line, "// ")
			}
			comment += thisLine + "\n"

			if strings.HasPrefix(line, "// TITLE:") {
				// Ignore everything before the title.
				comment = ""
				title = strings.TrimSpace(strings.TrimPrefix(line, "// TITLE:"))
			}

			continue
		}

		if line == "" {
			comment = ""
			title = ""
			continue
		}

		if !inConfig && configStructRe.MatchString(line) {
			inConfig = true

			typ := strings.Split(line, " ")[1]
			if _, ok := infos[typ]; ok {
				return errors.Newf("duplicate config type: %q", typ)
			}

			name := typ
			if title != "" {
				name = title
			}

			curSink = &sinkInfo{
				Comment:    comment,
				Name:       name,
				AnchorName: strings.ReplaceAll(strings.ToLower(name), " ", "-"),
			}
			comment = ""
			title = ""
			infos[typ] = curSink

			continue
		}
		if inConfig {
			if strings.HasPrefix(line, "}") {
				inConfig = false
				comment = ""
				continue
			}

			// Is it an exported field with a yaml definition?
			if len(line) == 0 || !unicode.IsUpper(rune(line[0])) || !strings.Contains(line, "`yaml:\"") {
				// No: skip.
				comment = ""
				continue
			}

			// A field.
			if !fieldDefRe.MatchString(line) {
				return errors.Newf("unknown field definition syntax: %q", line)
			}

			typ := fieldDefRe.ReplaceAllString(line, "$typ")
			name := fieldDefRe.ReplaceAllString(line, "$name")
			goName := name
			noverride := fieldDefRe.ReplaceAllString(line, "$noverride")
			if typ == "" {
				// Embedded type.
				typ = name
			}
			if noverride != "" {
				name = noverride
			} else {
				name = camelToSnake(name)
			}

			// Truncate the comment to increase legibility.
			if strings.HasPrefix(comment, goName) {
				comment = strings.TrimSpace(strings.TrimPrefix(comment, goName))
			}
			if strings.HasPrefix(comment, "indicates ") {
				comment = strings.TrimPrefix(comment, "indicates ")
			} else if strings.HasPrefix(comment, "is ") {
				comment = strings.TrimPrefix(comment, "is ")
			}

			if otherMsg, ok := infos[typ]; ok {
				if typ == "CommonSinkConfig" {
					// Inline the fields from the other struct here.
					curSink.CommonFields = append(curSink.CommonFields, otherMsg.Fields...)
				} else {
					for _, f := range otherMsg.Fields {
						f.Comment = fmt.Sprintf(
							"%v Inherited from `%v.%v` if not specified.",
							f.Comment, camelToSnake(otherMsg.Name), f.FieldName)
						curSink.Fields = append(curSink.Fields, f)
					}
					curSink.CommonFields = append(curSink.CommonFields, otherMsg.CommonFields...)
				}
			} else {
				fi := fieldInfo{
					Comment:   comment,
					FieldType: typ,
					FieldName: name,
				}
				curSink.Fields = append(curSink.Fields, fi)
			}
			comment = ""
		}
	}

	return nil
}

var configStructRe = regexp.MustCompile(`^type (?P<name>[A-Z][a-z0-9]*)(SinkConfig|Defaults) struct`)

var fieldDefRe = regexp.MustCompile(`^\s*` +
	// Field name in Go.
	`(?P<name>[A-Z][A-Za-z_0-9]*)` +
	// Go type. Empty if embedded type.
	`(?P<typ>(?: [^ ]+)?)` +
	// Start of YAML annotation.
	" `yaml:\"" +
	// Field name override in YAML
	`(?P<noverride>[^,"]*)` +
	// End of YAML annotation.
	`[^"]*"` + "`.*")

func camelToSnake(typeName string) string {
	var res strings.Builder
	res.WriteByte(typeName[0] + 'a' - 'A')
	for i := 1; i < len(typeName); i++ {
		if typeName[i] >= 'A' && typeName[i] <= 'Z' {
			res.WriteByte('-')
			res.WriteByte(typeName[i] + 'a' - 'A')
		} else {
			res.WriteByte(typeName[i])
		}
	}
	return res.String()
}

var tmplSrc = `
The supported log output sink types are documented below.

{{range .Sinks}}
- [{{.Name}}](#{{.AnchorName}})
{{end}}

{{range .Sinks}}
<a name="{{.AnchorName}}">

## Sink type: {{.Name}}

{{.Comment}}

Type-specific configuration options:

{{if .Fields -}}
| Field | Description |
|--|--|
{{range .Fields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }}{{if eq .FieldName "channels" }} See the [channel selection configuration](#channel-format) section for details. {{end}} |
{{end}}
{{- end}}

{{if .CommonFields -}}

Configuration options shared across all sink types:

| Field | Description |
|--|--|
{{range .CommonFields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }} |
{{end}}
{{- end}}

{{end}}

<a name="channel-format">

## Channel selection configuration

Each sink can select multiple channels. The names of selected channels can
be specified as a YAML array or as a string.

Example configurations:

    # Select just these two channels. Space is important.
    channels: [OPS, HEALTH]

    # The selection is case-insensitive.
    channels: [ops, HeAlTh]

    # Same configuration, as a YAML string. Avoid space around comma
    # if using the YAML "inline" format.
    channels: OPS,HEALTH

    # Same configuration, as a quoted string.
    channels: 'OPS, HEALTH'

    # Same configuration, as a multi-line YAML array.
    channels:
    - OPS
    - HEALTH

It is also possible to select all channels, using the "all" keyword.
For example:

    channels: all
    channels: 'all'
    channels: [all]
    channels: ['all']

It is also possible to select all channels except for a subset, using the
"all except" keyword prefix. This makes it possible to define sinks
that capture "everything else". For example:

    channels: all except ops,health
    channels: all except [ops,health]
    channels: 'all except ops, health'
    channels: 'all except [ops, health]'
`
