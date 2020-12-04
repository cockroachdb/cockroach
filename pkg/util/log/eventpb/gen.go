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

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/gostdlib/go/format"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

type eventInfo struct {
	Comment         string
	Type            string
	Fields          []fieldInfo
	InheritedFields []fieldInfo
}

type fieldInfo struct {
	Comment   string
	FieldType string
	FieldName string
}

func run() error {
	if len(os.Args) < 3 {
		return errors.Newf("usage: %s <template> <protos...>\n", os.Args[0])
	}

	// Which template are we running?
	tmplName := os.Args[1]
	tmplSrc, ok := templates[tmplName]
	if !ok {
		return errors.Newf("unknown template: %q", tmplName)
	}
	tmplFuncs := template.FuncMap{
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
	tmpl, err := template.New(tmplName).Funcs(tmplFuncs).Parse(tmplSrc)
	if err != nil {
		return errors.Wrap(err, tmplName)
	}

	// Read the input .proto file.
	info := map[string]*eventInfo{}
	for i := 2; i < len(os.Args); i++ {
		if err := readInput(info, os.Args[i]); err != nil {
			return err
		}
	}

	var keys []string
	for k := range info {
		if strings.HasPrefix(k, "Common") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sortedInfos []*eventInfo
	for _, k := range keys {
		sortedInfos = append(sortedInfos, info[k])
	}

	// Render the template.
	var src bytes.Buffer
	if err := tmpl.Execute(&src, struct {
		Events []*eventInfo
	}{sortedInfos}); err != nil {
		return err
	}

	// If we are generating a .go file, do a pass of gofmt.
	newBytes := src.Bytes()
	if strings.HasSuffix(tmplName, ".go") {
		newBytes, err = format.Source(newBytes)
		if err != nil {
			return errors.Wrap(err, "gofmt")
		}
	}

	// Write the output file.
	w := os.Stdout
	if _, err := w.Write(newBytes); err != nil {
		return err
	}

	return nil
}

func readInput(infos map[string]*eventInfo, protoName string) error {
	protoData, err := ioutil.ReadFile(protoName)
	if err != nil {
		return err
	}
	inMsg := false
	comment := ""
	var curMsg *eventInfo
	for _, line := range strings.Split(string(protoData), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			comment = ""
			continue
		}

		if strings.HasPrefix(line, "//") {
			comment += strings.TrimSpace(line[2:]) + "\n"
			continue
		}

		if !inMsg && strings.HasPrefix(line, "message ") {
			inMsg = true

			typ := strings.Split(line, " ")[1]
			if _, ok := infos[typ]; ok {
				return errors.Newf("duplicate message type: %q", typ)
			}
			snakeType := camelToSnake(typ)

			if strings.HasPrefix(comment, typ) {
				comment = "An event of type `" + snakeType + "`" + strings.TrimPrefix(comment, typ)
			}
			curMsg = &eventInfo{
				Comment: comment,
				Type:    snakeType,
			}
			comment = ""
			infos[typ] = curMsg

			continue
		}
		if inMsg {
			if strings.HasPrefix(line, "}") {
				inMsg = false
				comment = ""
				continue
			}

			// At this point, we don't support definitions that don't fit on a single line.
			if !strings.Contains(line, ";") {
				return errors.Newf("field definition must not span multiple lines: %q", line)
			}

			// A field.
			if strings.HasPrefix(line, "repeated") {
				line = "array_of_" + strings.TrimSpace(strings.TrimPrefix(line, "repeated"))
			}
			if !fieldDefRe.MatchString(line) {
				return errors.Newf("unknown field definition syntax: %q", line)
			}

			typ := fieldDefRe.ReplaceAllString(line, "$typ")
			if typ == "google.protobuf.Timestamp" {
				typ = "timestamp"
			}

			if otherMsg, ok := infos[typ]; ok {
				// Inline the fields from the other messages here.
				curMsg.InheritedFields = append(curMsg.InheritedFields, otherMsg.Fields...)
			} else {
				name := snakeToCamel(fieldDefRe.ReplaceAllString(line, "$name"))
				if nameOverride := fieldDefRe.ReplaceAllString(line, "$noverride"); nameOverride != "" {
					name = nameOverride
				}
				curMsg.Fields = append(curMsg.Fields, fieldInfo{
					Comment:   comment,
					FieldType: typ,
					FieldName: name,
				})
			}
			comment = ""
		}
	}

	return nil
}

var fieldDefRe = regexp.MustCompile(`\s*(?P<typ>[a-z.A-Z0-9]+)\s+(?P<name>[a-z_]+)(;|\s+(.*customname\) = "(?P<noverride>[A-Za-z]+)")?).*$`)

func camelToSnake(typeName string) string {
	var res strings.Builder
	res.WriteByte(typeName[0] + 'a' - 'A')
	for i := 1; i < len(typeName); i++ {
		if typeName[i] >= 'A' && typeName[i] <= 'Z' {
			res.WriteByte('_')
			res.WriteByte(typeName[i] + 'a' - 'A')
		} else {
			res.WriteByte(typeName[i])
		}
	}
	return res.String()
}

func snakeToCamel(typeName string) string {
	var res strings.Builder
	res.WriteByte(typeName[0] + 'A' - 'a')
	for i := 1; i < len(typeName); i++ {
		if typeName[i] == '_' {
			i++
			res.WriteByte(typeName[i] + 'A' - 'a')
		} else {
			res.WriteByte(typeName[i])
		}
	}
	return res.String()
}

var templates = map[string]string{
	"eventlog.md": `# Cluster events

{{range .Events}}
## ` + "`" + `{{.Type}}` + "`" + `

{{.Comment}}

{{if .Fields -}}
| Field | Description |
|--|--|
{{range .Fields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }} |
{{end}}
{{- end}}

{{if .InheritedFields -}}
### Inherited fields

| Field | Description |
|--|--|
{{range .InheritedFields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }} |
{{end}}
{{- end}}

{{- end}}
`,
}
