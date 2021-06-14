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
	"strconv"
	"strings"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log/logpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/gostdlib/go/format"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

type reInfos struct {
	reCnt    int
	infos    []reInfo
	reToName map[string]string
}

type reInfo struct {
	ReName string
	ReDef  string
}

type catInfo struct {
	Title      string
	Comment    string
	LogChannel string
	EventNames []string
	Events     []*eventInfo
}

type enumInfo struct {
	Comment string
	GoType  string
	Values  []enumValInfo
}

type enumValInfo struct {
	Comment string
	Name    string
	Value   int
}

type eventInfo struct {
	Comment         string
	LogChannel      string
	GoType          string
	Type            string
	Fields          []fieldInfo
	InheritedFields []fieldInfo
	AllFields       []fieldInfo
}

type fieldInfo struct {
	Comment             string
	FieldType           string
	FieldName           string
	AlwaysReportingSafe bool
	ReportingSafeRe     string
	MixedRedactable     bool
	Inherited           bool
	IsEnum              bool
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
	tmpl, err := template.New(tmplName).Funcs(tmplFuncs).Parse(tmplSrc)
	if err != nil {
		return errors.Wrap(err, tmplName)
	}

	// Read the input .proto file.
	info := map[string]*eventInfo{}
	enums := map[string]*enumInfo{}
	cats := map[string]*catInfo{}
	regexps := reInfos{reToName: map[string]string{}}
	for i := 2; i < len(os.Args); i++ {
		if err := readInput(&regexps, enums, info, cats, os.Args[i]); err != nil {
			return err
		}
	}

	var keys []string
	for k := range cats {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sortedInfos []*eventInfo
	var sortedCats []*catInfo
	for _, k := range keys {
		cat := cats[k]
		sort.Strings(cat.EventNames)
		for _, evname := range cat.EventNames {
			ev := info[evname]
			cat.Events = append(cat.Events, ev)
			sortedInfos = append(sortedInfos, ev)
		}
		sortedCats = append(sortedCats, cat)
	}

	keys = nil
	for k := range info {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var allSortedInfos []*eventInfo
	for _, k := range keys {
		allSortedInfos = append(allSortedInfos, info[k])
	}

	keys = nil
	for k := range enums {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var allSortedEnums []*enumInfo
	for _, k := range keys {
		allSortedEnums = append(allSortedEnums, enums[k])
	}

	// Render the template.
	var src bytes.Buffer
	if err := tmpl.Execute(&src, struct {
		AllRegexps []reInfo
		Categories []*catInfo
		Events     []*eventInfo
		AllEvents  []*eventInfo
		Enums      []*enumInfo
	}{regexps.infos, sortedCats, sortedInfos, allSortedInfos, allSortedEnums}); err != nil {
		return err
	}

	// If we are generating a .go file, do a pass of gofmt.
	newBytes := src.Bytes()
	if strings.HasSuffix(tmplName, "_go") {
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

func readInput(
	regexps *reInfos,
	enums map[string]*enumInfo,
	infos map[string]*eventInfo,
	cats map[string]*catInfo,
	protoName string,
) error {
	protoData, err := ioutil.ReadFile(protoName)
	if err != nil {
		return err
	}
	inMsg := false
	inEnum := false
	comment := ""
	channel := ""
	var curCat *catInfo
	var curMsg *eventInfo
	var curEnum *enumInfo
	for _, line := range strings.Split(string(protoData), "\n") {
		line = strings.TrimSpace(line)

		if strings.HasPrefix(line, "//") {
			comment += strings.TrimSpace(line[2:]) + "\n"
			continue
		}

		if line == "" {
			if strings.HasPrefix(comment, "Category:") {
				lines := strings.SplitN(comment, "\n", 3)
				if len(lines) < 3 || !strings.HasPrefix(lines[1], "Channel:") {
					return errors.New("invalid category comment: missing Channel specification")
				}
				title := strings.TrimSpace(strings.SplitN(lines[0], ":", 2)[1])
				channel = strings.TrimSpace(strings.SplitN(lines[1], ":", 2)[1])
				if _, ok := logpb.Channel_value[channel]; !ok {
					return errors.Newf("unknown channel name: %q", channel)
				}
				curCat = &catInfo{
					Title:      title,
					Comment:    strings.TrimSpace(strings.Join(lines[2:], "\n")),
					LogChannel: channel,
				}
				cats[title] = curCat
			}

			comment = ""
			continue
		}

		if !inEnum && !inMsg && strings.HasPrefix(line, "enum ") {
			inEnum = true

			typ := strings.Split(line, " ")[1]
			if _, ok := enums[typ]; ok {
				return errors.Newf("duplicate enum type: %q", typ)
			}

			curEnum = &enumInfo{
				Comment: comment,
				GoType:  typ,
			}
			comment = ""
			enums[typ] = curEnum
			continue
		}
		if inEnum {
			if strings.HasPrefix(line, "}") {
				inEnum = false
				comment = ""
				continue
			}

			// At this point, we don't support definitions that don't fit on a single line.
			if !strings.Contains(line, ";") {
				return errors.Newf("enum value definition must not span multiple lines: %q", line)
			}

			if !enumValDefRe.MatchString(line) {
				return errors.Newf("invalid enum value definition: %q", line)
			}

			tag := enumValDefRe.ReplaceAllString(line, "$tag")
			val := enumValDefRe.ReplaceAllString(line, "$val")
			vali, err := strconv.Atoi(val)
			if err != nil {
				return errors.Wrapf(err, "parsing %q", line)
			}

			comment = strings.TrimSpace(strings.TrimPrefix(comment, tag))

			curEnum.Values = append(curEnum.Values, enumValInfo{
				Comment: comment,
				Name:    tag,
				Value:   vali,
			})
			comment = ""
		}

		if !inMsg && !inEnum && strings.HasPrefix(line, "message ") {
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
				Comment:    comment,
				GoType:     typ,
				Type:       snakeType,
				LogChannel: channel,
			}
			comment = ""
			infos[typ] = curMsg
			if !strings.HasPrefix(typ, "Common") {
				if curCat == nil {
					return errors.New("missing category specification at top of file")
				}

				curCat.EventNames = append(curCat.EventNames, typ)
			}

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
				curMsg.AllFields = append(curMsg.AllFields, fieldInfo{
					FieldType: typ,
					FieldName: typ,
					Inherited: true,
				})
			} else {
				_, isEnum := enums[typ]

				name := snakeToCamel(fieldDefRe.ReplaceAllString(line, "$name"))
				alwayssafe := false
				mixed := false
				if nameOverride := fieldDefRe.ReplaceAllString(line, "$noverride"); nameOverride != "" {
					name = nameOverride
				}
				// redact:"nonsensitive" - always safe for reporting.
				if reportingSafe := fieldDefRe.ReplaceAllString(line, "$reportingsafe"); reportingSafe != "" {
					alwayssafe = true
					if reportingSafe == "mixed" {
						mixed = true
					}
				}
				// Certain types are also always safe for reporting.
				if !alwayssafe && isSafeType(typ) {
					alwayssafe = true
				}
				// redact:"safeif:<regexp>" - safe for reporting if the string matches the regexp.
				safeReName := ""
				if re := fieldDefRe.ReplaceAllString(line, "$safeif"); re != "" {
					// We're reading the regular expression from the .proto source, so we must
					// take care of string un-escaping ourselves. If this code ever improves
					// to apply as a protobuf plugin, this step can be removed.
					re, err := strconv.Unquote(`"` + re + `"`)
					if err != nil {
						return errors.Wrapf(err, "error while unquoting regexp at %q", line)
					}
					safeRe := "^" + re + "$"
					// Syntax check on regexp.
					_, err = regexp.Compile(safeRe)
					if err != nil {
						return errors.Wrapf(err, "regexp %s is invalid (%q)", re, line)
					}
					// We want to reuse the regexp variables across fields if the regexps are the same.
					if n, ok := regexps.reToName[safeRe]; ok {
						safeReName = n
					} else {
						regexps.reCnt++
						safeReName = fmt.Sprintf("safeRe%d", regexps.reCnt)
						regexps.reToName[safeRe] = safeReName
						regexps.infos = append(regexps.infos, reInfo{ReName: safeReName, ReDef: safeRe})
					}
				}
				fi := fieldInfo{
					Comment:             comment,
					FieldType:           typ,
					FieldName:           name,
					AlwaysReportingSafe: alwayssafe,
					ReportingSafeRe:     safeReName,
					MixedRedactable:     mixed,
					IsEnum:              isEnum,
				}
				curMsg.Fields = append(curMsg.Fields, fi)
				curMsg.AllFields = append(curMsg.AllFields, fi)
			}
			comment = ""
		}
	}

	return nil
}

func isSafeType(typ string) bool {
	switch typ {
	case "timestamp", "int32", "int64", "int16", "uint32", "uint64", "uint16", "bool", "float", "double":
		return true
	}
	return false
}

var enumValDefRe = regexp.MustCompile(`\s*(?P<tag>[_A-Z0-9]+)[^=]*=[^0-9]*(?P<val>[0-9]+).*;`)

var fieldDefRe = regexp.MustCompile(`\s*(?P<typ>[a-z._A-Z0-9]+)` +
	`\s+(?P<name>[a-z_]+)` +
	`(;|` +
	`\s+(.*customname\) = "(?P<noverride>[A-Za-z]+)")?` +
	`(.*"redact:\\"(?P<reportingsafe>nonsensitive|mixed)\\"")?` +
	`(.*"redact:\\"safeif:(?P<safeif>([^\\]|\\[^"])+)\\"")?` +
	`).*$`)

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
	"json_encode_go": `// Code generated by gen.go. DO NOT EDIT.

package eventpb

import (
  "strconv"
  "regexp"

  "github.com/cockroachdb/redact"
  "github.com/cockroachdb/cockroach/pkg/util/jsonbytes"
)

{{range .AllRegexps}}
var {{ .ReName }} = regexp.MustCompile(` + "`{{ .ReDef }}`" + `)
{{end}}

{{range .AllEvents}}
// AppendJSONFields implements the EventPayload interface.
func (m *{{.GoType}}) AppendJSONFields(printComma bool, b redact.RedactableBytes) (bool, redact.RedactableBytes) {
{{range .AllFields }}
   {{if .Inherited -}}
   printComma, b = m.{{.FieldName}}.AppendJSONFields(printComma, b)
   {{- else if eq .FieldType "string" -}}
   if m.{{.FieldName}} != "" {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":\""...)
     {{ if .AlwaysReportingSafe -}}
     b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(m.{{.FieldName}})))
     {{- else if ne .ReportingSafeRe "" }}
     if {{ .ReportingSafeRe }}.MatchString(m.{{.FieldName}}) {
       b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(m.{{.FieldName}})))))
     } else {
       b = append(b, redact.StartMarker()...)
       b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(m.{{.FieldName}})))))
       b = append(b, redact.EndMarker()...)
     }
     {{- else -}}
     b = append(b, redact.StartMarker()...)
     b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(m.{{.FieldName}})))))
     b = append(b, redact.EndMarker()...)
     {{- end }}
     b = append(b, '"')
   }
   {{- else if eq .FieldType "array_of_string" -}}
   if len(m.{{.FieldName}}) > 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":["...)
     for i, v := range m.{{.FieldName}} {
       if i > 0 { b = append(b, ',') }
       b = append(b, '"')
       {{ if .AlwaysReportingSafe -}}
       b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), v))
       {{- else if ne .ReportingSafeRe "" }}
       if {{ .ReportingSafeRe }}.MatchString(v) {
         b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(v)))))
       } else {
         b = append(b, redact.StartMarker()...)
         b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(v)))))
         b = append(b, redact.EndMarker()...)
       }
       {{- else -}}
       b = append(b, redact.StartMarker()...)
       b = redact.RedactableBytes(jsonbytes.EncodeString([]byte(b), string(redact.EscapeMarkers([]byte(v)))))
       b = append(b, redact.EndMarker()...)
       {{- end }}
       b = append(b, '"')
     }
     b = append(b, ']')
   }
   {{- else if eq .FieldType "bool" -}}
   if m.{{.FieldName}} {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":true"...)
   }
   {{- else if eq .FieldType "int16" "int32" "int64"}}
   if m.{{.FieldName}} != 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":"...)
     b = strconv.AppendInt(b, int64(m.{{.FieldName}}), 10)
   }
   {{- else if eq .FieldType "float"}}
   if m.{{.FieldName}} != 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":"...)
     b = strconv.AppendFloat(b, float64(m.{{.FieldName}}), 'f', -1, 32)
   }
   {{- else if eq .FieldType "double"}}
   if m.{{.FieldName}} != 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":"...)
     b = strconv.AppendFloat(b, float64(m.{{.FieldName}}), 'f', -1, 64)
   }
   {{- else if eq .FieldType "uint16" "uint32" "uint64"}}
   if m.{{.FieldName}} != 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":"...)
     b = strconv.AppendUint(b, uint64(m.{{.FieldName}}), 10)
   }
   {{- else if eq .FieldType "array_of_uint32" -}}
   if len(m.{{.FieldName}}) > 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":["...)
     for i, v := range m.{{.FieldName}} {
       if i > 0 { b = append(b, ',') }
       b = strconv.AppendUint(b, uint64(v), 10)
     }
     b = append(b, ']')
   }
   {{- else if .IsEnum }}
   if m.{{.FieldName}} != 0 {
     if printComma { b = append(b, ',')}; printComma = true
     b = append(b, "\"{{.FieldName}}\":"...)
     b = strconv.AppendInt(b, int64(m.{{.FieldName}}), 10)
   }
   {{- else}}
   {{ error  .FieldType }}
   {{- end}}
{{end}}
   return printComma, b
}
{{end}}


`,

	"eventlog_channels_go": `// Code generated by gen.go. DO NOT EDIT.

package eventpb

import "github.com/cockroachdb/cockroach/pkg/util/log/logpb"

{{range .Events}}
// LoggingChannel implements the EventPayload interface.
func (m *{{.GoType}}) LoggingChannel() logpb.Channel { return logpb.Channel_{{.LogChannel}} }
{{end}}
`,

	"eventlog.md": `Certain notable events are reported using a structured format.
Commonly, these notable events are also copied to the table
` + "`system.eventlog`" + `, unless the cluster setting
` + "`server.eventlog.enabled`" + ` is unset.

Additionally, notable events are copied to specific external logging
channels in log messages, where they can be collected for further processing.

The sections below document the possible notable event types
in this version of CockroachDB. For each event type, a table
documents the possible fields. A field may be omitted from
an event if its value is empty or zero.

A field is also considered "Sensitive" if it may contain
application-specific information or personally identifiable information (PII). In that case,
the copy of the event sent to the external logging channel
will contain redaction markers in a format that is compatible
with the redaction facilities in ` + "[`cockroach debug zip`](cockroach-debug-zip.html)" + `
and ` + "[`cockroach debug merge-logs`](cockroach-debug-merge-logs.html)" + `,
provided the ` + "`redactable`" + ` functionality is enabled on the logging sink.

Events not documented on this page will have an unstructured format in log messages.

{{range .Categories -}}
## {{.Title}}

{{.Comment}}

Events in this category are logged to the ` + "`" + `{{.LogChannel}}` + "`" + ` channel.

{{range .Events}}
### ` + "`" + `{{.Type}}` + "`" + `

{{.Comment}}

{{if .Fields -}}
| Field | Description | Sensitive |
|--|--|--|
{{range .Fields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }}{{- if .IsEnum }} See below for possible values for type ` + "`" + `{{- .FieldType -}}` + "`" + `.{{- end }} | {{ if .MixedRedactable }}partially{{ else if .AlwaysReportingSafe }}no{{else if ne .ReportingSafeRe "" }}depends{{else}}yes{{end}} |
{{end}}
{{- end}}

{{if .InheritedFields -}}
#### Common fields

| Field | Description | Sensitive |
|--|--|--|
{{range .InheritedFields -}}
| ` + "`" + `{{- .FieldName -}}` + "`" + ` | {{ .Comment | tableCell }} | {{ if .MixedRedactable }}partially{{ else if .AlwaysReportingSafe }}no{{else if ne .ReportingSafeRe "" }}depends{{else}}yes{{end}} |
{{end}}
{{- end}}

{{- end}}
{{end}}

{{if .Enums}}
## Enumeration types
{{range .Enums}}
### ` + "`" + `{{ .GoType }}` + "`" + `

{{ .Comment }}

| Value | Textual alias in code or documentation | Description |
|--|--|--|
{{range .Values -}}
| {{ .Value }} | {{ .Name }} | {{ .Comment | tableCell }} |
{{end}}
{{end}}
{{end}}
`,
}
