// Copyright 2020 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
)

func init() {
	var (
		protobufPath string
		genDocPath   string
		outPath      string
	)

	cmdHTTP := &cobra.Command{
		Use:   "http",
		Short: "Generate HTTP docs",
		Run: func(cmd *cobra.Command, args []string) {
			if err := runHTTP(genDocPath, protobufPath, outPath); err != nil {
				fmt.Fprintln(os.Stdout, err)
				os.Exit(1)
			}
		},
	}
	cmdHTTP.Flags().StringVar(&protobufPath, "protobuf", "", "Protobuf include paths.")
	cmdHTTP.Flags().StringVar(&genDocPath, "gendoc", "protoc-gen-doc", "Path to protoc-gen-doc binary.")
	cmdHTTP.Flags().StringVar(&outPath, "out", "docs/generated/http", "File output path.")

	cmds = append(cmds, cmdHTTP)
}

var singleMethods = []string{
	"HotRanges",
	"Nodes",
	"Health",
}

// runHTTP extracts HTTP endpoint documentation. It does this by reading the
// status.proto file and converting it into a JSON file. This JSON file is then
// fed as data into various Go templates that are used to populate markdown
// files. A full.md file is produced with all endpoints. The singleMethods
// string slice is used to produce additional markdown files with a single
// method per file.
func runHTTP(genDocPath, protobufPath, outPath string) error {
	// Extract out all the data into a JSON file. We will use this JSON
	// file to then generate full and single pages.
	if err := os.MkdirAll(outPath, 0777); err != nil {
		return err
	}
	tmpJSON, err := ioutil.TempDir("", "docgen-*")
	if err != nil {
		return err
	}
	// gen-doc wants a file on disk to use as its template. Make and
	// cleanup a temp file.
	jsonTmpl := filepath.Join(tmpJSON, "json.tmpl")
	if err := ioutil.WriteFile(jsonTmpl, []byte(tmplJSON), 0666); err != nil {
		return err
	}
	defer func() {
		_ = os.RemoveAll(tmpJSON)
	}()
	args := []string{"protoc",
		fmt.Sprintf("--doc_out=%s", tmpJSON),
		fmt.Sprintf("--doc_opt=%s,http.json", jsonTmpl),
		fmt.Sprintf("--plugin=protoc-gen-doc=%s", genDocPath),
	}
	args = append(args, strings.Fields(protobufPath)...)
	// Generate the JSON file.
	args = append(args,
		"./pkg/server/serverpb/status.proto",
		"./pkg/server/serverpb/admin.proto",
		"./pkg/server/status/statuspb/status.proto",
	)
	cmd := exec.Command("buf", args...)
	if out, err := cmd.CombinedOutput(); err != nil {
		fmt.Println(string(out))
		return err
	}
	dataFile, err := ioutil.ReadFile(filepath.Join(tmpJSON, "http.json"))
	if err != nil {
		return err
	}
	var data protoData
	if err := json.Unmarshal(dataFile, &data); err != nil {
		return fmt.Errorf("json unmarshal: %w", err)
	}

	// Annotate all non-public message, method and field descriptions
	// with a disclaimer.
	for k := range data.Files {
		file := &data.Files[k]
		for i := range file.Messages {
			m := &file.Messages[i]
			if !(strings.HasSuffix(m.Name, "Entry") && len(m.Fields) == 2 && m.Fields[0].Name == "key" && m.Fields[1].Name == "value") {
				// We only annotate the support status for non-KV
				// (auto-generated, intermediate) message types.
				annotateStatus(&m.Description, &m.SupportStatus, "payload")
				for j := range m.Fields {
					f := &m.Fields[j]
					annotateStatus(&f.Description, &f.SupportStatus, "field")
				}
			}
		}
		for j := range file.Services {
			service := &file.Services[j]
			for i := range service.Methods {
				m := &service.Methods[i]
				annotateStatus(&m.Description, &m.SupportStatus, "endpoint")
				if len(m.Options.GoogleAPIHTTP.Rules) > 0 {
					// Just keep the last entry. This is a special accommodation for
					// "/health" which aliases "/_admin/v1/health": we only
					// want to document the latter.
					m.Options.GoogleAPIHTTP.Rules = m.Options.GoogleAPIHTTP.Rules[len(m.Options.GoogleAPIHTTP.Rules)-1:]
				}
			}
		}
	}

	// Start by making maps of methods and messages for lookup.
	messages := make(map[string]*protoMessage)
	methods := make(map[string]*protoMethod)
	for f := range data.Files {
		file := &data.Files[f]
		for i := range file.Messages {
			messages[file.Messages[i].FullName] = &file.Messages[i]
		}

		for j := range file.Services {
			service := &file.Services[j]
			for i := range service.Methods {
				methods[service.Methods[i].Name] = &service.Methods[i]
			}
		}
	}

	// Given a message type, returns all message types in its type field that are
	// also present in the messages map. Useful to recurse into types that have
	// other types.
	extraMessages := func(name string) []string {
		seen := make(map[string]bool)
		var extraFn func(name string) []string
		extraFn = func(name string) []string {
			if seen[name] {
				return nil
			}
			seen[name] = true
			msg, ok := messages[name]
			if !ok {
				return nil
			}
			var other []string
			for _, field := range msg.Fields {
				if innerMsg, ok := messages[field.FullType]; ok {
					other = append(other, field.FullType)
					other = append(other, extraFn(innerMsg.FullName)...)
				}
			}
			return other
		}
		return extraFn(name)
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
		"getMessage": func(name string) *protoMessage {
			return messages[name]
		},
		"extraMessages": extraMessages,
	}
	tmplFull := template.Must(template.New("full").Funcs(tmplFuncs).Parse(fullTemplate))
	tmplMessages := template.Must(template.New("single").Funcs(tmplFuncs).Parse(messagesTemplate))

	// JSON data is now in memory. Generate full doc page.
	if err := execHTTPTmpl(tmplFull, &data, filepath.Join(outPath, "full.md")); err != nil {
		return fmt.Errorf("execHTTPTmpl: %w", err)
	}

	for _, methodName := range singleMethods {
		method, ok := methods[methodName]
		if !ok {
			return fmt.Errorf("single method not found: %s", methodName)
		}
		for name, messages := range map[string][]string{
			"request":  {method.RequestFullType},
			"response": {method.ResponseFullType},
			"other":    append(extraMessages(method.RequestFullType), extraMessages(method.ResponseFullType)...),
		} {
			path := filepath.Join(outPath, fmt.Sprintf("%s-%s.md", strings.ToLower(methodName), name))
			if err := execHTTPTmpl(tmplMessages, messages, path); err != nil {
				return err
			}
		}
	}
	return nil
}

func annotateStatus(desc *string, status *string, kind string) {
	if !strings.Contains(*desc, "API: PUBLIC") {
		*status = `[reserved](#support-status)`
	}
	if strings.Contains(*desc, "API: PUBLIC ALPHA") {
		*status = `[alpha](#support-status)`
	}
	if *status == "" {
		*status = `[public](#support-status)`
	}
	*desc = strings.Replace(*desc, "API: PUBLIC ALPHA", "", 1)
	*desc = strings.Replace(*desc, "API: PUBLIC", "", 1)
	*desc = strings.TrimSpace(*desc)
}

func execHTTPTmpl(tmpl *template.Template, data interface{}, path string) error {
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return err
	}
	return ioutil.WriteFile(path, buf.Bytes(), 0666)
}

type protoData struct {
	Files []struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		Package       string `json:"package"`
		HasEnums      bool   `json:"hasEnums"`
		HasExtensions bool   `json:"hasExtensions"`
		HasMessages   bool   `json:"hasMessages"`
		HasServices   bool   `json:"hasServices"`
		Enums         []struct {
			Name        string `json:"name"`
			LongName    string `json:"longName"`
			FullName    string `json:"fullName"`
			Description string `json:"description"`
			Values      []struct {
				Name        string `json:"name"`
				Number      string `json:"number"`
				Description string `json:"description"`
			} `json:"values"`
		} `json:"enums"`
		Extensions []interface{}  `json:"extensions"`
		Messages   []protoMessage `json:"messages"`
		Services   []struct {
			Name        string        `json:"name"`
			LongName    string        `json:"longName"`
			FullName    string        `json:"fullName"`
			Description string        `json:"description"`
			Methods     []protoMethod `json:"methods"`
		} `json:"services"`
	} `json:"files"`
	ScalarValueTypes []struct {
		ProtoType  string `json:"protoType"`
		Notes      string `json:"notes"`
		CppType    string `json:"cppType"`
		CsType     string `json:"csType"`
		GoType     string `json:"goType"`
		JavaType   string `json:"javaType"`
		PhpType    string `json:"phpType"`
		PythonType string `json:"pythonType"`
		RubyType   string `json:"rubyType"`
	} `json:"scalarValueTypes"`
}

type protoMethod struct {
	Name              string `json:"name"`
	Description       string `json:"description"`
	SupportStatus     string `json:"supportStatus"`
	RequestType       string `json:"requestType"`
	RequestLongType   string `json:"requestLongType"`
	RequestFullType   string `json:"requestFullType"`
	RequestStreaming  bool   `json:"requestStreaming"`
	ResponseType      string `json:"responseType"`
	ResponseLongType  string `json:"responseLongType"`
	ResponseFullType  string `json:"responseFullType"`
	ResponseStreaming bool   `json:"responseStreaming"`
	Options           struct {
		GoogleAPIHTTP struct {
			Rules []struct {
				Method  string `json:"method"`
				Pattern string `json:"pattern"`
			} `json:"rules"`
		} `json:"google.api.http"`
	} `json:"options"`
}

type protoMessage struct {
	Name          string        `json:"name"`
	LongName      string        `json:"longName"`
	FullName      string        `json:"fullName"`
	Description   string        `json:"description"`
	SupportStatus string        `json:"supportStatus"`
	HasExtensions bool          `json:"hasExtensions"`
	HasFields     bool          `json:"hasFields"`
	Extensions    []interface{} `json:"extensions"`
	Fields        []struct {
		Name          string `json:"name"`
		Description   string `json:"description"`
		SupportStatus string `json:"supportStatus"`
		Label         string `json:"label"`
		Type          string `json:"type"`
		LongType      string `json:"longType"`
		FullType      string `json:"fullType"`
		Ismap         bool   `json:"ismap"`
		DefaultValue  string `json:"defaultValue"`
	} `json:"fields"`
}

const tmplJSON = `{{toPrettyJson .}}`

const fullTemplate = `
{{- define "FIELDS" -}}
{{with getMessage .}}
{{$message := .}}

{{with .Description}}{{.}}{{end}}

{{with .Fields}}
| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
{{- range .}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{.Description | tableCell}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} | {{.SupportStatus | tableCell}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* with .Fields */}}

{{/* document extra messages */}}
{{range extraMessages .FullName}}
{{with getMessage .}}
{{if .Fields}}
<a name="{{$message.FullName}}-{{.FullName}}"></a>
#### {{.LongName}}

{{with .Description}}{{.}}{{end}}

| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
{{- range .Fields}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{.Description | tableCell}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} | {{.SupportStatus | tableCell}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* if .Fields */}}
{{end}} {{- /* with getMessage */}}
{{end}} {{- /* range */}}

{{end}} {{- /* with getMessage */}}
{{- end}} {{- /* template */}}

{{- range .Files}}

{{- range .Services}}

{{- range .Methods -}}
## {{.Name}}

{{range .Options.GoogleAPIHTTP.Rules -}}
` + "`{{.Method}} {{.Pattern}}` " + `
{{- end}}

{{with .Description}}{{.}}{{end}}

{{with .SupportStatus}}Support status: {{.}}{{end}}

#### Request Parameters

{{template "FIELDS" .RequestFullType}}

#### Response Parameters

{{template "FIELDS" .ResponseFullType}}

{{end}} {{- /* methods */}}

{{- end -}} {{- /* services */ -}}
{{- end -}} {{- /* files */ -}}
`

var messagesTemplate = `
{{- range .}}
{{with getMessage .}}
<a name="{{.FullName}}"></a>
#### {{.LongName}}

{{with .Description}}{{.}}{{end}}

{{with .SupportStatus}}Support status: {{.}}{{end}}

{{with .Fields}}
| Field | Type | Label | Description | Support status |
| ----- | ---- | ----- | ----------- | -------------- |
{{- range .}}
| {{.Name}} | [{{.LongType}}](#{{.FullType}}) | {{.Label}} | {{.Description|tableCell}} | {{.SupportStatus | tableCell}} |
{{- end}} {{- /* range */}}
{{end}}{{- /* with .Fields */}}
{{end}}{{- /* with getMessage */}}
{{end}}{{- /* range */ -}}
`
