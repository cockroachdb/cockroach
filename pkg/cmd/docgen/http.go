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

	gendoc "github.com/pseudomuto/protoc-gen-doc"
	"github.com/spf13/cobra"
)

func init() {
	var (
		protocPath   string
		protobufPath string
		genDocPath   string
		outPath      string
	)

	cmdHTTP := &cobra.Command{
		Use:   "http",
		Short: "Generate HTTP docs",
		Run: func(cmd *cobra.Command, args []string) {
			if err := runHTTP(protocPath, genDocPath, protobufPath, outPath); err != nil {
				fmt.Fprintln(os.Stdout, err)
				os.Exit(1)
			}
		},
	}
	cmdHTTP.Flags().StringVar(&protocPath, "protoc", "protoc", "Path to protoc binary.")
	cmdHTTP.Flags().StringVar(&protobufPath, "protobuf", "", "Protobuf include paths.")
	cmdHTTP.Flags().StringVar(&genDocPath, "gendoc", "protoc-gen-doc", "Path to protoc-gen-doc binary.")
	cmdHTTP.Flags().StringVar(&outPath, "out", "docs/generated/http", "File output path.")

	cmds = append(cmds, cmdHTTP)
}

var singleMethods = []string{
	"HotRanges",
}

// runHTTP extracts HTTP endpoint documentation. It does this by reading the
// status.proto file and converting it into a JSON file. This JSON file is then
// fed as data into various Go templates that are used to populate markdown
// files. A full.md file is produced with all endpoints. The singleMethods
// string slice is used to produce additional markdown files with a single
// method per file.
func runHTTP(protocPath, genDocPath, protobufPath, outPath string) error {
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
	// Generate the JSON file.
	cmd := exec.Command(protocPath,
		fmt.Sprintf("-I%s", protobufPath),
		fmt.Sprintf("--plugin=protoc-gen-doc=%s", genDocPath),
		fmt.Sprintf("--doc_out=%s", tmpJSON),
		fmt.Sprintf("--doc_opt=%s,http.json", jsonTmpl),
		"./pkg/server/serverpb/status.proto",
	)
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
	if len(data.Files) != 1 || len(data.Files[0].Services) != 1 {
		return fmt.Errorf("expected 1 file with 1 service")
	}
	file := data.Files[0]
	service := file.Services[0]

	// Start by making maps of methods and messages for lookup.
	messages := make(map[string]*protoMessage)
	for i := range file.Messages {
		messages[file.Messages[i].FullName] = &file.Messages[i]
	}
	methods := make(map[string]interface{})
	for i := range service.Methods {
		methods[service.Methods[i].Name] = &service.Methods[i]
	}
	tmplFuncs := template.FuncMap{
		"nobr": gendoc.NoBrFilter,
		"getMessage": func(name string) interface{} {
			return messages[name]
		},
		// Given a message type, returns all message types in its type
		// field that are also present in the messages map. Useful to
		// recurse into types that have other types.
		"extraMessages": func(name string) []string {
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
		},
	}
	tmplFull := template.Must(template.New("full").Funcs(tmplFuncs).Parse(fullTemplate))
	tmplSingle := template.Must(template.New("single").Funcs(tmplFuncs).Parse(singleTemplate))

	// JSON data is now in memory. Generate full doc page.
	if err := execHTTPTmpl(tmplFull, &file, filepath.Join(outPath, "full.md")); err != nil {
		return fmt.Errorf("execHTTPTmpl: %w", err)
	}

	for _, methodName := range singleMethods {
		method, ok := methods[methodName]
		if !ok {
			return fmt.Errorf("single method not found: %s", methodName)
		}
		path := filepath.Join(outPath, fmt.Sprintf("%s.md", strings.ToLower(methodName)))
		if err := execHTTPTmpl(tmplSingle, method, path); err != nil {
			return err
		}
	}
	return nil
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
			Name        string `json:"name"`
			LongName    string `json:"longName"`
			FullName    string `json:"fullName"`
			Description string `json:"description"`
			Methods     []struct {
				Name              string `json:"name"`
				Description       string `json:"description"`
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
			} `json:"methods"`
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

type protoMessage struct {
	Name          string        `json:"name"`
	LongName      string        `json:"longName"`
	FullName      string        `json:"fullName"`
	Description   string        `json:"description"`
	HasExtensions bool          `json:"hasExtensions"`
	HasFields     bool          `json:"hasFields"`
	Extensions    []interface{} `json:"extensions"`
	Fields        []struct {
		Name         string `json:"name"`
		Description  string `json:"description"`
		Label        string `json:"label"`
		Type         string `json:"type"`
		LongType     string `json:"longType"`
		FullType     string `json:"fullType"`
		Ismap        bool   `json:"ismap"`
		DefaultValue string `json:"defaultValue"`
	} `json:"fields"`
}

const tmplJSON = `{{toPrettyJson .}}`

const fullTemplate = `
{{- define "FIELDS" -}}
{{with getMessage .}}
{{$message := .}}
{{with .Fields}}
| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
{{- range .}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{nobr .Description}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* with .Fields */}}

{{/* document extra messages */}}
{{range extraMessages .FullName}}
{{with getMessage .}}
{{if .Fields}}
<a name="{{$message.FullName}}-{{.FullName}}"></a>
#### {{.LongName}}

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
{{- range .Fields}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{nobr .Description}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* if .Fields */}}
{{end}} {{- /* with getMessage */}}
{{end}} {{- /* range */}}

{{end}} {{- /* with getMessage */}}
{{- end}} {{- /* template */}}

{{- range .Services}}

{{- range .Methods -}}
## {{.Name}}

{{range .Options.GoogleAPIHTTP.Rules -}}
` + "`{{.Method}} {{.Pattern}}`" + `
{{- end}}

{{with .Description}}{{.}}{{end}}

### Request Parameters

{{template "FIELDS" .RequestFullType}}

### Response Parameters

{{template "FIELDS" .ResponseFullType}}

{{end}} {{- /* methods */}}

{{- end -}} {{- /* services */ -}}
`

var singleTemplate = `
{{- define "FIELDS" -}}
{{with getMessage .}}
{{$message := .}}
{{with .Fields}}
| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
{{- range .}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{nobr .Description}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* with .Fields */}}

{{/* document extra messages */}}
{{range extraMessages .FullName}}
{{with getMessage .}}
{{if .Fields}}
<a name="{{$message.FullName}}-{{.FullName}}"></a>
#### {{.LongName}}

| Field | Type | Label | Description |
| ----- | ---- | ----- | ----------- |
{{- range .Fields}}
| {{.Name}} | [{{.LongType}}](#{{$message.FullName}}-{{.FullType}}) | {{.Label}} | {{nobr .Description}}{{if .DefaultValue}} Default: {{.DefaultValue}}{{end}} |
{{- end}} {{- /* range */}}
{{end}} {{- /* if .Fields */}}
{{end}} {{- /* with getMessage */}}
{{end}} {{- /* range */}}

{{end}} {{- /* with getMessage */}}
{{- end}} {{- /* template */}}

### Request Parameters

{{template "FIELDS" .RequestFullType}}

### Response Parameters

{{template "FIELDS" .ResponseFullType}}
`
