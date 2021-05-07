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
	"fmt"
	"os"
	"sort"
	"text/template"

	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/spf13/cobra"
)

func init() {
	cmds = append(cmds, &cobra.Command{
		Use:   "logformats",
		Short: "Generate the markdown documentation for logging formats.",
		Args:  cobra.MaximumNArgs(1),
		Run:   runLogFormats,
	})
}

func runLogFormats(_ *cobra.Command, args []string) {
	if err := runLogFormatsInternal(args); err != nil {
		fmt.Fprintln(os.Stderr, "ERROR:", err)
		exit.WithCode(exit.UnspecifiedError())
	}
}

func runLogFormatsInternal(args []string) error {
	// Compile the template.
	tmpl, err := template.New("format docs").Parse(fmtDocTemplate)
	if err != nil {
		return err
	}

	m := log.GetFormatterDocs()

	// Sort the names.
	fNames := make([]string, 0, len(m))
	for k := range m {
		fNames = append(fNames, k)
	}
	sort.Strings(fNames)

	// Retrieve the metadata into a format that the templating engine can understand.
	type info struct {
		Name string
		Doc  string
	}
	var infos []info
	for _, k := range fNames {
		infos = append(infos, info{Name: k, Doc: m[k]})
	}

	// Render the template.
	var src bytes.Buffer
	if err := tmpl.Execute(&src, struct {
		Formats []info
	}{infos}); err != nil {
		return err
	}

	// Write the output file.
	w := os.Stdout
	if len(args) > 0 {
		f, err := os.OpenFile(args[0], os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0666)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		w = f
	}
	if _, err := w.Write(src.Bytes()); err != nil {
		return err
	}

	return nil
}

const fmtDocTemplate = `
The supported log output formats are documented below.

{{range .Formats}}
- [` + "`{{.Name}}`" + `](#format-{{.Name}})
{{end}}

{{range .Formats}}
## Format ` + "`{{.Name}}`" + `

{{.Doc}}
{{end}}
`
