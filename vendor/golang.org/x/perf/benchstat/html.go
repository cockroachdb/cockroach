// Copyright 2017 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package benchstat

import (
	"bytes"
	"html/template"
	"strings"
)

var htmlTemplate = template.Must(template.New("").Funcs(htmlFuncs).Parse(`
{{- if . -}}
{{with index . 0}}
<table class='benchstat {{if .OldNewDelta}}oldnew{{end}}'>
{{if eq (len .Configs) 1}}
{{- else -}}
<tr class='configs'><th>{{range .Configs}}<th>{{.}}{{end}}
{{end}}
{{end}}
{{- range $i, $table := .}}
<tbody>
{{if eq (len .Configs) 1}}
<tr><th><th>{{.Metric}}
{{else -}}
<tr><th><th colspan='{{len .Configs}}' class='metric'>{{.Metric}}{{if .OldNewDelta}}<th>delta{{end}}
{{end}}{{range $group := group $table.Rows -}}
{{if and (gt (len $table.Groups) 1) (len (index . 0).Group)}}<tr class='group'><th colspan='{{colspan (len $table.Configs) $table.OldNewDelta}}'>{{(index . 0).Group}}{{end}}
{{- range $row := . -}}
{{if $table.OldNewDelta -}}
<tr class='{{if eq .Change 1}}better{{else if eq .Change -1}}worse{{else}}unchanged{{end}}'>
{{- else -}}
<tr>
{{- end -}}
<td>{{.Benchmark}}{{range .Metrics}}<td>{{.Format $row.Scaler}}{{end}}{{if $table.OldNewDelta}}<td class='{{if eq .Delta "~"}}nodelta{{else}}delta{{end}}'>{{replace .Delta "-" "−" -1}}<td class='note'>{{.Note}}{{end}}
{{end -}}
{{- end -}}
<tr><td>&nbsp;
</tbody>
{{end}}
</table>
{{end -}}
`))

var htmlFuncs = template.FuncMap{
	"replace": strings.Replace,
	"group":   htmlGroup,
	"colspan": htmlColspan,
}

func htmlColspan(configs int, delta bool) int {
	if delta {
		configs++
	}
	return configs + 1
}

func htmlGroup(rows []*Row) (out [][]*Row) {
	var group string
	var cur []*Row
	for _, r := range rows {
		if r.Group != group {
			group = r.Group
			if len(cur) > 0 {
				out = append(out, cur)
				cur = nil
			}
		}
		cur = append(cur, r)
	}
	if len(cur) > 0 {
		out = append(out, cur)
	}
	return
}

// FormatHTML appends an HTML formatting of the tables to buf.
func FormatHTML(buf *bytes.Buffer, tables []*Table) {
	err := htmlTemplate.Execute(buf, tables)
	if err != nil {
		// Only possible errors here are template not matching data structure.
		// Don't make caller check - it's our fault.
		panic(err)
	}
}
