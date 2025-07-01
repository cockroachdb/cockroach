// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

func init() {
	cmds = append(cmds, &cobra.Command{
		Use:   "session-vars <output-dir>",
		Short: "generate markdown documentation of session variables",
		RunE: func(cmd *cobra.Command, args []string) error {
			outDir := filepath.Join("docs", "generated", "sql")
			if len(args) > 0 {
				outDir = args[0]
			}

			if stat, err := os.Stat(outDir); err != nil {
				return err
			} else if !stat.IsDir() {
				return errors.Errorf("%q is not a directory", outDir)
			}

			return os.WriteFile(
				filepath.Join(outDir, "session_vars.md"), generateSessionVars(), 0644,
			)
		},
	})
}

type sessionVar struct {
	name        string
	description string
	readOnly    bool
	defaultVal  string
}

type sessionVars []sessionVar

func (p sessionVars) Len() int      { return len(p) }
func (p sessionVars) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p sessionVars) Less(i, j int) bool {
	return p[i].name < p[j].name
}

func generateSessionVars() []byte {
	vars := make(sessionVars, 0)

	// Create a new cluster settings instance with default values
	st := cluster.MakeTestingClusterSettings()

	// Get all session variables from the sql package
	for name, v := range sql.GetSessionVars() {
		if v.Hidden {
			continue
		}
		var defaultVal string
		if v.GlobalDefault != nil {
			defaultVal = v.GlobalDefault(&st.SV)
		}
		vars = append(vars, sessionVar{
			name:        name,
			description: v.Description,
			readOnly:    v.Set == nil && v.RuntimeSet == nil && v.SetWithPlanner == nil,
			defaultVal:  defaultVal,
		})
	}

	sort.Sort(vars)

	b := new(bytes.Buffer)
	b.WriteString("# Session Variables\n\n")
	b.WriteString("This page documents the session variables available in CockroachDB.\n\n")
	b.WriteString("Session variables control the behavior of the current session and can be set using the `SET` statement. For example:\n\n")
	b.WriteString("```sql\n")
	b.WriteString("SET application_name = 'myapp';\n")
	b.WriteString("```\n\n")
	b.WriteString("To view the current value of a session variable, use `SHOW`:\n\n")
	b.WriteString("```sql\n")
	b.WriteString("SHOW application_name;\n")
	b.WriteString("```\n\n")

	b.WriteString("<table>\n")
	b.WriteString("<thead><tr>\n")
	b.WriteString("<th>Variable Name</th>\n")
	b.WriteString("<th>Description</th>\n")
	b.WriteString("<th>Default Value</th>\n")
	b.WriteString("<th>Read-only</th>\n")
	b.WriteString("</tr></thead>\n")
	b.WriteString("<tbody>\n")

	for _, v := range vars {
		readOnly := "No"
		if v.readOnly {
			readOnly = "Yes"
		}
		defaultVal := v.defaultVal
		if defaultVal == "" {
			defaultVal = "-"
		}
		fmt.Fprintf(b, "<tr><td><code>%s</code></td><td>%s</td><td><code>%s</code></td><td>%s</td></tr>\n",
			v.name,
			v.description,
			defaultVal,
			readOnly,
		)
	}

	b.WriteString("</tbody></table>\n")
	return b.Bytes()
}
