// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
	"gitlab.com/golang-commonmark/markdown"
)

func init() {
	cmds = append(cmds, &cobra.Command{
		Use:   "functions <output-dir>",
		Short: "generate markdown documentation of functions and operators",
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

			if err := os.WriteFile(
				filepath.Join(outDir, "functions.md"), generateFunctions(builtins.AllBuiltinNames(), true), 0644,
			); err != nil {
				return err
			}
			if err := os.WriteFile(
				filepath.Join(outDir, "aggregates.md"), generateFunctions(builtins.AllAggregateBuiltinNames(), false), 0644,
			); err != nil {
				return err
			}
			if err := os.WriteFile(
				filepath.Join(outDir, "window_functions.md"), generateFunctions(builtins.AllWindowBuiltinNames(), false), 0644,
			); err != nil {
				return err
			}
			return os.WriteFile(
				filepath.Join(outDir, "operators.md"), generateOperators(), 0644,
			)
		},
	})
}

type operation struct {
	left  string
	right string
	ret   string
	op    string
}

func (o operation) String() string {
	if o.right == "" {
		return fmt.Sprintf("<code>%s</code>%s", o.op, linkTypeName(o.left))
	}
	return fmt.Sprintf("%s <code>%s</code> %s", linkTypeName(o.left), o.op, linkTypeName(o.right))
}

type operations []operation

func (p operations) Len() int      { return len(p) }
func (p operations) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p operations) Less(i, j int) bool {
	if p[i].right != "" && p[j].right == "" {
		return false
	}
	if p[i].right == "" && p[j].right != "" {
		return true
	}
	if p[i].left != p[j].left {
		return p[i].left < p[j].left
	}
	if p[i].right != p[j].right {
		return p[i].right < p[j].right
	}
	return p[i].ret < p[j].ret
}

func generateOperators() []byte {
	ops := make(map[string]operations)
	for optyp, overloads := range tree.UnaryOps {
		op := optyp.String()
		_ = overloads.ForEachUnaryOp(func(v *tree.UnaryOp) error {
			ops[op] = append(ops[op], operation{
				left: v.Typ.String(),
				ret:  v.ReturnType.String(),
				op:   op,
			})
			return nil
		})
	}
	for optyp, overloads := range tree.BinOps {
		op := optyp.String()
		_ = overloads.ForEachBinOp(func(v *tree.BinOp) error {
			left := v.LeftType.String()
			right := v.RightType.String()
			ops[op] = append(ops[op], operation{
				left:  left,
				right: right,
				ret:   v.ReturnType.String(),
				op:    op,
			})
			return nil
		})
	}
	for optyp, overloads := range tree.CmpOps {
		op := optyp.String()
		_ = overloads.ForEachCmpOp(func(v *tree.CmpOp) error {
			left := v.LeftType.String()
			right := v.RightType.String()
			ops[op] = append(ops[op], operation{
				left:  left,
				right: right,
				ret:   "bool",
				op:    op,
			})
			return nil
		})
	}
	var opstrs []string
	for k, v := range ops {
		sort.Sort(v)
		opstrs = append(opstrs, k)
	}
	sort.Strings(opstrs)
	b := new(bytes.Buffer)
	seen := map[string]bool{}
	for _, op := range opstrs {
		fmt.Fprintf(b, "<table><thead>\n")
		fmt.Fprintf(b, "<tr><td><code>%s</code></td><td>Return</td></tr>\n", op)
		fmt.Fprintf(b, "</thead><tbody>\n")
		for _, v := range ops[op] {
			s := fmt.Sprintf("<tr><td>%s</td><td>%s</td></tr>\n", v.String(), linkTypeName(v.ret))
			if seen[s] {
				continue
			}
			seen[s] = true
			b.WriteString(s)
		}
		fmt.Fprintf(b, "</tbody></table>")
		fmt.Fprintln(b)
	}
	return b.Bytes()
}

// TODO(mjibson): use the exported value from sql/parser/pg_builtins.go.
const notUsableInfo = "Not usable; exposed only for compatibility with PostgreSQL."

func generateFunctions(from []string, categorize bool) []byte {
	functions := make(map[string][]string)
	seen := make(map[string]struct{})
	md := markdown.New(markdown.XHTMLOutput(true), markdown.Nofollow(true))
	for _, name := range from {
		// NB: funcs can appear more than once i.e. upper/lowercase variants for
		// faster lookups, so normalize to lowercase and de-dupe using a set.
		name = strings.ToLower(name)
		if _, ok := seen[name]; ok || strings.HasPrefix(name, "crdb_internal.") {
			continue
		}
		seen[name] = struct{}{}
		props, fns := builtinsregistry.GetBuiltinProperties(name)
		if !props.ShouldDocument() {
			continue
		}
		for _, fn := range fns {
			if fn.Info == notUsableInfo {
				continue
			}
			// We generate docs for both aggregates and window functions in separate
			// files, so we want to omit them when processing all builtins.
			if categorize && (fn.Class == tree.AggregateClass || fn.Class == tree.WindowClass) {
				continue
			}
			args := fn.Types.String()

			retType := fn.InferReturnTypeFromInputArgTypes(fn.Types.Types())
			ret := retType.String()

			cat := props.Category
			if cat == "" {
				cat = strings.ToUpper(ret)
			}
			if !categorize {
				cat = ""
			}
			extra := ""
			if fn.Info != "" {
				// Render the info field to HTML upfront, because Markdown
				// won't do it automatically in a table context.
				// Boo Markdown, bad Markdown.
				// TODO(knz): Do not use Markdown.
				info := md.RenderToString([]byte(fn.Info))
				extra = fmt.Sprintf("<span class=\"funcdesc\">%s</span>", info)
			}
			s := fmt.Sprintf("<tr><td><a name=\"%s\"></a><code>%s(%s) &rarr; %s</code></td><td>%s</td><td>%s</td></tr>",
				name,
				name,
				linkArguments(args),
				linkArguments(ret),
				extra,
				fn.Volatility.TitleString(),
			)
			functions[cat] = append(functions[cat], s)
		}
	}
	var cats []string
	for k, v := range functions {
		sort.Strings(v)
		cats = append(cats, k)
	}
	sort.Strings(cats)
	// HACK: swap "Compatibility" to be last.
	// TODO(dt): Break up generated list be one _include per category, to allow
	// manually written copy on some sections.
	for i, cat := range cats {
		if cat == "Compatibility" {
			cats = append(append(cats[:i], cats[i+1:]...), "Compatibility")
			break
		}
	}
	b := new(bytes.Buffer)
	for _, cat := range cats {
		if categorize {
			fmt.Fprintf(b, "### %s functions\n\n", cat)
		}
		b.WriteString("<table>\n<thead><tr><th>Function &rarr; Returns</th><th>Description</th><th>Volatility</th></tr></thead>\n")
		b.WriteString("<tbody>\n")
		b.WriteString(strings.Join(functions[cat], "\n"))
		b.WriteString("</tbody>\n</table>\n\n")
	}
	return b.Bytes()
}

var linkRE = regexp.MustCompile(`([a-z]+)([\.\[\]]*)$`)

func linkArguments(t string) string {
	sp := strings.Split(t, ", ")
	for i, s := range sp {
		sp[i] = linkRE.ReplaceAllStringFunc(s, func(s string) string {
			match := linkRE.FindStringSubmatch(s)
			s = linkTypeName(match[1])
			return s + match[2]
		})
	}
	return strings.Join(sp, ", ")
}

func linkTypeName(s string) string {
	s = strings.TrimSuffix(s, "{}")
	s = strings.TrimSuffix(s, "{*}")
	name := s
	switch s {
	case "timestamptz":
		s = "timestamp"
	case "collatedstring":
		s = "collate"
	}
	s = strings.TrimSuffix(s, "[]")
	s = strings.TrimSuffix(s, "*")
	switch s {
	case "int", "decimal", "float", "bool", "date", "timestamp", "interval", "string", "bytes",
		"inet", "uuid", "collate", "time":
		s = fmt.Sprintf("<a href=\"%s.html\">%s</a>", s, name)
	}
	return s
}
