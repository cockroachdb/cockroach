// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	"github.com/cockroachdb/cockroach/pkg/cmd/docgen/extract"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

func init() {
	const topStmt = "stmt_block"

	// Global vars.
	var (
		filter      string
		invertMatch bool
	)

	write := func(name string, data []byte) {
		if err := os.MkdirAll(filepath.Dir(name), 0755); err != nil {
			panic(err)
		}
		if err := ioutil.WriteFile(name, data, 0644); err != nil {
			panic(err)
		}
	}

	// BNF vars.
	var (
		addr string
	)

	cmdBNF := &cobra.Command{
		Use:   "bnf [dir]",
		Short: "Generate EBNF from sql.y.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			bnfDir := args[0]
			bnf, err := runBNF(addr)
			if err != nil {
				panic(err)
			}
			br := func() io.Reader {
				return bytes.NewReader(bnf)
			}

			filterRE := regexp.MustCompile(filter)

			if filterRE.MatchString(topStmt) != invertMatch {
				name := topStmt
				if !quiet {
					fmt.Println("processing", name)
				}
				g, err := runParse(br(), nil, name, true, true, nil, nil)
				if err != nil {
					log.Fatalf("%s: %+v", name, err)
				}
				write(filepath.Join(bnfDir, name+".bnf"), g)
			}

			for _, s := range specs {
				if filterRE.MatchString(s.name) == invertMatch {
					continue
				}
				if !quiet {
					fmt.Println("processing", s.name)
				}
				if s.stmt == "" {
					s.stmt = s.name
				}
				g, err := runParse(br(), s.inline, s.stmt, false, s.nosplit, s.match, s.exclude)
				if err != nil {
					log.Fatalf("%s: %+v", s.name, err)
				}
				replacements := make([]string, 0, len(s.replace))
				for from := range s.replace {
					replacements = append(replacements, from)
				}
				sort.Strings(replacements)
				for _, from := range replacements {
					g = bytes.Replace(g, []byte(from), []byte(s.replace[from]), -1)
				}
				replacements = replacements[:0]
				for from := range s.regreplace {
					replacements = append(replacements, from)
				}
				sort.Strings(replacements)
				for _, from := range replacements {
					re := regexp.MustCompile(from)
					g = re.ReplaceAll(g, []byte(s.regreplace[from]))
				}
				write(filepath.Join(bnfDir, s.name+".bnf"), g)
			}
		},
	}

	cmdBNF.Flags().StringVar(&addr, "addr", "./pkg/sql/parser/sql.y", "Location of sql.y file. Can also specify an http address.")

	// SVG vars.
	var (
		maxWorkers  int
		railroadJar string
	)

	cmdSVG := &cobra.Command{
		Use:   "svg [bnf dir] [svg dir]",
		Short: "Generate SVG diagrams from SQL grammar",
		Long:  `With no arguments, generates SQL diagrams for all statements.`,
		Args:  cobra.ExactArgs(2),
		Run: func(cmd *cobra.Command, args []string) {
			bnfDir := args[0]
			svgDir := args[1]
			if railroadJar != "" {
				_, err := os.Stat(railroadJar)
				if err != nil {
					if envutil.EnvOrDefaultBool("COCKROACH_REQUIRE_RAILROAD", false) {
						log.Fatalf("%s not found\n", railroadJar)
					} else {
						log.Printf("%s not found, falling back to slower web service (employees can find Railroad.jar on Google Drive).", railroadJar)
						railroadJar = ""
					}
				}
			}

			filterRE := regexp.MustCompile(filter)

			matches, err := filepath.Glob(filepath.Join(bnfDir, "*.bnf"))
			if err != nil {
				panic(err)
			}

			specMap := make(map[string]stmtSpec)
			for _, s := range specs {
				specMap[s.name] = s
			}
			if len(specs) != len(specMap) {
				panic("duplicate spec name")
			}

			var wg sync.WaitGroup
			sem := make(chan struct{}, maxWorkers) // max number of concurrent workers
			for _, m := range matches {
				name := strings.TrimSuffix(filepath.Base(m), ".bnf")
				if filterRE.MatchString(name) == invertMatch {
					continue
				}
				wg.Add(1)
				sem <- struct{}{}
				go func(m string) {
					defer wg.Done()
					defer func() { <-sem }()

					if !quiet {
						fmt.Printf("generating svg of %s (%s)\n", name, m)
					}

					f, err := os.Open(m)
					if err != nil {
						panic(err)
					}
					defer f.Close()

					rr, err := runRR(f, railroadJar)
					if err != nil {
						log.Fatalf("%s: %s\n", m, err)
					}

					var body string
					if strings.HasSuffix(m, topStmt+".bnf") {
						body, err = extract.InnerTag(bytes.NewReader(rr), "body")
						body = strings.SplitN(body, "<hr/>", 2)[0]
						body += `<p>generated by <a href="http://www.bottlecaps.de/rr/ui" data-proofer-ignore>Railroad Diagram Generator</a></p>`
						body = fmt.Sprintf("<div>%s</div>", body)
						if err != nil {
							log.Fatal(err)
						}
					} else {
						s, ok := specMap[name]
						if !ok {
							log.Fatalf("unfound spec: %s", name)
						}
						body, err = extract.Tag(bytes.NewReader(rr), "svg")
						if err != nil {
							panic(err)
						}
						body = strings.Replace(body, `<a xlink:href="#`, `<a xlink:href="sql-grammar.html#`, -1)
						for _, u := range s.unlink {
							s := fmt.Sprintf(`<a xlink:href="sql-grammar.html#%s" xlink:title="%s">((?s).*?)</a>`, u, u)
							link := regexp.MustCompile(s)
							body = link.ReplaceAllString(body, "$1")
						}
						for from, to := range s.relink {
							replaceFrom := fmt.Sprintf(`<a xlink:href="sql-grammar.html#%s" xlink:title="%s">`, from, from)
							replaceTo := fmt.Sprintf(`<a xlink:href="sql-grammar.html#%s" xlink:title="%s">`, to, to)
							body = strings.Replace(body, replaceFrom, replaceTo, -1)
						}
						// Wrap the output in a <div> so that the Markdown parser
						// doesn't attempt to parse the inside of the contained
						// <svg> as Markdown.
						body = fmt.Sprintf(`<div>%s</div>`, body)
					}
					name = strings.Replace(name, "_stmt", "", 1)
					write(filepath.Join(svgDir, name+".html"), []byte(body))
				}(m)
			}
			wg.Wait()
		},
	}

	cmdSVG.Flags().IntVar(&maxWorkers, "max-workers", 1, "maximum number of concurrent workers")
	cmdSVG.Flags().StringVar(&railroadJar, "railroad", "", "Location of Railroad.jar; empty to use website")

	diagramCmd := &cobra.Command{
		Use:   "grammar",
		Short: "Generate diagrams.",
	}

	diagramCmd.PersistentFlags().StringVar(&filter, "filter", ".*", "Filter statement names (regular expression)")
	diagramCmd.PersistentFlags().BoolVar(&invertMatch, "invert-match", false, "Generate everything that doesn't match the filter")

	diagramCmd.AddCommand(cmdBNF, cmdSVG)
	cmds = append(cmds, diagramCmd)
}

type stmtSpec struct {
	name           string
	stmt           string // if unspecified, uses name
	inline         []string
	replace        map[string]string
	regreplace     map[string]string
	match, exclude []*regexp.Regexp
	unlink         []string
	relink         map[string]string
	nosplit        bool
}

func runBNF(addr string) ([]byte, error) {
	return extract.GenerateBNF(addr)
}

func runParse(
	r io.Reader,
	inline []string,
	topStmt string,
	descend, nosplit bool,
	match, exclude []*regexp.Regexp,
) ([]byte, error) {
	g, err := extract.ParseGrammar(r)
	if err != nil {
		return nil, errors.Wrap(err, "parse grammar")
	}
	if err := g.Inline(inline...); err != nil {
		return nil, errors.Wrap(err, "inline")
	}
	b, err := g.ExtractProduction(topStmt, descend, nosplit, match, exclude)
	b = bytes.Replace(b, []byte("IDENT"), []byte("identifier"), -1)
	b = bytes.Replace(b, []byte("_LA"), []byte(""), -1)
	return b, err
}

func runRR(r io.Reader, railroadJar string) ([]byte, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var html []byte
	if railroadJar == "" {
		html, err = extract.GenerateRRNet(b)
	} else {
		html, err = extract.GenerateRRJar(railroadJar, b)
	}
	if err != nil {
		return nil, err
	}
	s, err := extract.XHTMLtoHTML(bytes.NewReader(html))
	return []byte(s), err
}

var specs = []stmtSpec{
	// TODO(mjibson): improve SET filtering
	// TODO(mjibson): improve SELECT display
	{
		name:   "add_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "column_def"},
		match:  []*regexp.Regexp{regexp.MustCompile("'ADD' ('COLUMN')? ?('IF' 'NOT' 'EXISTS')? ?column_name")},
		replace: map[string]string{
			" | 'ALTER' opt_column column_name alter_column_default" +
				" | 'ALTER' opt_column column_name 'DROP' 'NOT' 'NULL'" +
				" | 'DROP' opt_column 'IF' 'EXISTS' column_name opt_drop_behavior" +
				" | 'DROP' opt_column column_name opt_drop_behavior" +
				" | 'ADD' table_constraint opt_validate_behavior" +
				" | 'VALIDATE' 'CONSTRAINT' constraint_name" +
				" | 'DROP' 'CONSTRAINT' 'IF' 'EXISTS' constraint_name opt_drop_behavior" +
				" | 'DROP' 'CONSTRAINT' constraint_name opt_drop_behavior": "",
			"relation_expr": "table_name",
			"col_qual_list": "( col_qualification | )"},
		unlink: []string{"table_name"},
	},
	{
		name:    "add_constraint",
		stmt:    "alter_onetable_stmt",
		replace: map[string]string{"relation_expr": "table_name", "alter_table_cmds": "'ADD' 'CONSTRAINT' constraint_name constraint_elem"},
		unlink:  []string{"table_name"},
	},
	{
		name:   "alter_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_column", "alter_column_default"},
		match:  []*regexp.Regexp{regexp.MustCompile("relation_expr 'ALTER' ")},
		replace: map[string]string{
			"( ',' (" +
				" 'ADD' column_def" +
				" | 'ADD' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ADD' 'COLUMN' column_def" +
				" | 'ADD' 'COLUMN' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ALTER' ( 'COLUMN' |  ) column_name ( 'SET' 'DEFAULT' a_expr | 'DROP' 'DEFAULT' )" +
				" | 'ALTER' ( 'COLUMN' |  ) column_name 'DROP' 'NOT' 'NULL'" +
				" | 'DROP' ( 'COLUMN' |  ) 'IF' 'EXISTS' column_name opt_drop_behavior" +
				" | 'DROP' ( 'COLUMN' |  ) column_name opt_drop_behavior" +
				" | 'ADD' table_constraint opt_validate_behavior" +
				" | 'VALIDATE' 'CONSTRAINT' constraint_name" +
				" | 'DROP' 'CONSTRAINT' 'IF' 'EXISTS' constraint_name opt_drop_behavior" +
				" | 'DROP' 'CONSTRAINT' constraint_name opt_drop_behavior" +
				" ) )*": "",
			"relation_expr": "table_name"},
		unlink: []string{"table_name"},
	},
	{
		name:    "alter_sequence_options_stmt",
		inline:  []string{"sequence_option_list", "sequence_option_elem"},
		replace: map[string]string{"relation_expr": "sequence_name", "signed_iconst64": "integer"},
		unlink:  []string{"integer", "sequence_name"},
		nosplit: true,
	},
	{
		name:   "alter_table",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "column_def", "opt_drop_behavior", "alter_column_default", "opt_column", "opt_set_data", "table_constraint"},
		replace: map[string]string{
			"'VALIDATE' 'CONSTRAINT' name": "",
			"opt_validate_behavior":        "",
			"relation_expr":                "table_name"},
		unlink:  []string{"table_name"},
		nosplit: true,
	},
	{
		name:    "alter_view",
		stmt:    "alter_rename_view_stmt",
		inline:  []string{"opt_transaction"},
		replace: map[string]string{"relation_expr": "view_name", "qualified_name": "name"}, unlink: []string{"view_name", "name"},
	},
	{
		name:   "backup",
		stmt:   "backup_stmt",
		inline: []string{"targets", "table_pattern_list", "name_list", "opt_as_of_clause", "opt_incremental", "opt_with_options"},
		match:  []*regexp.Regexp{regexp.MustCompile("'BACKUP'")},
		replace: map[string]string{
			"non_reserved_word_or_sconst":                     "destination",
			"'AS' 'OF' 'SYSTEM' 'TIME' a_expr_const":          "'AS OF SYSTEM TIME' timestamp",
			"'INCREMENTAL' 'FROM' string_or_placeholder_list": "'INCREMENTAL FROM' full_backup_location ( | ',' incremental_backup_location ( ',' incremental_backup_location )* )",
			"'WITH' 'OPTIONS' '(' kv_option_list ')'":         "",
		},
		unlink: []string{"destination", "timestamp", "full_backup_location", "incremental_backup_location"},
	},
	{
		name:    "begin_transaction",
		stmt:    "begin_stmt",
		inline:  []string{"opt_transaction", "begin_transaction", "transaction_mode", "transaction_iso_level", "transaction_user_priority", "user_priority", "iso_level", "transaction_mode_list", "opt_comma", "transaction_read_mode"},
		exclude: []*regexp.Regexp{regexp.MustCompile("'START'")},
		replace: map[string]string{
			"'ISOLATION' 'LEVEL'": "'ISOLATION LEVEL'",
			//" | transaction_read_mode":                                                                      "",
			//" transaction_read_mode":                                                                        "",
			"'READ' 'UNCOMMITTED' | 'READ' 'COMMITTED' | 'SNAPSHOT' | 'REPEATABLE' 'READ' | 'SERIALIZABLE'": "'SNAPSHOT' | 'SERIALIZABLE'",
			"'READ' 'UNCOMMITTED'": "'SNAPSHOT'",
			"'READ' 'COMMITTED'":   "'SNAPSHOT'",
			"'REPEATABLE' 'READ'":  "'SERIALIZABLE'",
		},
	},
	{
		name:    "check_column_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'CHECK' '(' check_expr ')' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink:  []string{"table_name", "column_name", "column_type", "check_expr", "column_constraints", "table_constraints"},
	},
	{
		name:    "check_table_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' constraint_name | ) 'CHECK' '(' check_expr ')' ( table_constraints | ) ')'"},
		unlink:  []string{"table_name", "check_expr", "table_constraints"},
	},
	{
		name:    "column_def",
		stmt:    "column_def",
		replace: map[string]string{"col_qual_list": " ( | ( col_qualification ( col_qualification )* ) )"},
	},
	{name: "col_qualification", stmt: "col_qualification", inline: []string{"col_qualification_elem"}},
	{
		name:   "commit_transaction",
		stmt:   "commit_stmt",
		inline: []string{"opt_transaction"},
		match:  []*regexp.Regexp{regexp.MustCompile("'COMMIT'|'END'")},
	},
	{name: "cancel_query", stmt: "cancel_query_stmt", replace: map[string]string{"a_expr": "query_id"}, unlink: []string{"query_id"}},
	{name: "create_database_stmt", inline: []string{"opt_encoding_clause"}, replace: map[string]string{"'SCONST'": "encoding"}, unlink: []string{"name", "encoding"}},
	{
		name:   "create_index_stmt",
		inline: []string{"opt_storing", "storing", "opt_unique", "opt_name", "index_params", "index_elem", "opt_asc_desc", "name_list"},
		replace: map[string]string{
			"'INDEX' ( name": "'INDEX' ( index_name",
			"'EXISTS' name":  "'EXISTS' index_name",
			"qualified_name": "table_name",
			"',' name":       "',' column_name",
			"( name (":       "( column_name (",
		},
		unlink:  []string{"index_name", "table_name", "column_name"},
		nosplit: true,
	},
	{
		name:    "create_sequence_stmt",
		inline:  []string{"opt_sequence_option_list", "sequence_option_list", "sequence_option_elem"},
		replace: map[string]string{"signed_iconst64": "integer", "any_name": "sequence_name"},
		unlink:  []string{"integer", "sequence_name"},
		nosplit: true,
	},
	{name: "create_table_as_stmt", inline: []string{"opt_column_list", "name_list"}},
	{name: "create_table_stmt", inline: []string{"opt_table_elem_list", "table_elem_list", "table_elem"}},
	{
		name:    "create_view_stmt",
		inline:  []string{"opt_column_list"},
		replace: map[string]string{"name_list": "column_list"},
		relink:  map[string]string{"column_list": "name_list"},
	},
	{name: "create_user_stmt",
		inline: []string{"opt_with", "opt_password"},
		replace: map[string]string{
			"'PASSWORD' string_or_placeholder": "'PASSWORD' password",
			"'USER' string_or_placeholder":     "'USER' name",
			"'EXISTS' string_or_placeholder":   "'EXISTS' name",
		},
		unlink: []string{"password"},
	},
	{
		name: "default_value_column_level",
		stmt: "stmt_block",
		replace: map[string]string{
			"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'DEFAULT' default_value ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'",
		},
		unlink: []string{"table_name", "column_name", "column_type", "default_value", "table_constraints"},
	},
	{name: "delete_stmt", inline: []string{"relation_expr_opt_alias", "where_clause", "returning_clause", "target_list", "target_elem"}},
	{
		name:   "drop_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_column", "opt_drop_behavior"},
		match:  []*regexp.Regexp{regexp.MustCompile("relation_expr 'DROP' 'COLUMN'")},
		replace: map[string]string{
			"( ',' (" +
				" 'ADD' column_def" +
				" | 'ADD' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ADD' 'COLUMN' column_def" +
				" | 'ADD' 'COLUMN' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ALTER' ( 'COLUMN' |  ) column_name alter_column_default" +
				" | 'ALTER' ( 'COLUMN' |  ) column_name 'DROP' 'NOT' 'NULL'" +
				" | 'DROP' ( 'COLUMN' |  ) 'IF' 'EXISTS' column_name ( 'CASCADE' | 'RESTRICT' |  )" +
				" | 'DROP' ( 'COLUMN' |  ) column_name ( 'CASCADE' | 'RESTRICT' |  )" +
				" | 'ADD' table_constraint opt_validate_behavior" +
				" | 'VALIDATE' 'CONSTRAINT' constraint_name" +
				" | 'DROP' 'CONSTRAINT' 'IF' 'EXISTS' constraint_name ( 'CASCADE' | 'RESTRICT' |  )" +
				" | 'DROP' 'CONSTRAINT' constraint_name ( 'CASCADE' | 'RESTRICT' |  )" +
				" ) )*": "",
			"'COLUMN'":      "( 'COLUMN' | )",
			"relation_expr": "table_name"},
		unlink: []string{"table_name"},
	},
	{
		name:   "drop_constraint",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd"},
		match:  []*regexp.Regexp{regexp.MustCompile("relation_expr 'DROP' 'CONSTRAINT'")},
		replace: map[string]string{
			"opt_drop_behavior ( ',' (" +
				" 'ADD' column_def" +
				" | 'ADD' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ADD' 'COLUMN' column_def" +
				" | 'ADD' 'COLUMN' 'IF' 'NOT' 'EXISTS' column_def" +
				" | 'ALTER' opt_column column_name alter_column_default" +
				" | 'ALTER' opt_column column_name 'DROP' 'NOT' 'NULL'" +
				" | 'DROP' opt_column 'IF' 'EXISTS' column_name opt_drop_behavior" +
				" | 'DROP' opt_column column_name opt_drop_behavior" +
				" | 'ADD' table_constraint opt_validate_behavior" +
				" | 'VALIDATE' 'CONSTRAINT' constraint_name" +
				" | 'DROP' 'CONSTRAINT' 'IF' 'EXISTS' constraint_name opt_drop_behavior" +
				" | 'DROP' 'CONSTRAINT' constraint_name opt_drop_behavior" +
				" ) )*": "",
			"'CONSTRAINT'":  "( 'CONSTRAINT' | )",
			"relation_expr": "table_name"},
		unlink: []string{"table_name"},
	},
	{
		name:   "drop_database",
		stmt:   "drop_database_stmt",
		inline: []string{"opt_drop_behavior"},
		match:  []*regexp.Regexp{regexp.MustCompile("'DROP' 'DATABASE'")},
	},
	{
		name:    "drop_index",
		stmt:    "drop_index_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'DROP' 'INDEX'")},
		inline:  []string{"opt_drop_behavior", "table_name_with_index_list", "table_name_with_index"},
		replace: map[string]string{"qualified_name": "table_name", "'@' name": "'@' index_name"}, unlink: []string{"table_name", "index_name"},
	},
	{
		name:   "drop_sequence_stmt",
		inline: []string{"table_name_list", "opt_drop_behavior"},
		unlink: []string{"sequence_name"},
	},
	{
		name:   "drop_stmt",
		inline: []string{"table_name_list", "drop_ddl_stmt"},
	},
	{
		name:   "drop_table",
		stmt:   "drop_table_stmt",
		inline: []string{"opt_drop_behavior", "table_name_list"},
		match:  []*regexp.Regexp{regexp.MustCompile("'DROP' 'TABLE'")},
	},
	{
		name:   "drop_view",
		stmt:   "drop_view_stmt",
		inline: []string{"opt_drop_behavior", "table_name_list"},
		match:  []*regexp.Regexp{regexp.MustCompile("'DROP' 'VIEW'")},
	},
	{
		name:   "explain_stmt",
		inline: []string{"explain_option_list"},
		replace: map[string]string{
			"explain_option_name": "( | 'EXPRS' | 'METADATA' | 'QUALIFY' | 'VERBOSE' | 'TYPES' )",
		},
	},
	{name: "family_def", inline: []string{"opt_name", "name_list"}},
	{
		name:    "grant_stmt",
		inline:  []string{"privileges", "privilege_list", "privilege", "targets", "table_pattern_list", "name_list"},
		replace: map[string]string{"table_pattern": "table_name", "'DATABASE' ( name ( ',' name )* )": "'DATABASE' ( database_name ( ',' database_name )* )", "'TO' ( name ( ',' name )* )": "'TO' ( user_name ( ',' user_name )* )"},
		unlink:  []string{"table_name", "database_name", "user_name"},
		nosplit: true,
	},
	{
		name:    "foreign_key_column_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'REFERENCES' parent_table ( '(' ref_column_name ')' | ) ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink:  []string{"table_name", "column_name", "column_type", "parent_table", "table_constraints"},
	},
	{
		name:    "foreign_key_table_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' constraint_name | ) 'FOREIGN KEY' '(' ( fk_column_name ( ',' fk_column_name )* ) ')' 'REFERENCES' parent_table ( '(' ( ref_column_name ( ',' ref_column_name )* ) ')' | ) ( table_constraints | ) ')'"},
		unlink:  []string{"table_name", "column_name", "parent_table", "table_constraints"},
	},
	{name: "index_def", inline: []string{"opt_storing", "storing", "index_params", "opt_name"}},
	{name: "import_table", stmt: "import_stmt"},
	{
		name:   "insert_stmt",
		inline: []string{"insert_target", "insert_rest", "returning_clause"},
		match:  []*regexp.Regexp{regexp.MustCompile("'INSERT'")},
	},
	{name: "iso_level"},
	{
		name:    "interleave",
		stmt:    "create_table_stmt",
		inline:  []string{"opt_interleave"},
		replace: map[string]string{"opt_table_elem_list": "table_definition", "name_list": "interleave_prefix", " name": " parent_table"},
		unlink:  []string{"table_name", "table_definition", "parent_table", "child_columns"},
	},
	{
		name:    "not_null_column_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'NOT NULL' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink:  []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{name: "opt_interleave", replace: map[string]string{"name_list": "interleave_prefix"}, unlink: []string{"interleave_prefix"}},
	{
		name:    "primary_key_column_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'PRIMARY KEY' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink:  []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{
		name:    "primary_key_table_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' name | ) 'PRIMARY KEY' '(' ( column_name ( ',' column_name )* ) ')' ( table_constraints | ) ')'"},
		unlink:  []string{"table_name", "column_name", "table_constraints"},
	},
	{name: "release_savepoint", stmt: "release_stmt", inline: []string{"savepoint_name"}},
	{name: "rename_column", stmt: "alter_rename_table_stmt", inline: []string{"opt_column"}, match: []*regexp.Regexp{regexp.MustCompile("'ALTER' 'TABLE' .* 'RENAME' ('COLUMN'|name)")}, replace: map[string]string{"relation_expr": "table_name", "name 'TO'": "current_name 'TO'"}, unlink: []string{"table_name", "current_name"}},
	{name: "rename_database", stmt: "alter_rename_database_stmt", match: []*regexp.Regexp{regexp.MustCompile("'ALTER' 'DATABASE'")}},
	{name: "rename_index", stmt: "alter_rename_index_stmt", match: []*regexp.Regexp{regexp.MustCompile("'ALTER' 'INDEX'")}, inline: []string{"table_name_with_index"}, replace: map[string]string{"qualified_name": "table_name", "'@' name": "'@' index_name"}, unlink: []string{"table_name", "index_name"}},
	{name: "rename_table", stmt: "alter_rename_table_stmt", match: []*regexp.Regexp{regexp.MustCompile("'ALTER' 'TABLE' .* 'RENAME' 'TO'")}, replace: map[string]string{"relation_expr": "current_name", "qualified_name": "new_name"}, unlink: []string{"current_name"}, relink: map[string]string{"new_name": "name"}},
	{
		name:    "restore",
		stmt:    "restore_stmt",
		inline:  []string{"targets", "table_pattern_list", "name_list", "opt_as_of_clause", "opt_with_options"},
		match:   []*regexp.Regexp{regexp.MustCompile("'RESTORE'")},
		exclude: []*regexp.Regexp{regexp.MustCompile("'RESTORE' 'DATABASE'")},
		replace: map[string]string{
			"non_reserved_word_or_sconst":            "destination",
			"'AS' 'OF' 'SYSTEM' 'TIME' a_expr_const": "",
			"string_or_placeholder_list":             "full_backup_location ( | incremental_backup_location ( ',' incremental_backup_location )*)",
			"'WITH' 'OPTIONS'":                       "'WITH OPTIONS'",
		},
		unlink: []string{"destination", "timestamp", "full_backup_location", "incremental_backup_location"},
	},
	{
		name:   "revoke_stmt",
		inline: []string{"privileges", "privilege_list", "privilege", "targets"},
		replace: map[string]string{
			"table_pattern_list":        "table_name ( ',' table_name )*",
			"name_list":                 "database_name ( ',' database_name )*",
			"'FROM' name ( ',' name )*": "'FROM' user_name ( ',' user_name )*"},
		unlink: []string{"table_name", "database_name", "user_name"},
	},
	{
		name:    "rollback_transaction",
		stmt:    "rollback_stmt",
		inline:  []string{"opt_to_savepoint"},
		match:   []*regexp.Regexp{regexp.MustCompile("'ROLLBACK'")},
		replace: map[string]string{"'TRANSACTION'": "", "'TO'": "'TO' 'SAVEPOINT'", "savepoint_name": "cockroach_restart"},
		unlink:  []string{"cockroach_restart"},
	},
	{name: "savepoint_stmt", inline: []string{"savepoint_name"}},
	{
		name:   "select_stmt",
		inline: []string{"select_no_parens", "simple_select", "opt_sort_clause", "select_limit", "opt_all_clause", "distinct_clause", "target_list", "from_clause", "where_clause", "group_clause", "having_clause"},
		replace: map[string]string{"'SELECT' ( 'ALL' |  ) ( target_elem ( ',' target_elem )* ) ( 'FROM' from_list opt_as_of_clause |  ) ( 'WHERE' a_expr |  ) ( 'GROUP' 'BY' expr_list |  ) ( 'HAVING' a_expr |  ) window_clause | 'SELECT' ( 'DISTINCT' ) ( target_elem ( ',' target_elem )* ) ( 'FROM' from_list opt_as_of_clause |  ) ( 'WHERE' a_expr |  ) ( 'GROUP' 'BY' expr_list |  ) ( 'HAVING' a_expr |  ) window_clause | values_clause | 'TABLE' relation_expr | ": "",
			"select_clause 'UNION' all_or_distinct select_clause | select_clause 'INTERSECT' all_or_distinct select_clause | select_clause 'EXCEPT' all_or_distinct select_clause": "select_clause ( | ( ( 'UNION' | 'INTERSECT' | 'EXCEPT' ) all_or_distinct select_clause ) )",
			"select_clause sort_clause | select_clause ( sort_clause |  ) ( limit_clause offset_clause | offset_clause limit_clause | limit_clause | offset_clause )":              "select_clause ( 'ORDER BY' sortby_list | ) ( 'LIMIT' limit_val | ) ( 'OFFSET' offset_val | )",
			"all_or_distinct select_clause": "all_or_distinct 'SELECT ...'",
			"| select_with_parens":          "",
			"all_or_distinct":               "( 'ALL' | )",
			"select_clause":                 "'SELECT' ( 'DISTINCT' | ) ( target_elem ('AS' col_label | ) ( ',' target_elem ('AS' col_label | ) )* ) 'FROM' ( table_ref ( '@' index_name | ) ( ',' table_ref ( '@' index_name | ) )* ) ('AS OF SYSTEM TIME' timestamp | ) ( 'WHERE' a_expr |  ) ( 'GROUP BY' expr_list ( 'HAVING' a_expr |  ) |  ) "},
		unlink:  []string{"index_name"},
		nosplit: true,
	},
	{
		name:   "set_var",
		stmt:   "set_stmt",
		inline: []string{"set_session_stmt", "set_rest_more", "generic_set", "var_list"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile(`'SET' . 'TRANSACTION'`),
			regexp.MustCompile(`'SET' 'TRANSACTION'`),
			regexp.MustCompile(`'SET' 'SESSION' var_name`),
			regexp.MustCompile(`'SET' 'SESSION' 'TRANSACTION'`),
			regexp.MustCompile(`'SET' 'SESSION' 'CHARACTERISTICS'`),
			regexp.MustCompile("'SET' 'CLUSTER'"),
		},
		replace: map[string]string{
			"'=' 'DEFAULT'":  "'=' 'DEFAULT' | 'SET' 'TIME' 'ZONE' ( var_value | 'DEFAULT' | 'LOCAL' )",
			"'SET' var_name": "'SET' ( 'SESSION' | ) var_name",
		},
	},
	{
		name:   "set_cluster_setting",
		stmt:   "set_stmt",
		inline: []string{"set_csetting_stmt", "generic_set", "var_list"},
		match: []*regexp.Regexp{
			regexp.MustCompile("'SET' 'CLUSTER'"),
		},
	},
	{name: "set_transaction", stmt: "set_stmt", inline: []string{"set_transaction_stmt", "transaction_mode_list", "transaction_iso_level", "transaction_user_priority", "iso_level", "user_priority"}, match: []*regexp.Regexp{regexp.MustCompile("'SET' 'TRANSACTION'")}, exclude: []*regexp.Regexp{regexp.MustCompile("'READ'")}, replace: map[string]string{"'ISOLATION' 'LEVEL'": "'ISOLATION LEVEL'"}},
	{
		name: "show_var",
		stmt: "show_stmt",
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'SHOW' 'ALL' 'CLUSTER'"),
			regexp.MustCompile("'SHOW' 'IN"),       // INDEX, INDEXES
			regexp.MustCompile("'SHOW' '[B-HJ-Z]"), // Keep ALL and IDENT.
		},
		replace: map[string]string{"identifier": "var_name"},
	},
	{
		name: "show_cluster_setting",
		stmt: "show_stmt",
		match: []*regexp.Regexp{
			regexp.MustCompile("'SHOW'.* 'CLUSTER'"),
		},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'SHOW' 'CLUSTER' 'SETTING' 'ALL'"),
		},
	},
	{name: "show_columns", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'COLUMNS'")}, replace: map[string]string{"var_name": "table_name"}, unlink: []string{"table_name"}},
	{name: "show_constraints", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'CONSTRAINTS'")}, replace: map[string]string{"var_name": "table_name"}, unlink: []string{"table_name"}},
	{
		name: "show_create_sequence_stmt",
		match: []*regexp.Regexp{
			regexp.MustCompile("'SHOW' 'CREATE' 'SEQUENCE'"),
		},
		replace: map[string]string{"var_name": "sequence_name"},
		unlink:  []string{"sequence_name"},
	},
	{
		name: "show_create_table_stmt",
		match: []*regexp.Regexp{
			regexp.MustCompile("'SHOW' 'CREATE' 'TABLE'"),
		},
		replace: map[string]string{"var_name": "table_name"},
		unlink:  []string{"table_name"},
	},
	{
		name: "show_create_view_stmt",
		match: []*regexp.Regexp{
			regexp.MustCompile("'SHOW' 'CREATE' 'VIEW'"),
		},
		replace: map[string]string{"var_name": "view_name"},
		unlink:  []string{"view_name"},
	},
	{name: "show_databases", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'DATABASES'")}},
	{
		name:    "show_backup",
		stmt:    "show_backup_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'SHOW' 'BACKUP'")},
		replace: map[string]string{"string_or_placeholder": "location"},
		unlink:  []string{"location"},
	},
	{
		name:   "show_grants",
		stmt:   "show_stmt",
		inline: []string{"on_privilege_target_clause", "targets", "for_grantee_clause", "table_pattern_list", "name_list"},
		match:  []*regexp.Regexp{regexp.MustCompile("'SHOW' 'GRANTS'")},
		replace: map[string]string{
			"table_pattern":                 "table_name",
			"'DATABASE' name ( ',' name )*": "'DATABASE' database_name ( ',' database_name )*",
			"'FOR' name ( ',' name )*":      "'FOR' user_name ( ',' user_name )*",
		},
		unlink: []string{"table_name", "database_name", "user_name"},
	},
	{name: "show_index", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'INDEX'")}, replace: map[string]string{"var_name": "table_name"}, unlink: []string{"table_name"}},
	{name: "show_jobs", stmt: "show_jobs_stmt"},
	{name: "show_keys", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'KEYS'")}},
	{name: "show_queries", stmt: "show_queries_stmt"},
	{name: "show_sessions", stmt: "show_sessions_stmt"},
	{name: "show_tables", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'TABLES'")}},
	{name: "show_trace", stmt: "show_trace_stmt"},
	{name: "show_transaction", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'TRANSACTION'")}},
	{name: "show_users", stmt: "show_stmt", match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'USERS'")}},
	{name: "table_constraint", inline: []string{"constraint_elem", "opt_storing", "storing"}},
	{
		name:    "truncate_stmt",
		inline:  []string{"opt_table", "relation_expr_list", "opt_drop_behavior"},
		replace: map[string]string{"relation_expr": "table_name"},
		unlink:  []string{"table_name"},
		//inline:  []string{"opt_table", "relation_expr_list", "relation_expr", "opt_drop_behavior"},
		//replace: map[string]string{"'ONLY' '(' qualified_name ')'": "", "'ONLY' qualified_name": "", "qualified_name": "table_name", "'*'": "", "'CASCADE'": "", "'RESTRICT'": ""},
		//unlink:  []string{"table_name"},
	},
	{
		name:    "unique_column_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' column_name column_type 'UNIQUE' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink:  []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{
		name:    "unique_table_level",
		stmt:    "stmt_block",
		replace: map[string]string{"stmt_list": "'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' name | ) 'UNIQUE' '(' ( column_name ( ',' column_name )* ) ')' ( table_constraints | ) ')'"},
		unlink:  []string{"table_name", "check_expr", "table_constraints"},
	},
	/*
		{
			name:    "update_stmt",
			inline:  []string{"relation_expr_opt_alias", "set_clause_list", "set_clause", "single_set_clause", "multiple_set_clause", "ctext_row", "ctext_expr_list", "ctext_expr", "from_clause", "from_list", "where_clause", "returning_clause"},
			replace: map[string]string{"relation_expr": "table_name", "qualified_name": "column_name", "qualified_name_list": "column_name_list"},
			relink:  map[string]string{"table_name": "relation_expr", "column_name": "qualified_name", "column_name_list": "qualified_name_list"},
			nosplit: true,
		},
	*/
	{name: "upsert_stmt", stmt: "insert_stmt", inline: []string{"insert_target", "insert_rest", "returning_clause"}, match: []*regexp.Regexp{regexp.MustCompile("'UPSERT'")}},
	{
		name:    "validate_constraint",
		stmt:    "alter_onetable_stmt",
		replace: map[string]string{"alter_table_cmds": "'VALIDATE' 'CONSTRAINT' constraint_name", "relation_expr": "table_name"},
		unlink:  []string{"constraint_name", "table_name"},
	},
}
