// Copyright 2017 The Cockroach Authors.
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
	"bufio"
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/docgen/extract"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
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
			log.Fatal(err)
		}
		if err := ioutil.WriteFile(name, data, 0644); err != nil {
			log.Fatal(err)
		}
	}

	// BNF vars.
	var (
		addr          string
		bnfAPITimeout time.Duration
	)

	cmdBNF := &cobra.Command{
		Use:   "bnf [dir]",
		Short: "Generate EBNF from sql.y.",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			bnfDir := args[0]
			bnf, err := runBNF(addr, bnfAPITimeout)
			if err != nil {
				log.Fatal(err)
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

			stmtSpecs, err := getAllStmtSpecs(addr, bnfAPITimeout)
			if err != nil {
				log.Fatal(err)
			}
			for _, s := range stmtSpecs {
				if filterRE.MatchString(s.name) == invertMatch {
					continue
				}
				if !quiet {
					fmt.Println("processing", s.name)
				}
				g, err := runParse(br(), s.inline, s.GetStatement(), false, s.nosplit, s.match, s.exclude)
				if err != nil {
					log.Fatalf("%s: %+v", s.name, err)
				}
				if !quiet {
					fmt.Printf("raw data:\n%s\n", string(g))
				}
				replacements := make([]string, 0, len(s.replace))
				for from := range s.replace {
					replacements = append(replacements, from)
				}
				sort.Strings(replacements)
				for _, from := range replacements {
					if !quiet {
						fmt.Printf("replacing: %q -> %q\n", from, s.replace[from])
					}
					g = bytes.Replace(g, []byte(from), []byte(s.replace[from]), -1)
				}
				replacements = replacements[:0]
				for from := range s.regreplace {
					replacements = append(replacements, from)
				}
				sort.Strings(replacements)
				for _, from := range replacements {
					if !quiet {
						fmt.Printf("replacing re: %q -> %q\n", from, s.replace[from])
					}
					re := regexp.MustCompile(from)
					g = re.ReplaceAll(g, []byte(s.regreplace[from]))
				}
				if !quiet {
					fmt.Printf("result:\n%s\n", string(g))
				}
				write(filepath.Join(bnfDir, s.name+".bnf"), g)
			}
		},
	}

	cmdBNF.Flags().StringVar(&addr, "addr", "./pkg/sql/parser/sql.y", "Location of sql.y file. Can also specify an http address.")
	cmdBNF.Flags().DurationVar(&bnfAPITimeout, "timeout", time.Second*120, "Timeout in seconds for bnf HTTP Api, "+
		"only relevant when the web api is used; default 120s.")

	// SVG vars.
	var (
		maxWorkers         int
		railroadJar        string
		railroadAPITimeout time.Duration
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
			stripRE := regexp.MustCompile("\n(\n| )+")

			matches, err := filepath.Glob(filepath.Join(bnfDir, "*.bnf"))
			if err != nil {
				log.Fatal(err)
			}

			specMap := make(map[string]stmtSpec)
			stmtSpecs, err := getAllStmtSpecs(addr, bnfAPITimeout)
			if err != nil {
				log.Fatal(err)
			}
			for _, s := range stmtSpecs {
				specMap[s.name] = s
			}
			if len(stmtSpecs) != len(specMap) {
				log.Fatal("duplicate spec name")
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
				go func(m, name string) {
					defer wg.Done()
					defer func() { <-sem }()

					if !quiet {
						fmt.Printf("generating svg of %s (%s)\n", name, m)
					}

					f, err := os.Open(m)
					if err != nil {
						log.Fatal(err)
					}
					defer f.Close()

					rr, err := runRR(f, railroadJar, railroadAPITimeout)
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
							log.Fatal(err)
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
						// Remove blank lines and strip spaces.
						body = stripRE.ReplaceAllString(body, "\n") + "\n"
					}
					name = strings.Replace(name, "_stmt", "", 1)
					write(filepath.Join(svgDir, name+".html"), []byte(body))
				}(m, name)
			}
			wg.Wait()
		},
	}

	cmdSVG.Flags().IntVar(&maxWorkers, "max-workers", 1, "maximum number of concurrent workers")
	cmdSVG.Flags().StringVar(&railroadJar, "railroad", "", "Location of Railroad.jar; empty to use website")
	cmdSVG.Flags().DurationVar(&railroadAPITimeout, "timeout", time.Second*120, "Timeout in seconds for railroad HTTP Api, "+
		"only relevant when the web api is used; default 120s.")

	diagramCmd := &cobra.Command{
		Use:   "grammar",
		Short: "Generate diagrams.",
	}

	diagramCmd.PersistentFlags().StringVar(&filter, "filter", ".*", "Filter statement names (regular expression)")
	diagramCmd.PersistentFlags().BoolVar(&invertMatch, "invert-match", false, "Generate everything that doesn't match the filter")

	diagramCmd.AddCommand(cmdBNF, cmdSVG)
	cmds = append(cmds, diagramCmd)
}

// stmtSpec is needed for each top-level bnf file to process.
// See the wiki page for more details about what these controls do.
// https://github.com/cockroachdb/docs/wiki/SQL-Grammar-Railroad-Diagram-Changes#structure
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

// GetStatement returns the sql statement of a stmtSpec.
func (s stmtSpec) GetStatement() string {
	if s.stmt == "" {
		return s.name
	}

	return s.stmt
}

func runBNF(addr string, bnfAPITimeout time.Duration) ([]byte, error) {
	return extract.GenerateBNF(addr, bnfAPITimeout)
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
	b = bytes.Replace(b, []byte("'IDENT'"), []byte("'identifier'"), -1)
	b = bytes.Replace(b, []byte("_LA"), []byte(""), -1)
	return b, err
}

func runRR(r io.Reader, railroadJar string, railroadAPITimeout time.Duration) ([]byte, error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var html []byte
	if railroadJar == "" {
		html, err = extract.GenerateRRNet(b, railroadAPITimeout)
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
	{
		name:   "add_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "column_def", "col_qual_list"},
		regreplace: map[string]string{
			` \( \( col_qualification \) \)\* .*`: `( ( col_qualification ) )*`,
		},
		match: []*regexp.Regexp{regexp.MustCompile("'ADD' ('COLUMN')? ?('IF' 'NOT' 'EXISTS')? ?column_name")},
		replace: map[string]string{
			"relation_expr": "table_name",
		},
		unlink: []string{"table_name"},
	},
	{
		name:    "add_constraint",
		stmt:    "alter_onetable_stmt",
		replace: map[string]string{"relation_expr": "table_name", "alter_table_cmds": "'ADD' 'CONSTRAINT' constraint_name constraint_elem opt_validate_behavior"},
		unlink:  []string{"table_name"},
	},
	{
		name:   "alter_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_column", "alter_column_default", "opt_set_data", "opt_collate", "opt_alter_column_using"},
		regreplace: map[string]string{
			regList: "",
		},
		match: []*regexp.Regexp{regexp.MustCompile("relation_expr 'ALTER' ")},
		replace: map[string]string{
			"relation_expr": "table_name",
		},
		exclude: []*regexp.Regexp{regexp.MustCompile("relation_expr 'ALTER' 'PRIMARY' 'KEY' ")},
		unlink:  []string{"table_name"},
	},
	{
		name:   "alter_database_primary_region",
		stmt:   "alter_database_primary_region_stmt",
		inline: []string{"primary_region_clause", "opt_equal"},
	},
	{
		name: "alter_database_drop_region",
		stmt: "alter_database_drop_region_stmt",
	},
	{
		name:   "alter_primary_key",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_hash_sharded"},
		regreplace: map[string]string{
			"'=' a_expr": "'=' n_buckets",
			regList:      "",
		},
		match: []*regexp.Regexp{regexp.MustCompile("relation_expr 'ALTER' 'PRIMARY' 'KEY' ")},
		replace: map[string]string{
			"relation_expr": "table_name",
		},
		unlink: []string{"table_name", "n_buckets"},
	},
	{
		name:   "alter_role_stmt",
		inline: []string{"role_or_group_or_user", "opt_role_options"},
		replace: map[string]string{
			"string_or_placeholder":             "name",
			"opt_role_options":                  "OPTIONS",
			"string_or_placeholder  'PASSWORD'": "name 'PASSWORD'",
			"'PASSWORD' string_or_placeholder":  "'PASSWORD' password"},
		unlink: []string{"name", "password"},
	},
	{
		name:    "alter_schema",
		stmt:    "alter_schema_stmt",
		inline:  []string{"qualifiable_schema_name"},
		nosplit: true,
	},
	{
		name:    "alter_sequence",
		stmt:    "alter_sequence_stmt",
		inline:  []string{"alter_rename_sequence_stmt", "alter_sequence_options_stmt", "alter_sequence_set_schema_stmt", "alter_sequence_owner_stmt", "sequence_option_list", "sequence_option_elem"},
		replace: map[string]string{"relation_expr": "sequence_name", "signed_iconst64": "integer", "column_path": "column_name"},
		unlink:  []string{"integer", "sequence_name", "column_path"},
		nosplit: true,
	},
	{
		name:   "alter_table",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "column_def", "opt_drop_behavior", "alter_column_default", "opt_column", "opt_set_data", "table_constraint", "opt_collate", "opt_alter_column_using"},
		replace: map[string]string{
			"'VALIDATE' 'CONSTRAINT' name": "",
			"opt_validate_behavior":        "",
			"relation_expr":                "table_name"},
		unlink:  []string{"table_name"},
		nosplit: true,
	},
	{
		name:    "alter_type",
		stmt:    "alter_type_stmt",
		inline:  []string{"opt_add_val_placement"},
		replace: map[string]string{"'SCONST'": "value"},
		unlink:  []string{"value"},
	},
	{
		name:    "alter_view",
		stmt:    "alter_view_stmt",
		inline:  []string{"alter_rename_view_stmt", "alter_view_set_schema_stmt", "alter_view_owner_stmt", "opt_transaction"},
		replace: map[string]string{"relation_expr": "view_name", "qualified_name": "name"}, unlink: []string{"view_name", "name"},
	},
	{
		name:    "alter_zone_database_stmt",
		inline:  []string{"set_zone_config", "var_set_list"},
		replace: map[string]string{"var_name": "variable", "var_value": "value"},
		unlink:  []string{"variable", "value"},
	},
	{
		name:    "alter_zone_index_stmt",
		inline:  []string{"table_index_name", "set_zone_config", "var_set_list"},
		replace: map[string]string{"var_name": "variable", "var_value": "value", "standalone_index_name": "index_name"},
		unlink:  []string{"variable", "value"},
	},
	{
		name:    "alter_zone_range_stmt",
		inline:  []string{"set_zone_config", "var_set_list"},
		replace: map[string]string{"zone_name": "range_name", "var_name": "variable", "var_value": "value"},
		unlink:  []string{"range_name", "variable", "value"},
	},
	{
		name:    "alter_zone_table_stmt",
		inline:  []string{"set_zone_config", "var_set_list"},
		replace: map[string]string{"var_name": "variable", "var_value": "value"},
		unlink:  []string{"variable", "value"},
	},
	{
		name:    "alter_zone_partition_stmt",
		inline:  []string{"table_index_name", "set_zone_config", "var_set_list"},
		replace: map[string]string{"var_name": "variable", "var_value": "value", "standalone_index_name": "index_name"},
		unlink:  []string{"variable", "value"},
	},
	{
		name: "analyze_stmt",
	},
	{
		name:   "backup",
		stmt:   "backup_stmt",
		inline: []string{"opt_backup_targets", "opt_with_backup_options", "opt_as_of_clause", "as_of_clause", "backup_options_list"},
		match:  []*regexp.Regexp{regexp.MustCompile("'BACKUP' targets 'INTO'")},
		replace: map[string]string{
			"targets":                        "( | 'TABLE' table_pattern ( ( ',' table_pattern ) )* | 'DATABASE' database_name ( ( ',' database_name ) )* )",
			"string_or_placeholder_opt_list": "( destination | '(' partitioned_backup_location ( ',' partitioned_backup_location )* ')' )",
			"'INTO'":                         "'INTO' ( | subdirectory 'IN' | 'LATEST' 'IN')",
			"sconst_or_placeholder":          "subdirectory",
			"a_expr":                         "timestamp",
		},
		unlink:  []string{"targets", "subdirectory", "destination", "timestamp", "partitioned_backup_location"},
		exclude: []*regexp.Regexp{regexp.MustCompile("'IN'")},
	},
	{
		name: "begin_stmt",
		inline: []string{
			"opt_transaction",
			"begin_transaction",
			"transaction_mode",
			"transaction_user_priority",
			"user_priority",
			"iso_level",
			"transaction_mode_list",
			"opt_comma",
			"transaction_read_mode",
			"as_of_clause",
			"transaction_deferrable_mode",
		},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'START'"),
		},
	},
	{
		name: "check_column_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'CHECK' '(' check_expr ')' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink: []string{"table_name", "column_name", "column_type", "check_expr", "column_constraints", "table_constraints"},
	},
	{
		name: "check_table_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' constraint_name | ) 'CHECK' '(' check_expr ')' ( table_constraints | ) ')'"},
		unlink: []string{"table_name", "check_expr", "table_constraints"},
	},
	{
		name:   "column_def",
		stmt:   "column_def",
		inline: []string{"col_qual_list"},
	},
	{
		name:   "for_locking",
		stmt:   "for_locking_item",
		inline: []string{"for_locking_strength", "opt_locked_rels", "opt_nowait_or_skip", "table_name_list"},
	},
	{
		name:   "col_qualification",
		stmt:   "col_qualification",
		inline: []string{"col_qualification_elem", "opt_hash_sharded", "generated_as"},
		replace: map[string]string{
			"'=' a_expr": "'=' n_buckets",
		},
		unlink: []string{"n_buckets"},
	},
	{
		name:    "comment",
		stmt:    "comment_stmt",
		replace: map[string]string{"column_path": "column_name"},
		unlink:  []string{"column_path"},
	},
	{
		name:   "commit_transaction",
		stmt:   "commit_stmt",
		inline: []string{"opt_transaction"},
		match:  []*regexp.Regexp{regexp.MustCompile("'COMMIT'|'END'")},
	},
	{
		name:    "copy_from_stmt",
		inline:  []string{"opt_with_copy_options", "copy_options_list", "opt_with", "opt_where_clause", "where_clause"},
		exclude: []*regexp.Regexp{regexp.MustCompile("'WHERE'")},
	},
	{
		name:    "cancel_job",
		stmt:    "cancel_jobs_stmt",
		replace: map[string]string{"a_expr": "job_id"},
		unlink:  []string{"job_id"},
	},
	{
		name:   "create_as_col_qual_list",
		inline: []string{"create_as_col_qualification", "create_as_col_qualification_elem"},
	},
	{
		name:   "create_as_constraint_def",
		inline: []string{"create_as_constraint_elem"},
	},
	{
		name:    "cancel_query",
		stmt:    "cancel_queries_stmt",
		replace: map[string]string{"a_expr": "query_id"},
		unlink:  []string{"query_id"},
	},
	{
		name:    "cancel_session",
		stmt:    "cancel_sessions_stmt",
		replace: map[string]string{"a_expr": "session_id"},
		unlink:  []string{"session_id"},
	},
	{
		name:   "create_database_stmt",
		inline: []string{"opt_with", "opt_encoding_clause", "opt_connection_limit", "opt_equal", "opt_primary_region_clause", "primary_region_clause", "opt_regions_list", "region_or_regions", "opt_survival_goal_clause", "survival_goal_clause", "opt_equal"},
		replace: map[string]string{
			"non_reserved_word_or_sconst": "encoding",
			"signed_iconst":               "limit"},
		unlink:  []string{"non_reserved_word_or_sconst", "signed_iconst", "encoding", "limit"},
		nosplit: true,
	},
	{
		name:   "create_changefeed_stmt",
		inline: []string{"changefeed_targets", "single_table_pattern_list", "opt_changefeed_sink", "opt_with_options", "kv_option_list", "kv_option"},
		replace: map[string]string{
			"table_option":                 "table_name",
			"'INTO' string_or_placeholder": "'INTO' sink",
			"name":                         "option",
			"'SCONST'":                     "option",
			"'=' string_or_placeholder":    "'=' value"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'OPTIONS'")},
		unlink: []string{"table_name", "sink", "option", "value"},
	},
	{
		name:   "create_index_stmt",
		inline: []string{"opt_unique", "opt_storing", "storing", "index_params", "index_elem", "opt_asc_desc", "opt_index_access_method", "opt_hash_sharded", "opt_concurrently", "opt_with_storage_parameter_list", "storage_parameter_list"},
		replace: map[string]string{
			"'ON' a_expr":     "'ON' column_name",
			"'=' a_expr":      "'=' n_buckets",
			"opt_nulls_order": "",
		},
		unlink: []string{"n_buckets"},
		regreplace: map[string]string{
			".* 'CREATE' .* 'INVERTED' 'INDEX' .*": "",
		},
		nosplit: true,
	},
	{
		name:   "create_index_interleaved_stmt",
		stmt:   "create_index_stmt",
		match:  []*regexp.Regexp{regexp.MustCompile("'INTERLEAVE'")},
		inline: []string{"opt_unique", "opt_storing", "opt_interleave", "opt_concurrently"},
		replace: map[string]string{
			"'ON' a_expr":                          "'ON' column_name",
			"'=' a_expr":                           "'=' n_buckets",
			" opt_index_name":                      "",
			" opt_partition_by":                    "",
			" opt_index_access_method":             "",
			"'ON' table_name '(' index_params ')'": "'...'",
			"storing '(' name_list ')'":            "'STORING' '(' stored_columns ')'",
			"table_name '(' name_list":             "parent_table '(' interleave_prefix",
		},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'CREATE' 'INVERTED'"),
			regexp.MustCompile("'EXISTS'"),
		},
		unlink: []string{"stored_columns", "parent_table", "interleave_prefix", "n_buckets"},
	},
	{
		name:   "create_inverted_index_stmt",
		stmt:   "create_index_stmt",
		match:  []*regexp.Regexp{regexp.MustCompile("'CREATE' .* 'INVERTED' 'INDEX'")},
		inline: []string{"opt_unique", "opt_storing", "storing", "index_params", "index_elem", "opt_asc_desc", "opt_concurrently", "opt_with_storage_parameter_list", "storage_parameter_list"},
		replace: map[string]string{
			"opt_nulls_order": "",
		},
		nosplit: true,
	},
	{
		name:   "create_schedule_for_backup_stmt",
		inline: []string{"opt_description", "string_or_placeholder_opt_list", "string_or_placeholder_list", "opt_with_backup_options", "cron_expr", "opt_full_backup_clause", "opt_with_schedule_options", "opt_backup_targets"},
		replace: map[string]string{
			"string_or_placeholder 'FOR'":       "label 'FOR'",
			"'RECURRING' sconst_or_placeholder": "'RECURRING' cronexpr",
			"targets":                           "( | ( 'TABLE' | ) table_pattern ( ( ',' table_pattern ) )* | 'DATABASE' database_name ( ( ',' database_name ) )* )"},
	},
	{
		name:    "create_schema_stmt",
		inline:  []string{"qualifiable_schema_name", "opt_schema_name", "opt_name"},
		nosplit: true,
	},
	{
		name:    "create_sequence_stmt",
		inline:  []string{"opt_sequence_option_list", "sequence_option_list", "sequence_option_elem"},
		replace: map[string]string{"signed_iconst64": "integer", "any_name": "sequence_name", "column_path": "column_name"},
		unlink:  []string{"integer", "sequence_name", "column_path"},
		nosplit: true,
	},
	{
		name:    "create_stats_stmt",
		replace: map[string]string{"name_list": "column_name"},
		unlink:  []string{"statistics_name", "column_name"},
	},
	{
		name:   "create_table_as_stmt",
		inline: []string{"create_as_opt_col_list", "create_as_table_defs", "opt_table_with", "opt_create_table_on_commit"},
	},
	{
		name:    "create_table_stmt",
		inline:  []string{"opt_table_elem_list", "table_elem_list", "table_elem", "opt_table_with", "opt_create_table_on_commit"},
		nosplit: true,
	},
	{
		name:    "opt_locality",
		inline:  []string{"locality"},
		replace: map[string]string{" name": "column_name"},
		unlink:  []string{"column_name"},
	},
	{
		name: "create_type",
		stmt: "create_type_stmt",
	},
	{
		name:   "create_view_stmt",
		inline: []string{"opt_column_list"},
	},
	{
		name:   "create_role_stmt",
		inline: []string{"role_or_group_or_user", "opt_role_options"},
		replace: map[string]string{
			"string_or_placeholder":             "name",
			"opt_role_options":                  "OPTIONS",
			"string_or_placeholder  'PASSWORD'": "name 'PASSWORD'",
			"'PASSWORD' string_or_placeholder":  "'PASSWORD' password"},
	},
	{
		name: "default_value_column_level",
		stmt: "stmt_block",
		replace: map[string]string{
			"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'DEFAULT' default_value ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'",
		},
		unlink: []string{"table_name", "column_name", "column_type", "default_value", "table_constraints"},
	},
	{
		name:   "delete_stmt",
		inline: []string{"opt_with_clause", "with_clause", "cte_list", "table_expr_opt_alias_idx", "table_name_opt_idx", "opt_where_clause", "where_clause", "returning_clause", "opt_sort_clause", "opt_limit_clause", "opt_only", "opt_descendant"},
		replace: map[string]string{
			"relation_expr": "table_name",
		},
		unlink:  []string{"count"},
		nosplit: true,
	},
	{
		name:   "with_clause",
		inline: []string{"cte_list", "common_table_expr", "name_list", "opt_column_list", "materialize_clause"},
		replace: map[string]string{
			"preparable_stmt ')' ) ) )* )": "preparable_stmt ')' ) ) )* ) ( insert_stmt | update_stmt | delete_stmt | upsert_stmt | select_stmt )",
		},
		nosplit: true,
	},
	{
		name:   "drop_column",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_column", "opt_drop_behavior"},
		match:  []*regexp.Regexp{regexp.MustCompile("relation_expr 'DROP' 'COLUMN'")},
		regreplace: map[string]string{
			regList: "",
		},
		replace: map[string]string{
			"relation_expr": "table_name",
			"column_name":   "name",
		},
		unlink: []string{"table_name", "name"},
	},
	{
		name:   "drop_constraint",
		stmt:   "alter_onetable_stmt",
		inline: []string{"alter_table_cmds", "alter_table_cmd", "opt_drop_behavior"},
		match:  []*regexp.Regexp{regexp.MustCompile("relation_expr 'DROP' 'CONSTRAINT'")},
		regreplace: map[string]string{
			regList: "",
		},
		replace: map[string]string{
			"relation_expr":   "table_name",
			"constraint_name": "name",
		},
		unlink: []string{"table_name"},
	},
	{
		name:   "drop_database",
		stmt:   "drop_database_stmt",
		inline: []string{"opt_drop_behavior"},
		match:  []*regexp.Regexp{regexp.MustCompile("'DROP' 'DATABASE'")},
	},
	{
		name:   "drop_index",
		stmt:   "drop_index_stmt",
		match:  []*regexp.Regexp{regexp.MustCompile("'DROP' 'INDEX'")},
		inline: []string{"opt_drop_behavior", "table_index_name_list", "table_index_name", "opt_concurrently"},
		regreplace: map[string]string{
			regList: "",
		},
		replace: map[string]string{"standalone_index_name": "index_name"},
	},
	{
		name:    "drop_role_stmt",
		inline:  []string{"role_or_group_or_user"},
		replace: map[string]string{"string_or_placeholder_list": "name"},
	},
	{
		name:   "drop_sequence_stmt",
		inline: []string{"table_name_list", "opt_drop_behavior"},
		unlink: []string{"sequence_name"},
	},
	{
		name:    "drop_schema",
		stmt:    "drop_schema_stmt",
		inline:  []string{"opt_drop_behavior", "qualifiable_schema_name"},
		nosplit: true,
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
		name:    "drop_type",
		stmt:    "drop_type_stmt",
		inline:  []string{"table_name_list"},
		replace: map[string]string{"opt_drop_behavior": ""},
	},
	{
		name:    "drop_view",
		stmt:    "drop_view_stmt",
		inline:  []string{"opt_drop_behavior", "table_name_list"},
		nosplit: true,
	},
	{
		name:   "experimental_audit",
		stmt:   "alter_onetable_stmt",
		inline: []string{"audit_mode", "alter_table_cmd", "alter_table_cmds"},
		match:  []*regexp.Regexp{regexp.MustCompile(`relation_expr 'EXPERIMENTAL_AUDIT'`)},
		replace: map[string]string{
			"relation_expr": "table_name",
		},
		regreplace: map[string]string{
			`'READ' 'WRITE' .*`: `'READ' 'WRITE'`,
			`'OFF' .*`:          `'OFF'`,
		},
	},
	{
		name:    "alter_table_partition_by",
		stmt:    "alter_onetable_stmt",
		inline:  []string{"alter_table_cmds", "alter_table_cmd", "partition_by"},
		replace: map[string]string{"relation_expr": "table_name"},
		regreplace: map[string]string{
			`'NOTHING' .*`:        `'NOTHING'`,
			`_partitions '\)' .*`: `_partitions ')'`,
		},
		match: []*regexp.Regexp{regexp.MustCompile("relation_expr 'PARTITION")},
	},
	{
		name:    "alter_index_partition_by",
		stmt:    "alter_oneindex_stmt",
		inline:  []string{"alter_index_cmds", "alter_index_cmd", "partition_by", "table_index_name"},
		replace: map[string]string{"standalone_index_name": "index_name"},
	},
	{
		name:    "create_table_partition_by",
		stmt:    "create_table_stmt",
		inline:  []string{"opt_partition_by", "partition_by"},
		replace: map[string]string{"opt_table_elem_list": "table_definition", "opt_interleave": ""},
		match:   []*regexp.Regexp{regexp.MustCompile("PARTITION")},
		unlink:  []string{"table_definition"},
	},
	{
		name:   "explain_stmt",
		inline: []string{"explain_option_list"},
		replace: map[string]string{
			"explain_option_name": "( 'VERBOSE' | 'TYPES' | 'OPT' | 'DISTSQL' | 'VEC' )",
		},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'ANALYZE'"),
			regexp.MustCompile("'ANALYSE'"),
		},
	},
	{
		name:   "explain_analyze_stmt",
		stmt:   "explain_stmt",
		inline: []string{"explain_option_list"},
		replace: map[string]string{
			"explain_option_name": "( 'PLAN' | 'DISTSQL' | 'DEBUG' )",
		},
		unlink: []string{"'DISTSQL'"},
	},
	{
		name: "export_stmt",
		replace: map[string]string{
			"import_data_format":    "CSV",
			"string_or_placeholder": "file_location",
			"select_stmt":           "(| 'select_stmt' | 'TABLE' 'table_name')",
		},
		unlink: []string{"CSV", "file_location"},
	},
	{
		name:   "family_def",
		inline: []string{"name_list"},
	},
	{
		name:   "grant_stmt",
		inline: []string{"privileges", "opt_privileges_clause"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'TYPE' target_types"),
			regexp.MustCompile("'SCHEMA' schema_name_list"),
		},
		unlink: []string{"targets"},
	},
	{
		name: "foreign_key_column_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'REFERENCES' parent_table ( '(' ref_column_name ')' | ) ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink: []string{"table_name", "column_name", "column_type", "parent_table", "table_constraints"},
	},
	{
		name: "foreign_key_table_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' constraint_name | ) 'FOREIGN KEY' '(' ( fk_column_name ( ',' fk_column_name )* ) ')' 'REFERENCES' parent_table ( '(' ( ref_column_name ( ',' ref_column_name )* ) ')' | ) ( table_constraints | ) ')'"},
		unlink: []string{"table_name", "column_name", "parent_table", "table_constraints"},
	},
	{
		name:    "index_def",
		inline:  []string{"opt_storing", "storing", "index_params", "opt_name", "opt_hash_sharded"},
		replace: map[string]string{"a_expr": "n_buckets"},
		unlink:  []string{"n_buckets"},
	},
	{
		name:   "import_csv",
		stmt:   "import_stmt",
		inline: []string{"string_or_placeholder_list", "opt_with_options"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'IMPORT' import_format"),
			regexp.MustCompile("'FROM' import_format"),
			regexp.MustCompile("'WITH' 'OPTIONS'"),
		},
		replace: map[string]string{
			"string_or_placeholder": "file_location",
			"import_format":         "'CSV'",
		},
		unlink: []string{"file_location"},
	},
	{
		name:   "import_into",
		stmt:   "import_stmt",
		match:  []*regexp.Regexp{regexp.MustCompile("INTO")},
		inline: []string{"insert_column_list", "string_or_placeholder_list", "opt_with_options", "kv_option_list"},
		replace: map[string]string{
			"table_option":          "table_name",
			"insert_column_item":    "column_name",
			"import_format":         "( 'CSV' | 'AVRO' | 'DELIMITED' )",
			"string_or_placeholder": "file_location",
			"kv_option":             "option '=' value"},
		unlink: []string{"table_name", "column_name", "file_location", "option", "value"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'WITH' 'OPTIONS'"),
		},
	},
	{
		name:   "import_dump",
		stmt:   "import_stmt",
		inline: []string{"string_or_placeholder_list", "opt_with_options"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("CREATE' 'USING'"),
			regexp.MustCompile("table_elem_list"),
			regexp.MustCompile("'WITH' 'OPTIONS'"),
		},
		replace: map[string]string{
			"string_or_placeholder": "file_location",
		},
		unlink: []string{"file_location"},
	},
	{
		name:    "insert_stmt",
		inline:  []string{"insert_target", "insert_rest", "returning_clause", "insert_column_list", "insert_column_item", "target_list", "opt_with_clause", "with_clause", "cte_list"},
		nosplit: true,
	},
	{
		name:   "like_table_option_list",
		inline: []string{"like_table_option"},
	},
	{
		name: "on_conflict",
		inline: []string{"name_list", "set_clause_list", "insert_column_list",
			"insert_column_item", "set_clause", "single_set_clause", "multiple_set_clause", "in_expr", "expr_list",
			"expr_tuple1_ambiguous", "tuple1_ambiguous_values"},
		replace: map[string]string{
			"select_with_parens": "'(' select_stmt ')'",
			"opt_where_clause":   "",
		},
		nosplit: true,
	},
	{name: "iso_level"},
	{
		name:    "interleave",
		stmt:    "create_table_stmt",
		inline:  []string{"opt_interleave", "opt_table_with", "opt_with_storage_parameter_list", "storage_parameter_list", "opt_create_table_on_commit"},
		replace: map[string]string{"opt_table_elem_list": "table_definition"},
		unlink:  []string{"table_definition"},
	},
	{
		name: "not_null_column_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'NOT NULL' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink: []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{
		name: "opt_interleave",
	},
	{
		name:   "opt_with_storage_parameter_list",
		inline: []string{"storage_parameter_list"},
	},
	{
		name:    "pause_job",
		stmt:    "pause_jobs_stmt",
		replace: map[string]string{"a_expr": "job_id"},
		unlink:  []string{"job_id"},
	},
	{
		name:    "pause_schedule",
		stmt:    "pause_schedules_stmt",
		replace: map[string]string{"a_expr": "schedule_id"},
		unlink:  []string{"schedule_id"},
	},
	{
		name: "primary_key_column_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'PRIMARY KEY' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink: []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{
		name: "primary_key_table_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' name | ) 'PRIMARY KEY' '(' ( column_name ( ',' column_name )* ) ')' ( table_constraints | ) ')'"},
		unlink: []string{"table_name", "column_name", "table_constraints"},
	},
	{
		name: "refresh_materialized_views",
		stmt: "refresh_stmt",
	},
	{
		name:   "release_savepoint",
		stmt:   "release_stmt",
		inline: []string{"savepoint_name"},
	},
	{
		name:    "rename_column",
		stmt:    "alter_rename_table_stmt",
		inline:  []string{"opt_column"},
		match:   []*regexp.Regexp{regexp.MustCompile("'ALTER' 'TABLE' .* 'RENAME' ('COLUMN'|name)")},
		replace: map[string]string{"relation_expr": "table_name", "name 'TO'": "current_name 'TO'"},
		unlink:  []string{"table_name", "current_name"},
	},
	{
		name:    "rename_constraint",
		stmt:    "alter_onetable_stmt",
		replace: map[string]string{"relation_expr": "table_name", "alter_table_cmds": "'RENAME' 'CONSTRAINT' current_name 'TO' name"},
		unlink:  []string{"table_name", "current_name"},
	},
	{
		name:  "rename_database",
		stmt:  "alter_rename_database_stmt",
		match: []*regexp.Regexp{regexp.MustCompile("'ALTER' 'DATABASE'")}},
	{
		name:    "rename_index",
		stmt:    "alter_rename_index_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'ALTER' 'INDEX'")},
		inline:  []string{"table_index_name"},
		replace: map[string]string{"standalone_index_name": "index_name"},
	},
	{
		name:    "rename_sequence",
		stmt:    "alter_rename_sequence_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'ALTER' 'SEQUENCE' .* 'RENAME' 'TO'")},
		replace: map[string]string{"relation_expr": "current_name", "sequence_name": "new_name"},
		unlink:  []string{"current_name"},
		relink:  map[string]string{"new_name": "name"}},
	{
		name:    "rename_table",
		stmt:    "alter_rename_table_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'ALTER' 'TABLE' .* 'RENAME' 'TO'")},
		replace: map[string]string{"relation_expr": "current_name", "qualified_name": "new_name"},
		unlink:  []string{"current_name"}, relink: map[string]string{"new_name": "name"}},
	{
		name:   "restore",
		stmt:   "restore_stmt",
		inline: []string{"opt_as_of_clause", "as_of_clause", "opt_with_restore_options"},
		match:  []*regexp.Regexp{regexp.MustCompile("'FROM'")},
		replace: map[string]string{
			"a_expr": "timestamp",
			"'WITH' 'OPTIONS' '(' kv_option_list ')'": "",
			"targets":                                "( 'TABLE' table_pattern ( ( ',' table_pattern ) )* | 'DATABASE' database_name ( ( ',' database_name ) )* )",
			"string_or_placeholder":                  "subdirectory",
			"list_of_string_or_placeholder_opt_list": "( destination | '(' partitioned_backup_location ( ',' partitioned_backup_location )* ')' )",
		},
		unlink: []string{"subdirectory", "timestamp", "destination", "partitioned_backup_location"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'REPLICATION' 'STREAM' 'FROM'"),
		},
	},
	{
		name:    "resume_job",
		stmt:    "resume_jobs_stmt",
		replace: map[string]string{"a_expr": "job_id"},
		unlink:  []string{"job_id"},
	},
	{
		name:    "resume_schedule",
		stmt:    "resume_schedules_stmt",
		replace: map[string]string{"a_expr": "schedule_id"},
		unlink:  []string{"schedule_id"},
	},
	{
		name:   "revoke_stmt",
		inline: []string{"privileges", "opt_privileges_clause"},
		exclude: []*regexp.Regexp{
			regexp.MustCompile("'TYPE' target_types"),
			regexp.MustCompile("'SCHEMA' schema_name_list"),
		},
		unlink: []string{"targets"},
	},
	{
		name:    "rollback_transaction",
		stmt:    "rollback_stmt",
		inline:  []string{"opt_transaction"},
		match:   []*regexp.Regexp{regexp.MustCompile("'ROLLBACK'")},
		replace: map[string]string{"'TRANSACTION'": "", "'TO'": "'TO' 'SAVEPOINT'"},
	},
	{
		name:   "limit_clause",
		inline: []string{"row_or_rows", "first_or_next"},
		replace: map[string]string{
			"select_limit_value":           "count",
			"opt_select_fetch_first_value": "count",
		},
	},
	{
		name:   "offset_clause",
		inline: []string{"row_or_rows"},
	},
	{name: "savepoint_stmt", inline: []string{"savepoint_name"}},
	{
		name: "select_stmt",
		inline: []string{
			"with_clause",
			"cte_list",
			"select_no_parens",
			"opt_sort_clause",
			"select_limit",
		},
		replace: map[string]string{
			"( simple_select |":    "(",
			"| select_with_parens": "",
			"select_clause sort_clause | select_clause ( sort_clause |  ) ( limit_clause offset_clause | offset_clause limit_clause | limit_clause | offset_clause )":                                                                                                                                           "select_clause ( sort_clause | ) ( limit_clause | ) ( offset_clause | )",
			"| ( 'WITH' ( ( common_table_expr ) ( ( ',' common_table_expr ) )* ) ) select_clause sort_clause | ( 'WITH' ( ( common_table_expr ) ( ( ',' common_table_expr ) )* ) ) select_clause ( sort_clause |  ) ( limit_clause offset_clause | offset_clause limit_clause | limit_clause | offset_clause )": "( sort_clause | ) ( limit_clause | ) ( offset_clause | )",
		},
		unlink:  []string{"index_name"},
		nosplit: true,
	},
	{
		name:   "select_clause",
		inline: []string{"simple_select"},
		replace: map[string]string{
			"| select_with_parens": "| '(' ( simple_select_clause | values_clause | table_clause | set_operation ) ')'",
		},
		nosplit: true,
	},
	{name: "table_clause"},
	{
		name:    "set_operation",
		inline:  []string{"all_or_distinct"},
		nosplit: true,
	},
	{
		name:   "values_clause",
		inline: []string{"expr_list"},
	},
	{
		name:    "simple_select_clause",
		inline:  []string{"opt_all_clause", "distinct_clause", "distinct_on_clause", "opt_as_of_clause", "as_of_clause", "expr_list", "target_list", "from_clause", "opt_where_clause", "where_clause", "group_clause", "having_clause", "window_clause", "from_list"},
		unlink:  []string{"index_name"},
		nosplit: true,
	},
	{
		name:    "joined_table",
		inline:  []string{"join_qual", "name_list", "join_type", "join_outer"},
		nosplit: true,
	},
	{
		name:   "table_ref",
		inline: []string{"opt_ordinality", "opt_alias_clause", "opt_expr_list", "opt_column_list", "name_list", "alias_clause"},
		replace: map[string]string{
			"select_with_parens": "'(' select_stmt ')'",
			"opt_index_flags":    "( '@' index_name | )",
			"relation_expr":      "table_name",
			"func_table":         "func_application",
			//			"| func_name '(' ( expr_list |  ) ')' ( 'WITH' 'ORDINALITY' |  ) ( ( 'AS' table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) | table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) ) |  )": "",
			"| special_function ( 'WITH' 'ORDINALITY' |  ) ( ( 'AS' table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) | table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) ) |  )": "",
			"| '(' joined_table ')' ( 'WITH' 'ORDINALITY' |  ) ( 'AS' table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) | table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) )":    "| '(' joined_table ')' ( 'WITH' 'ORDINALITY' |  ) ( ( 'AS' table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) | table_alias_name ( '(' ( ( name ) ( ( ',' name ) )* ) ')' |  ) ) |  )",
		},
		unlink:  []string{"index_name"},
		nosplit: true,
	},
	{
		name:   "set_var",
		stmt:   "preparable_set_stmt",
		inline: []string{"set_session_stmt", "set_rest_more", "generic_set", "var_list", "to_or_eq"},
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
		stmt:   "preparable_set_stmt",
		inline: []string{"set_csetting_stmt", "generic_set", "var_list", "to_or_eq"},
		match: []*regexp.Regexp{
			regexp.MustCompile("'SET' 'CLUSTER'"),
		},
	},
	{
		name: "set_transaction",
		stmt: "nonpreparable_set_stmt",
		inline: []string{
			"set_transaction_stmt",
			"transaction_mode",
			"transaction_mode_list",
			"transaction_read_mode",
			"transaction_user_priority",
			"user_priority",
			"as_of_clause",
			"opt_comma",
			"transaction_deferrable_mode",
		},
		match: []*regexp.Regexp{regexp.MustCompile("'SET' 'TRANSACTION'")},
	},
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
		stmt: "show_csettings_stmt",
	},
	{
		name:   "show_columns_stmt",
		inline: []string{"with_comment"},
	},
	{
		name:    "show_constraints",
		stmt:    "show_stmt",
		match:   []*regexp.Regexp{regexp.MustCompile("'SHOW' 'CONSTRAINTS'")},
		replace: map[string]string{"var_name": "table_name"},
		unlink:  []string{"table_name"},
	},
	{
		name:    "show_create_stmt",
		replace: map[string]string{"table_name": "object_name"},
		unlink:  []string{"object_name"},
	},
	{
		name:   "show_databases_stmt",
		inline: []string{"with_comment"},
	},
	{
		name: "show_enums",
		stmt: "show_enums_stmt",
	},
	{
		name:   "show_backup",
		stmt:   "show_backup_stmt",
		inline: []string{"opt_with_options"},
		replace: map[string]string{
			"'BACKUPS' 'IN' string_or_placeholder":                      "'BACKUPS' 'IN' location",
			"'BACKUP' string_or_placeholder 'IN' string_or_placeholder": "'BACKUP' subdirectory 'IN' location",
			"'BACKUP' 'SCHEMAS' string_or_placeholder":                  "'BACKUP' 'SCHEMAS' location",
		},
		unlink: []string{"location"},
	},
	{
		name:    "show_jobs",
		stmt:    "show_jobs_stmt",
		replace: map[string]string{"a_expr": "job_id"},
		unlink:  []string{"job_id"},
	},
	{
		name: "show_grants_stmt",
		inline: []string{
			"opt_on_targets_roles",
			"for_grantee_clause",
			"targets_roles",
			"name_list",
			"schema_name_list",
			"type_name_list",
		},
		replace: map[string]string{
			"targets":                 "( | 'TABLE' table_name ( ( ',' table_name ) )* | 'DATABASE' database_name ( ( ',' database_name ) )* )",
			"qualifiable_schema_name": "schema_name",
		},
		unlink: []string{"table_name", "database_name", "schema_name", "name"},
	},
	{
		name:   "show_indexes",
		inline: []string{"with_comment"},
		stmt:   "show_indexes_stmt",
	},
	{
		name:    "show_index",
		stmt:    "show_stmt",
		inline:  []string{"with_comment"},
		match:   []*regexp.Regexp{regexp.MustCompile("'SHOW' 'INDEX'")},
		replace: map[string]string{"var_name": "table_name"},
		unlink:  []string{"table_name"},
	},
	{
		name:  "show_keys",
		stmt:  "show_stmt",
		match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'KEYS'")},
	},
	{
		name:    "show_locality",
		stmt:    "show_roles_stmt",
		replace: map[string]string{"'ROLES'": "'LOCALITY'"},
	},
	{
		name: "show_partitions_stmt",
	},
	{
		name: "show_regions",
		stmt: "show_regions_stmt",
	},
	{
		name:   "show_statements",
		stmt:   "show_statements_stmt",
		inline: []string{"opt_cluster", "statements_or_queries"},
	},
	{
		name: "show_roles_stmt",
	},
	{
		name: "show_users_stmt",
	},
	{
		name: "show_ranges_stmt",
		stmt: "show_ranges_stmt",
	},
	{
		name:    "show_range_for_row_stmt",
		stmt:    "show_range_for_row_stmt",
		inline:  []string{"expr_list"},
		replace: map[string]string{"a_expr": "row_vals"},
		unlink:  []string{"row_vals"},
	},
	{
		name:   "show_schedules",
		stmt:   "show_schedules_stmt",
		inline: []string{"schedule_state", "opt_schedule_executor_type"},
	},
	{
		name: "show_schemas",
		stmt: "show_schemas_stmt",
	},
	{
		name: "show_sequences",
		stmt: "show_sequences_stmt",
	},
	{
		name:   "show_sessions",
		stmt:   "show_sessions_stmt",
		inline: []string{"opt_cluster"},
	},
	{
		name: "show_stats",
		stmt: "show_stats_stmt",
	},
	{
		name:    "show_tables",
		stmt:    "show_tables_stmt",
		inline:  []string{"with_comment"},
		replace: map[string]string{"'FROM' name": "'FROM' database_name", "'.' name": "'.' schema_name"},
		unlink:  []string{"schema.name"},
	},
	{
		name:    "show_trace",
		stmt:    "show_trace_stmt",
		inline:  []string{"opt_compact"},
		exclude: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'EXPERIMENTAL_REPLICA'")},
	},
	{
		name:  "show_transaction",
		stmt:  "show_stmt",
		match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'TRANSACTION'")},
	},
	{
		name:  "show_savepoint_status",
		stmt:  "show_savepoint_stmt",
		match: []*regexp.Regexp{regexp.MustCompile("'SHOW' 'SAVEPOINT' 'STATUS'")},
	},
	{
		name:   "show_zone_stmt",
		inline: []string{"opt_partition", "table_index_name", "partition", "from_with_implicit_for_alias"},
	},
	{
		name:   "sort_clause",
		inline: []string{"sortby_list", "sortby", "opt_asc_desc", "opt_nulls_order"},
	},
	{
		name:    "split_index_at",
		stmt:    "alter_split_index_stmt",
		inline:  []string{"table_index_name"},
		replace: map[string]string{"standalone_index_name": "index_name"},
	},
	{
		name:   "split_table_at",
		stmt:   "alter_split_stmt",
		unlink: []string{"table_name"},
	},
	{
		name:   "table_constraint",
		inline: []string{"constraint_elem", "opt_storing", "storing", "opt_hash_sharded"},
		replace: map[string]string{
			"'=' a_expr": "'=' n_buckets",
		},
		unlink: []string{"n_buckets"},
	},
	{
		name:   "opt_persistence_temp_table",
		inline: []string{"opt_temp"},
	},
	{
		name:    "truncate_stmt",
		inline:  []string{"opt_table", "relation_expr_list", "opt_drop_behavior"},
		replace: map[string]string{"relation_expr": "table_name"},
		unlink:  []string{"table_name"},
	},
	{
		name: "unique_column_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' column_name column_type 'UNIQUE' ( column_constraints | ) ( ',' ( column_def ( ',' column_def )* ) | ) ( table_constraints | ) ')' ')'"},
		unlink: []string{"table_name", "column_name", "column_type", "table_constraints"},
	},
	{
		name: "unique_table_level",
		stmt: "stmt_block",
		replace: map[string]string{"	stmt": "	'CREATE' 'TABLE' table_name '(' ( column_def ( ',' column_def )* ) ( 'CONSTRAINT' name | ) 'UNIQUE' '(' ( column_name ( ',' column_name )* ) ')' ( table_constraints | ) ')'"},
		unlink: []string{"table_name", "check_expr", "table_constraints"},
	},
	{
		name:    "unsplit_index_at",
		stmt:    "alter_unsplit_index_stmt",
		inline:  []string{"table_index_name"},
		replace: map[string]string{"standalone_index_name": "index_name"},
	},
	{
		name:   "unsplit_table_at",
		stmt:   "alter_unsplit_stmt",
		unlink: []string{"table_name"},
	},
	{
		name: "update_stmt",
		inline: []string{
			"opt_with_clause",
			"with_clause",
			"cte_list",
			"table_expr_opt_alias_idx",
			"table_name_opt_idx",
			"set_clause_list",
			"set_clause",
			"single_set_clause",
			"multiple_set_clause",
			"in_expr",
			"expr_list",
			"expr_tuple1_ambiguous",
			"tuple1_ambiguous_values",
			"opt_where_clause",
			"where_clause",
			"opt_sort_clause",
			"returning_clause",
			"insert_column_list",
			"insert_column_item",
			"opt_limit_clause",
			"opt_only",
			"opt_descendant",
			"opt_from_list",
			"from_list",
		},
		replace: map[string]string{
			"relation_expr":      "table_name",
			"select_with_parens": "'(' select_stmt ')'",
		},
		relink: map[string]string{
			"table_name":       "relation_expr",
			"column_name_list": "insert_column_list",
		},
		nosplit: true,
	},
	{
		name:    "upsert_stmt",
		stmt:    "upsert_stmt",
		inline:  []string{"insert_target", "insert_rest", "returning_clause", "opt_with_clause", "with_clause", "cte_list", "insert_column_list", "insert_column_item"},
		unlink:  []string{"select_stmt"},
		nosplit: true,
	},
	{
		name:    "validate_constraint",
		stmt:    "alter_onetable_stmt",
		replace: map[string]string{"alter_table_cmds": "'VALIDATE' 'CONSTRAINT' constraint_name", "relation_expr": "table_name"},
		unlink:  []string{"constraint_name", "table_name"},
	},
	{
		name:   "window_definition",
		inline: []string{"window_specification"},
	},
	{
		name:   "opt_frame_clause",
		inline: []string{"frame_extent"},
	},
}

// getAllStmtSpecs returns a slice of stmtSpecs for all sql.y statements that
// should have a diagram generated for.
// getAllStmtSpecs appends to the "specs" slice any sql.y statements that do
// not have an entry in specs but are not specified to be skipped.
func getAllStmtSpecs(sqlGrammarFile string, bnfAPITimeout time.Duration) ([]stmtSpec, error) {
	sqlStmts := make(map[string]struct{})
	// Map all the sql stmts that are defined in specs.
	for _, s := range specs {
		sqlStmts[s.GetStatement()] = struct{}{}
	}

	file, err := os.Open(sqlGrammarFile)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bnf, err := runBNF(sqlGrammarFile, bnfAPITimeout)
	if err != nil {
		return nil, err
	}

	br := func() io.Reader {
		return bytes.NewReader(bnf)
	}

	grammar, err := extract.ParseGrammar(br())
	if err != nil {
		return nil, err
	}

	stmtRegex := regexp.MustCompile(`%type\s*<tree.Statement>\s*(.*)$`)

	scanner := bufio.NewScanner(file)
	if err != nil {
		return nil, err
	}
	for scanner.Scan() {
		text := scanner.Text()
		if matches := stmtRegex.FindAllStringSubmatch(text, -1); len(matches) > 0 {
			for _, match := range matches {
				// The second submatch does not include <tree.Statement>.
				// We want to get only the stmt names.
				stmts := strings.Split(match[1], " ")
				for _, stmt := range stmts {
					// If the statement does not appear in grammar, the statement
					// has no branches that are required to be documented, we can
					// skip it.
					if _, ok := grammar[stmt]; !ok {
						continue
					}

					// If the statement is not defined in specs, create an entry.
					if _, found := sqlStmts[stmt]; !found {
						specs = append(specs, stmtSpec{
							name: stmt,
						})
						sqlStmts[stmt] = struct{}{}
					}
				}
			}
		}
	}

	return specs, nil
}

// regList is a common regex used when removing loops from alter and drop
// statements.
const regList = ` \( \( ',' .* \) \)\*`
