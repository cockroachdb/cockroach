// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parser

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// HelpMessage describes a contextual help message.
type HelpMessage struct {
	// Command is set if the message is about a statement.
	Command string
	// Function is set if the message is about a built-in function.
	Function string

	// HelpMessageBody contains the details of the message.
	HelpMessageBody
}

// String implements the fmt.String interface.
func (h *HelpMessage) String() string {
	var buf bytes.Buffer
	buf.WriteString("help:\n")
	h.Format(&buf)
	return buf.String()
}

// Format prints out details about the message onto the specified output stream.
func (h *HelpMessage) Format(w io.Writer) {
	if h.Command != "" {
		fmt.Fprintf(w, "Command:     %s\n", h.Command)
	}
	if h.Function != "" {
		fmt.Fprintf(w, "Function:    %s\n", h.Function)
	}
	if h.ShortDescription != "" {
		fmt.Fprintf(w, "Description: %s\n", h.ShortDescription)
	}
	if h.Category != "" {
		fmt.Fprintf(w, "Category:    %s\n", h.Category)
	}
	if h.Command != "" {
		fmt.Fprintln(w, "Syntax:")
	}
	fmt.Fprintln(w, strings.TrimSpace(h.Text))
	if h.SeeAlso != "" {
		fmt.Fprintf(w, "\nSee also:\n  %s\n", h.SeeAlso)
	}
}

// helpWith is to be used in parser actions to mark the parser "in
// error", with the error set to a contextual help message about the
// current statement.
func helpWith(sqllex sqlLexer, helpText string) int {
	scan := sqllex.(*lexer)
	if helpText == "" {
		scan.lastError = pgerror.WithCandidateCode(errors.New("help upon syntax error"), pgcode.Syntax)
		scan.populateHelpMsg("help:\n" + AllHelp)
		return 1
	}
	msg := HelpMessage{Command: helpText, HelpMessageBody: HelpMessages[helpText]}
	scan.SetHelp(msg)
	// We return non-zero to indicate to the caller of Parse() that the
	// parse was unsuccessful.
	return 1
}

// helpWithFunction is to be used in parser actions to mark the parser
// "in error", with the error set to a contextual help message about
// the current built-in function.
func helpWithFunction(sqllex sqlLexer, f tree.ResolvableFunctionReference) int {
	d, err := f.Resolve(sessiondata.SearchPath{})
	if err != nil {
		return 1
	}

	msg := HelpMessage{
		Function: f.String(),
		HelpMessageBody: HelpMessageBody{
			Category: d.Category,
			SeeAlso:  docs.URL("functions-and-operators.html"),
		},
	}

	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', 0)

	// Each function definition contains one or more overloads. We need
	// to extract them all; moreover each overload may have a different
	// documentation, so we need to also combine the descriptions
	// together.
	lastInfo := ""
	for i, overload := range d.Definition {
		b := overload.(*tree.Overload)
		if b.Info != "" && b.Info != lastInfo {
			if i > 0 {
				fmt.Fprintln(w, "---")
			}
			fmt.Fprintf(w, "\n%s\n\n", b.Info)
			fmt.Fprintln(w, "Signature")
		}
		lastInfo = b.Info

		simplifyRet := d.Class == tree.GeneratorClass
		fmt.Fprintf(w, "%s%s\n", d.Name, b.Signature(simplifyRet))
	}
	_ = w.Flush()
	msg.Text = buf.String()

	sqllex.(*lexer).SetHelp(msg)
	return 1
}

func helpWithFunctionByName(sqllex sqlLexer, s string) int {
	un := &tree.UnresolvedName{NumParts: 1, Parts: tree.NameParts{s}}
	return helpWithFunction(sqllex, tree.ResolvableFunctionReference{FunctionReference: un})
}

const (
	hGroup        = ""
	hDDL          = "schema manipulation"
	hDML          = "data manipulation"
	hTxn          = "transaction control"
	hPriv         = "privileges and security"
	hMisc         = "miscellaneous"
	hCfg          = "configuration"
	hExperimental = "experimental"
	hCCL          = "enterprise features"
)

// HelpMessageBody defines the body of a help text. The messages are
// structured to facilitate future help navigation functionality.
type HelpMessageBody struct {
	Category         string
	ShortDescription string
	Text             string
	SeeAlso          string
}

// HelpMessages is the registry of all help messages, keyed by the
// top-level statement that they document. The key is intended for use
// via the \h client-side command.
var HelpMessages = func(h map[string]HelpMessageBody) map[string]HelpMessageBody {
	appendSeeAlso := func(newItem, prevItems string) string {
		// "See also" items start with no indentation, and then use two
		// space indentation from the 2nd item onward.
		if prevItems != "" {
			return newItem + "\n  " + prevItems
		}
		return newItem
	}
	reformatSeeAlso := func(seeAlso string) string {
		return strings.Replace(
			strings.Replace(seeAlso, ", ", "\n  ", -1),
			"WEBDOCS", docs.URLBase, -1)
	}
	srcMsg := h["<SOURCE>"]
	srcMsg.SeeAlso = reformatSeeAlso(strings.TrimSpace(srcMsg.SeeAlso))
	selectMsg := h["<SELECTCLAUSE>"]
	selectMsg.SeeAlso = reformatSeeAlso(strings.TrimSpace(selectMsg.SeeAlso))
	for k, m := range h {
		m = h[k]
		m.ShortDescription = strings.TrimSpace(m.ShortDescription)
		m.Text = strings.TrimSpace(m.Text)
		m.SeeAlso = strings.TrimSpace(m.SeeAlso)

		// If the description contains <source>, append the <source> help.
		if strings.Contains(m.Text, "<source>") && k != "<SOURCE>" {
			m.Text = strings.TrimSpace(m.Text) + "\n\n" + strings.TrimSpace(srcMsg.Text)
			m.SeeAlso = appendSeeAlso(srcMsg.SeeAlso, m.SeeAlso)
		}
		// Ditto for <selectclause>.
		if strings.Contains(m.Text, "<selectclause>") && k != "<SELECTCLAUSE>" {
			m.Text = strings.TrimSpace(m.Text) + "\n\n" + strings.TrimSpace(selectMsg.Text)
			m.SeeAlso = appendSeeAlso(selectMsg.SeeAlso, m.SeeAlso)
		}
		// If the description contains <tablename>, mention SHOW TABLES in "See Also".
		if strings.Contains(m.Text, "<tablename>") {
			m.SeeAlso = appendSeeAlso("SHOW TABLES", m.SeeAlso)
		}
		m.SeeAlso = reformatSeeAlso(m.SeeAlso)
		h[k] = m
	}
	return h
}(helpMessages)

// AllHelp contains an overview of all statements with help messages.
// For example, displayed in the CLI shell with \h without additional parameters.
var AllHelp = func(h map[string]HelpMessageBody) string {
	// Aggregate the help items.
	cmds := make(map[string][]string)
	for c, details := range h {
		if details.Category == "" {
			continue
		}
		cmds[details.Category] = append(cmds[details.Category], c)
	}

	// Ensure the result is deterministic.
	var categories []string
	for c, l := range cmds {
		categories = append(categories, c)
		sort.Strings(l)
	}
	sort.Strings(categories)

	// Compile the final help index.
	var buf bytes.Buffer
	w := tabwriter.NewWriter(&buf, 0, 0, 1, ' ', 0)
	for _, cat := range categories {
		fmt.Fprintf(w, "%s:\n", strings.Title(cat))
		for _, item := range cmds[cat] {
			fmt.Fprintf(w, "\t\t%s\t%s\n", item, h[item].ShortDescription)
		}
		fmt.Fprintln(w)
	}
	_ = w.Flush()
	return buf.String()
}(helpMessages)
