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

package parser

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
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
	fmt.Fprint(&buf, "help: ")
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
	msg := HelpMessage{Command: helpText, HelpMessageBody: HelpMessages[helpText]}
	sqllex.(*Scanner).SetHelp(msg)
	// We return non-zero to indicate to the caller of Parse() that the
	// parse was unsuccessful.
	return 1
}

// helpWithFunction is to be used in parser actions to mark the parser
// "in error", with the error set to a contextual help message about
// the current built-in function.
func helpWithFunction(sqllex sqlLexer, f ResolvableFunctionReference) int {
	d, err := f.Resolve(nil)
	if err != nil {
		return 1
	}

	msg := HelpMessage{
		Function: f.String(),
		HelpMessageBody: HelpMessageBody{
			Category: "built-in functions",
			SeeAlso:  `https://www.cockroachlabs.com/docs/functions-and-operators.html`,
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
		b := overload.(Builtin)
		if b.Info != "" && b.Info != lastInfo {
			if i > 0 {
				fmt.Fprintln(w, "---")
			}
			fmt.Fprintf(w, "\n%s\n\n", b.Info)
			fmt.Fprintln(w, "Signature\tCategory")
		}
		lastInfo = b.Info

		cat := b.Category()
		if cat != "" {
			cat = "[" + cat + "]"
		}
		fmt.Fprintf(w, "%s%s\t%s\n", d.Name, b.Signature(), cat)
	}
	_ = w.Flush()
	msg.Text = buf.String()

	sqllex.(*Scanner).SetHelp(msg)
	return 1
}

const (
	hGroup = ""
	hDDL   = "schema manipulation"
	hDML   = "data manipulation"
	hTxn   = "transaction control"
	hPriv  = "privileges and security"
	hMisc  = "miscellaneous"
	hCfg   = "configuration"
	hCCL   = "enterprise features"
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
// via a (subsequent) \h client-side command.
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
		return strings.Replace(seeAlso, ", ", "\n  ", -1)
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
