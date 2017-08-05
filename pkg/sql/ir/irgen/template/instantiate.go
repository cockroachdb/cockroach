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

package template

import (
	"fmt"
	"io"
	"regexp"
)

// Node is a type for hierarchical data.
type Node struct {
	label        string
	replacements []replacement
	children     []*Node
}

type replacement struct {
	expr *regexp.Regexp
	repl string
}

// NewRoot returns a new root node.
func NewRoot() *Node {
	return &Node{}
}

// AddReplacement adds a regexp replacement to the given node. The replacement
// value is formatted with a %s conversion.
func (node *Node) AddReplacement(expr string, repl interface{}) {
	node.replacements = append(node.replacements, replacement{regexp.MustCompile(expr), fmt.Sprintf("%s", repl)})
}

// AddReplacementf adds a regexp replacement to the given node. The replacement
// format and arguments are formatted with fmt.Sprintf.
func (node *Node) AddReplacementf(expr string, replFormat string, a ...interface{}) {
	node.AddReplacement(expr, fmt.Sprintf(replFormat, a...))
}

// NewChild adds a child with the given label to the given node.
func (node *Node) NewChild(label string) *Node {
	child := &Node{label: label}
	node.children = append(node.children, child)
	return child
}

// Instantiate executes the given template on the given input.
func (tmpl *Template) Instantiate(w io.Writer, node *Node) error {
	return tmpl.instantiate(w, node, append([]replacement(nil), node.replacements...))
}

func (tmpl *Template) instantiate(w io.Writer, node *Node, replacements []replacement) error {
	switch tmpl.typ {
	case blockStatement:
		return tmpl.instantiateChildren(w, node, replacements)
	case echoStatement:
		text := tmpl.arg
		for j := range replacements {
			r := replacements[len(replacements)-1-j]
			text = r.expr.ReplaceAllString(text, r.repl)
		}
		_, err := fmt.Fprintln(w, text)
		return err
	case forStatement:
		for _, nodeChild := range node.children {
			if nodeChild.label != tmpl.arg {
				continue
			}
			if err := tmpl.instantiateChildren(w, nodeChild, append(replacements, nodeChild.replacements...)); err != nil {
				return err
			}
		}
		return nil
	case ifStatement:
		for _, nodeChild := range node.children {
			if nodeChild.label != tmpl.arg {
				continue
			}
			return tmpl.instantiateChildren(w, node, replacements)
		}
		return nil
	default:
		panic(fmt.Sprintf("case %d is not handled", tmpl.typ))
	}
}

func (tmpl *Template) instantiateChildren(
	w io.Writer, node *Node, replacements []replacement,
) error {
	for _, child := range tmpl.children {
		if err := child.instantiate(w, node, replacements); err != nil {
			return err
		}
	}
	return nil
}
