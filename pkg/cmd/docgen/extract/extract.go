// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package extract

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/url"
	"os/exec"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

const (
	rrAddr = "http://bottlecaps.de/rr/ui"
)

var (
	reIsExpr  = regexp.MustCompile("^[a-z_0-9]+$")
	reIsIdent = regexp.MustCompile("^[A-Z_0-9]+$")
	rrLock    syncutil.Mutex
)

// GenerateRRJar generates via the Railroad jar.
func GenerateRRJar(jar string, bnf []byte) ([]byte, error) {
	// Note: the RR generator is already multithreaded.  The
	// -max-workers setting at the toplevel is probably already
	// optimally set to 1.

	// JAR generation is enabled by placing Railroad.jar (ask mjibson for a link)
	// in the generate directory.
	cmd := exec.Command(
		"java",
		"-jar", jar,
		"-suppressebnf",
		"-color:#ffffff",
		"-width:760",
		"-")
	cmd.Stdin = bytes.NewReader(bnf)

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("%s: %s", err, out)
	}
	return out, nil
}

// GenerateRRNet generates the RR XHTML from a EBNF file.
func GenerateRRNet(bnf []byte, railroadAPITimeout time.Duration) ([]byte, error) {
	rrLock.Lock()
	defer rrLock.Unlock()

	v := url.Values{}
	v.Add("color", "#ffffff")
	v.Add("frame", "diagram")
	//v.Add("options", "suppressebnf")
	v.Add("text", string(bnf))
	v.Add("width", "760")
	v.Add("options", "eliminaterecursion")
	v.Add("options", "factoring")
	v.Add("options", "inline")

	httpClient := httputil.NewClientWithTimeout(railroadAPITimeout)
	resp, err := httpClient.Post(context.TODO(), rrAddr, "application/x-www-form-urlencoded", strings.NewReader(v.Encode()))
	if err != nil {
		return nil, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(body))
	}
	return body, nil
}

// GenerateBNF Opens or downloads the .y file at addr and returns at as an EBNF
// file. Unimplemented branches are removed. Resulting empty nodes and their
// uses are further removed. Empty nodes are elided.
func GenerateBNF(addr string, bnfAPITimeout time.Duration) (ebnf []byte, err error) {
	var b []byte
	if strings.HasPrefix(addr, "http") {
		httpClient := httputil.NewClientWithTimeout(bnfAPITimeout)
		resp, err := httpClient.Get(context.TODO(), addr)
		if err != nil {
			return nil, err
		}
		b, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}
		resp.Body.Close()
	} else {
		body, err := ioutil.ReadFile(addr)
		if err != nil {
			return nil, err
		}
		b = body
	}
	t, err := yacc.Parse(addr, string(b))
	if err != nil {
		return nil, err
	}
	buf := new(bytes.Buffer)

	// Remove unimplemented branches.
	prods := make(map[string][][]yacc.Item)
	for _, p := range t.Productions {
		var impl [][]yacc.Item
		for _, e := range p.Expressions {
			if strings.Contains(e.Command, "unimplemented") && !strings.Contains(e.Command, "FORCE DOC") {
				continue
			}
			if strings.Contains(e.Command, "SKIP DOC") {
				continue
			}
			impl = append(impl, e.Items)
		}
		prods[p.Name] = impl
	}
	// Cascade removal of empty nodes. That is, for any node that has no branches,
	// remove it and anything it refers to.
	for {
		changed := false
		for name, exprs := range prods {
			var next [][]yacc.Item
			for _, expr := range exprs {
				add := true
				var items []yacc.Item
				for _, item := range expr {
					p := prods[item.Value]
					if item.Typ == yacc.TypToken && !isUpper(item.Value) && len(p) == 0 {
						add = false
						changed = true
						break
					}
					// Remove items that have one branch which accepts nothing.
					if len(p) == 1 && len(p[0]) == 0 {
						changed = true
						continue
					}
					items = append(items, item)
				}
				if add {
					next = append(next, items)
				}
			}
			prods[name] = next
		}
		if !changed {
			break
		}
	}

	start := true
	for _, prod := range t.Productions {
		p := prods[prod.Name]
		if len(p) == 0 {
			continue
		}
		if start {
			start = false
		} else {
			buf.WriteString("\n")
		}
		fmt.Fprintf(buf, "%s ::=\n", prod.Name)
		for i, items := range p {
			buf.WriteString("\t")
			if i > 0 {
				buf.WriteString("| ")
			}
			for j, item := range items {
				if j > 0 {
					buf.WriteString(" ")
				}
				buf.WriteString(item.Value)
			}
			buf.WriteString("\n")
		}
	}
	return buf.Bytes(), nil
}

func isUpper(s string) bool {
	return s == strings.ToUpper(s)
}

// ParseGrammar parses the grammar from b.
func ParseGrammar(r io.Reader) (Grammar, error) {
	g := make(Grammar)

	var name string
	var prods productions
	scan := bufio.NewScanner(r)
	i := 0
	for scan.Scan() {
		s := scan.Text()
		i++
		f := strings.Fields(s)
		if len(f) == 0 {
			if len(prods) > 0 {
				g[name] = prods
			}
			continue
		}
		if !unicode.IsSpace(rune(s[0])) {
			if len(f) != 2 {
				return nil, fmt.Errorf("bad line: %v: %s", i, s)
			}
			name = f[0]
			prods = nil
			continue
		}
		if f[0] == "|" {
			f = f[1:]
		}
		var seq sequence
		for _, v := range f {
			if reIsIdent.MatchString(v) {
				seq = append(seq, literal(v))
			} else if reIsExpr.MatchString(v) {
				seq = append(seq, token(v))
			} else if strings.HasPrefix(v, `'`) && strings.HasSuffix(v, `'`) {
				seq = append(seq, literal(v[1:len(v)-1]))
			} else if strings.HasPrefix(v, `/*`) && strings.HasSuffix(v, `*/`) {
				seq = append(seq, comment(v))
			} else {
				panic(v)
			}
		}
		prods = append(prods, seq)
	}
	if err := scan.Err(); err != nil {
		return nil, err
	}
	if len(prods) > 0 {
		g[name] = prods
	}
	g.simplify()
	return g, nil
}

// Grammar represents a parsed grammar.
type Grammar map[string]productions

// ExtractProduction extracts the named statement and all its dependencies,
// in order, into a BNF file. If descend is false, only the named statement
// is extracted.
func (g Grammar) ExtractProduction(
	name string, descend, nosplit bool, match, exclude []*regexp.Regexp,
) ([]byte, error) {
	names := []token{token(name)}
	b := new(bytes.Buffer)
	done := map[token]bool{token(name): true}
	for i := 0; i < len(names); i++ {
		if i > 0 {
			b.WriteString("\n")
		}
		n := names[i]
		prods := g[string(n)]
		if len(prods) == 0 {
			return nil, fmt.Errorf("couldn't find %s", n)
		}
		walkToken(prods, func(t token) {
			if !done[t] && descend {
				names = append(names, t)
				done[t] = true
			}
		})
		fmt.Fprintf(b, "%s ::=\n", n)
		b.WriteString(prods.Match(nosplit, match, exclude))
	}
	return b.Bytes(), nil
}

// Inline inlines names.
func (g Grammar) Inline(names ...string) error {
	for _, name := range names {
		p, ok := g[name]
		if !ok {
			return fmt.Errorf("unknown name: %s", name)
		}
		grp := group(p)
		for _, prods := range g {
			replaceToken(prods, func(t token) expression {
				if string(t) == name {
					return grp
				}
				return nil
			})
		}
	}
	return nil
}

func (g Grammar) simplify() {
	for name, prods := range g {
		p := simplify(name, prods)
		if p != nil {
			g[name] = p
		}
	}
}

func simplify(name string, prods productions) productions {
	funcs := []func(string, productions) productions{
		simplifySelfRefList,
	}
	for _, f := range funcs {
		if e := f(name, prods); e != nil {
			return e
		}
	}
	return nil
}

func simplifySelfRefList(name string, prods productions) productions {
	// First check we have sequences everywhere, and that the production
	// is a prefix of at least one of them.
	// Split the sequences in leaf and recursive groups:
	// X := A | B | X C | X D
	// group 1: A | B
	// group 2: C | D
	// Final: (A | B) (C | D)*
	var group1, group2 group
	for _, p := range prods {
		s, ok := p.(sequence)
		if !ok {
			return nil
		}
		if len(s) > 0 && s[0] == token(name) {
			group2 = append(group2, s[1:])
		} else {
			group1 = append(group1, s)
		}
	}
	if len(group2) == 0 {
		// Not a recursive rule; do nothing.
		return nil
	}
	return productions{
		sequence{group1, repeat{group2}},
	}
}

func replaceToken(p productions, f func(token) expression) {
	replacetoken(p, f)
}

func replacetoken(e expression, f func(token) expression) expression {
	switch e := e.(type) {
	case sequence:
		for i, v := range e {
			n := replacetoken(v, f)
			if n != nil {
				e[i] = n
			}
		}
	case token:
		return f(e)
	case group:
		for i, v := range e {
			n := replacetoken(v, f)
			if n != nil {
				e[i] = n
			}
		}
	case productions:
		for i, v := range e {
			n := replacetoken(v, f)
			if n != nil {
				e[i] = n
			}
		}
	case repeat:
		return replacetoken(e.expression, f)
	case literal, comment:
		// ignore
	default:
		panic(fmt.Errorf("unknown type: %T", e))
	}
	return nil
}

func walkToken(e expression, f func(token)) {
	switch e := e.(type) {
	case sequence:
		for _, v := range e {
			walkToken(v, f)
		}
	case token:
		f(e)
	case group:
		for _, v := range e {
			walkToken(v, f)
		}
	case repeat:
		walkToken(e.expression, f)
	case productions:
		for _, v := range e {
			walkToken(v, f)
		}
	case literal, comment:
		// ignore
	default:
		panic(fmt.Errorf("unknown type: %T", e))
	}
}

type productions []expression

func (p productions) Match(nosplit bool, match, exclude []*regexp.Regexp) string {
	b := new(bytes.Buffer)
	first := true
	for _, e := range p {
		if nosplit {
			b.WriteString("\t")
			if !first {
				b.WriteString("| ")
			} else {
				first = false
			}
			b.WriteString(e.String())
			b.WriteString("\n")
			continue
		}
	Loop:
		for _, s := range split(e) {
			for _, ex := range exclude {
				if ex.MatchString(s) {
					continue Loop
				}
			}
			for _, ma := range match {
				if !ma.MatchString(s) {
					continue Loop
				}
			}
			b.WriteString("\t")
			if !first {
				b.WriteString("| ")
			} else {
				first = false
			}
			b.WriteString(s)
			b.WriteString("\n")
		}
	}
	return b.String()
}

func (p productions) String() string {
	b := new(bytes.Buffer)
	for i, e := range p {
		b.WriteString("\t")
		if i > 0 {
			b.WriteString("| ")
		}
		b.WriteString(e.String())
		b.WriteString("\n")
	}
	return b.String()
}

type expression interface {
	String() string
}

type sequence []expression

func (s sequence) String() string {
	b := new(bytes.Buffer)
	for i, e := range s {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(e.String())
	}
	return b.String()
}

type token string

func (t token) String() string {
	return string(t)
}

type literal string

func (l literal) String() string {
	return fmt.Sprintf("'%s'", string(l))
}

type group []expression

func (g group) String() string {
	b := new(bytes.Buffer)
	b.WriteString("( ")
	for i, e := range g {
		if i > 0 {
			b.WriteString(" | ")
		}
		b.WriteString(e.String())
	}
	b.WriteString(" )")
	return b.String()
}

type repeat struct {
	expression
}

func (r repeat) String() string {
	return fmt.Sprintf("( %s )*", r.expression)
}

type comment string

func (c comment) String() string {
	return string(c)
}

func split(e expression) []string {
	appendRet := func(cur, add []string) []string {
		if len(cur) == 0 {
			if len(add) == 0 {
				return []string{""}
			}
			return add
		}
		var next []string
		for _, r := range cur {
			for _, s := range add {
				next = append(next, r+" "+s)
			}
		}
		return next
	}
	var ret []string
	switch e := e.(type) {
	case sequence:
		for _, v := range e {
			ret = appendRet(ret, split(v))
		}
	case group:
		var next []string
		for _, v := range e {
			next = append(next, appendRet(ret, split(v))...)
		}
		ret = next
	case literal, comment, repeat, token:
		ret = append(ret, e.String())
	default:
		panic(fmt.Errorf("unknown type: %T", e))
	}
	return ret
}
