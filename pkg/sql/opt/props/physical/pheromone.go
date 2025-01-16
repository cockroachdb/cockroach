// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package physical

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type Pheromone struct {
	op        opt.Operator
	fields    json.JSON
	children  [3]*Pheromone // is 3 right?
	alternate *Pheromone    // note: using a slice instead would simplify many things
	reference pheromoneBinding
}

type pheromoneBinding struct {
	id  string
	ptr *Pheromone
}

// NonePheromone is a sentinel that represents [], which matches nothing. It
// always needs to be the final pheromone in the alternates list. The use of
// this sentinel allows a nil pointer to represent {}, a.k.a. Any, which matches
// everything.
//
// If we're looking inside NonePheromone we've made a mistake somewhere, so it
// doesn't have any fields set to avoid infinite recursion.
var NonePheromone = Pheromone{}

func (p *Pheromone) Any() bool {
	return p == nil
}

func PheromoneFromJSON(j json.JSON) (*Pheromone, error) {
	if json.IsEmptyObject(j) {
		return nil, nil
	}

	typeCheck := func(j json.JSON, path string, types ...json.Type) error {
		for _, typ := range types {
			if j.Type() == typ {
				return nil
			}
		}
		return errors.Newf(
			"unexpected %s at %s, should be one of %v: %v", j.Type().String(), path, types, j,
		)
	}

	getStringField := func(j json.JSON, path string) (string, error) {
		if err := typeCheck(j, path, json.StringJSONType); err != nil {
			return "", err
		}
		stringP, err := j.AsText()
		if err != nil {
			return "", err
		}
		if stringP == nil {
			return "", errors.Newf("unexpected null at %s, should be string", path)
		}
		if *stringP == "" {
			return "", errors.Newf("unexpected empty string at %s", path)
		}
		return *stringP, nil
	}

	var fromJSON func(json.JSON, string, []map[string][]*pheromoneBinding) (*Pheromone, error)
	fromJSON = func(
		j json.JSON, path string, environment []map[string][]*pheromoneBinding,
	) (*Pheromone, error) {
		if err := typeCheck(
			j, path, json.StringJSONType, json.ArrayJSONType, json.ObjectJSONType,
		); err != nil {
			return nil, err
		}

		// [<alt1>, alternates...] declares alternates for this pattern
		if a, ok := j.AsArray(); ok {
			p := &NonePheromone
			for i := len(a) - 1; i >= 0; i-- {
				alt, err := fromJSON(a[i], fmt.Sprintf("%s[%d]", path, i), environment)
				if err != nil {
					return nil, err
				}
				if alt == &NonePheromone {
					continue
				}
				// if we had a nested array, walk to the end of the inner alternation to
				// splice it into this alternation
				last := alt
				for last.alternate != &NonePheromone {
					last = last.alternate
				}
				last.alternate = p
				p = alt
			}
			return p, nil
		}

		// {"_in": <body>, let...} binds names to patterns within the scope of body
		if inner, err := j.FetchValKey("_in"); inner != nil || err != nil {
			if err != nil {
				return nil, err
			}

			// first pass: make a new environment frame
			references := make(map[string][]*pheromoneBinding, max(0, j.Len()-1))
			if it, err := j.ObjectIter(); it != nil || err != nil {
				if err != nil {
					return nil, err
				}
				for it.Next() {
					switch it.Key() {
					case "_in":
						// handled above
					case "":
						return nil, errors.Newf("unexpected empty binding name at %s", path)
					default:
						references[it.Key()] = nil
					}
				}
			}
			environment = append(environment, references)

			// second pass: build the bindings and collect references
			bindings := make([]pheromoneBinding, 0, max(0, j.Len()-1))
			if it, err := j.ObjectIter(); it != nil || err != nil {
				if err != nil {
					return nil, err
				}
				for it.Next() {
					switch it.Key() {
					case "_in":
						// handled below
					default:
						let, err := fromJSON(it.Value(), fmt.Sprintf("%s.%s", path, it.Key()), environment)
						if err != nil {
							return nil, err
						}
						bindings = append(bindings, pheromoneBinding{id: it.Key(), ptr: let})
					}
				}
			}

			body, err := fromJSON(inner, path+"._in", environment)
			if err != nil {
				return nil, err
			}

			// finally, fill in all the references
			for _, binding := range bindings {
				refs, ok := references[binding.id]
				if !ok || len(refs) == 0 {
					return nil, errors.Newf("unused binding at %s: %s", path, binding.id)
				}
				for _, r := range refs {
					r.ptr = binding.ptr
				}
			}

			return body, nil
		}

		// {"_op": <operator>, fields...} is a pattern matching an operator
		if it, err := j.ObjectIter(); it != nil || err != nil {
			if err != nil {
				return nil, err
			}

			pattern := &Pheromone{
				alternate: &NonePheromone,
			}

			// pull out _op first, if it exists
			if opVal, err := j.FetchValKey("_op"); opVal != nil || err != nil {
				if err != nil {
					return nil, err
				}
				path = path + "._op"
				opName, err := getStringField(opVal, path)
				if err != nil {
					return nil, err
				}
				op, err := opt.OperatorFromString(opName)
				if err != nil {
					return nil, errors.Wrapf(
						err, "unknown operator at %s: %v", path, opName,
					)
				}
				pattern.op = op
			}

			fields := json.NewObjectBuilder(max(0, j.Len()-1))
			for it.Next() {
				path = fmt.Sprintf("%s.%s", path, it.Key())
				switch it.Key() {
				case "_op":
					// handled above
				case "table", "index":
					val, err := getStringField(it.Value(), path)
					if err != nil {
						return nil, err
					}
					fields.Add(it.Key(), json.FromString(val))
				case "constrained":
					if err := typeCheck(
						it.Value(), path, json.FalseJSONType, json.TrueJSONType,
					); err != nil {
						return nil, err
					}
					fields.Add(it.Key(), it.Value())
				case "input", "left":
					child, err := fromJSON(it.Value(), path, environment)
					if err != nil {
						return nil, err
					}
					pattern.children[0] = child
				case "right":
					child, err := fromJSON(it.Value(), path, environment)
					if err != nil {
						return nil, err
					}
					pattern.children[1] = child
				case "on":
					child, err := fromJSON(it.Value(), path, environment)
					if err != nil {
						return nil, err
					}
					pattern.children[2] = child
				default:
					return nil, errors.Newf("unknown field at %s: %v", path, it.Key())
				}
			}
			pattern.fields = fields.Build()
			return pattern, nil
		}

		// "id" is a reference to a pattern bound to "id" within this scope
		id, err := getStringField(j, path)
		if err != nil {
			return nil, err
		}
		for i := len(environment) - 1; i >= 0; i-- {
			if references, ok := environment[i][id]; ok {
				p := &Pheromone{
					alternate: &NonePheromone,
					reference: pheromoneBinding{
						id: id,
					},
				}
				environment[i][id] = append(references, &p.reference)
				return p, nil
			}
		}
		return nil, errors.Newf("unknown reference at %s: %v", path, id)
	}

	return fromJSON(j, "$", nil)
}

func PheromoneFromString(s string) (*Pheromone, error) {
	if s == "" {
		return nil, nil
	}
	j, err := json.ParseJSON(s)
	if err != nil {
		return nil, err
	}
	return PheromoneFromJSON(j)
}

func (p *Pheromone) ToJSON() json.JSON {
	if p.Any() {
		return json.EmptyJSONObject
	}

	type bindingScope struct {
		p *Pheromone
		a bool
	}

	youngestCommonAncestor := func(a, b []bindingScope) []bindingScope {
		var i int
		for i < len(a) && i < len(b) && a[i] == b[i] {
			i++
		}
		return a[:i]
	}

	// first pass: for each referenced pattern in the main tree, find the youngest
	// common ancestor of all references, which is the minimal binding scope. this
	// is where we will place the binding
	bindingScopes := make(map[pheromoneBinding][]bindingScope)
	var bindingScopesChanged bool

	var walk func(*Pheromone, []bindingScope)
	var walkAlternate func(*Pheromone, []bindingScope)

	walk = func(p *Pheromone, scopes []bindingScope) {
		if p == nil || p == &NonePheromone {
			return
		}
		if p.alternate != &NonePheromone {
			scopes = append(scopes, bindingScope{p: p, a: true})
			for p != &NonePheromone {
				walkAlternate(p, scopes)
				p = p.alternate
			}
			return
		}
		walkAlternate(p, scopes)
	}

	walkAlternate = func(p *Pheromone, scopes []bindingScope) {
		scopes = append(scopes, bindingScope{p: p, a: false})
		if p.reference.id != "" {
			if a, ok := bindingScopes[p.reference]; ok {
				bindingScopes[p.reference] = youngestCommonAncestor(a, scopes)
				if len(bindingScopes[p.reference]) < len(a) {
					bindingScopesChanged = true
				}
			} else {
				bindingScopes[p.reference] = append([]bindingScope(nil), scopes...)
				bindingScopesChanged = true
			}
			return
		}
		walk(p.children[0], scopes)
		walk(p.children[1], scopes)
		walk(p.children[2], scopes)
	}

	walk(p, nil)

	// second - nth passes: walk each binding, which potentially will update the
	// set of binding scopes, until the binding scopes stop changing
	for bindingScopesChanged {
		// not sure we actually need to make this copy
		bindingScopesCopy := make(map[pheromoneBinding][]bindingScope, len(bindingScopes))
		for binding, scopes := range bindingScopes {
			// scopes slice can only shrink, so shallow copy is fine
			bindingScopesCopy[binding] = scopes
		}
		bindingScopesChanged = false

		for binding, scopes := range bindingScopesCopy {
			walk(binding.ptr, append([]bindingScope(nil), scopes...))
		}
	}

	// invert bindingScopes to get the bindings
	bindingsByScope := make(map[bindingScope][]pheromoneBinding)
	for binding, scopes := range bindingScopes {
		scope := scopes[len(scopes)-1]
		bindingsByScope[scope] = append(bindingsByScope[scope], binding)
	}

	// final pass: convert to JSON now that we know where to place the bindings
	var bindingsToJSON func([]pheromoneBinding, json.JSON) json.JSON
	var toJSON func(*Pheromone) json.JSON
	var alternateToJSON func(*Pheromone) json.JSON

	toJSON = func(p *Pheromone) (j json.JSON) {
		if bindings, ok := bindingsByScope[bindingScope{p: p, a: true}]; ok {
			defer func() {
				j = bindingsToJSON(bindings, j)
			}()
		}
		if p == &NonePheromone {
			return json.EmptyJSONArray
		}
		if p.alternate != &NonePheromone {
			b := json.NewArrayBuilder(2)
			for p != &NonePheromone {
				b.Add(alternateToJSON(p))
				p = p.alternate
			}
			return b.Build()
		}
		return alternateToJSON(p)
	}

	alternateToJSON = func(p *Pheromone) (j json.JSON) {
		if bindings, ok := bindingsByScope[bindingScope{p: p, a: false}]; ok {
			defer func() {
				j = bindingsToJSON(bindings, j)
			}()
		}
		if p.reference.id != "" {
			return json.FromString(p.reference.id)
		}
		b := json.NewObjectBuilder(1 + p.fields.Len() + 3)
		if p.op != opt.UnknownOp {
			b.Add("_op", json.FromString(p.op.String()))
		}
		if it, _ := p.fields.ObjectIter(); it != nil {
			for it.Next() {
				b.Add(it.Key(), it.Value())
			}
		}
		if p.children[0] != nil {
			b.Add("left", toJSON(p.children[0]))
		}
		if p.children[1] != nil {
			b.Add("right", toJSON(p.children[1]))
		}
		if p.children[2] != nil {
			b.Add("on", toJSON(p.children[2]))
		}
		return b.Build()
	}

	bindingsToJSON = func(bindings []pheromoneBinding, inner json.JSON) json.JSON {
		b := json.NewObjectBuilder(1 + len(bindings))
		b.Add("_in", inner)
		for _, binding := range bindings {
			b.Add(binding.id, toJSON(binding.ptr))
		}
		return b.Build()
	}

	return toJSON(p)
}

func (p *Pheromone) String() string {
	if p.Any() {
		return ""
	}
	return p.ToJSON().String()
}

func (p *Pheromone) Format(buf *bytes.Buffer) {
	if p.Any() {
		return
	}
	p.ToJSON().Format(buf)
}

func (p *Pheromone) Equals(rhs *Pheromone) bool {
	// this is a hack
	return p.String() == rhs.String()
}

func (p *Pheromone) HasAlternates() bool {
	if p == nil || p == &NonePheromone {
		return false
	}
	if p.alternate != &NonePheromone {
		return true
	}
	if p.reference.id != "" {
		return p.reference.ptr.HasAlternates()
	}
	return false
}

func (p *Pheromone) GetMatchingAlternates(op opt.Operator) (alternates []*Pheromone) {
	if p == nil {
		return []*Pheromone{nil}
	}
	if p == &NonePheromone {
		return nil
	}
	for alt := p; alt != &NonePheromone; alt = alt.alternate {
		if alt.reference.id != "" {
			alternates = append(alternates, alt.reference.ptr.GetMatchingAlternates(op)...)
		} else if alt.Matches(op) {
			altCopy := new(Pheromone)
			*altCopy = *alt
			altCopy.alternate = &NonePheromone
			alternates = append(alternates, altCopy)
		}
	}
	return alternates
}

func (p *Pheromone) Matches(op opt.Operator) bool {
	if p.Any() {
		return true
	}
	if p == &NonePheromone {
		return false
	}
	if p.reference.id != "" {
		return p.reference.ptr.Matches(op)
	}
	if p.op != opt.UnknownOp && op != opt.UnknownOp && p.op != op {
		return false
	}

	/* TODO
	switch t := expr.(type) {
	case *memo.ScanExpr:
		if table, err := p.fields.FetchValKey("table"); table != nil || err != nil {
			if err != nil {
				panic(errors.AssertionFailedf("unexpected %v", err))
			}
			if table != t.
		}
	}
	*/
	return true
}

func (p *Pheromone) Child(nth int) *Pheromone {
	if p.Any() {
		return nil
	}
	if p == &NonePheromone {
		return &NonePheromone
	}
	if p.reference.id != "" {
		return p.reference.ptr.Child(nth)
	}
	return p.children[nth]
}

/*

type Pheromone struct {
	op        opt.Operator
	fields    json.JSON
	input     [3]*Pheromone
	alternate *Pheromone
	softlink  *Pheromone
}

// NonePheromone is a sentinel that represents [], which matches nothing. It
// always needs to be the final pheromone in the alternates list. The use of
// this sentinel allows a nil pointer to represent {}, a.k.a. Any, which matches
// everything.
var NonePheromone = Pheromone{}

func (p *Pheromone) Any() bool {
	return p == nil
}

func PheromoneFromJSON(j json.JSON) (*Pheromone, error) {
	if json.IsEmptyObject(j) {
		return nil, nil
	}

	typeCheck := func(j json.JSON, pth string, typs ...json.Type) error {
		for _, typ := range typs {
			if j.Type() == typ {
				return nil
			}
		}
		return errors.Newf(
			"unexpected %s at %s, should be one of %v: %v", j.Type().String(), pth, typs, j,
		)
	}

	getStringField := func(j json.JSON, pth string) (string, error) {
		if err := typeCheck(j, pth, json.StringJSONType); err != nil {
			return "", err
		}
		stringP, err := j.AsText()
		if err != nil {
			return "", err
		}
		if stringP == nil {
			return "", errors.Newf("unexpected null at %s, should be string", pth)
		}
		return *stringP, nil
	}

	// 1. build the Pheromone tree without cycles, gathering references

	byPath := make(map[string]*Pheromone)
	references := make(map[string]string)

	var buildPheromone func(json.JSON, string) (*Pheromone, error)

	buildPheromoneAlternation := func(j json.JSON, pth string) (*Pheromone, error) {
		if a, ok := j.AsArray(); ok {
			p := &NonePheromone
			for i := len(a) - 1; i >= 0; i-- {
				valPth := path.Join(pth, strconv.Itoa(i))
				alt, err := buildPheromone(a[i], valPth)
				if err != nil {
					return nil, err
				}
				if alt.alternate != &NonePheromone {
					return nil, errors.AssertionFailedf("unexpected alternation at %s: %v", valPth, a[i])
				}
				alt.alternate = p
				p = alt
			}
			byPath[pth] = p
			return p, nil
		}
		return buildPheromone(j, pth)
	}

	buildPheromone = func(j json.JSON, pth string) (*Pheromone, error) {
		if err := typeCheck(j, pth, json.StringJSONType, json.ObjectJSONType); err != nil {
			return nil, err
		}

		p := &Pheromone{
			alternate: &NonePheromone,
		}

		if it, err := j.ObjectIter(); it != nil || err != nil {
			if err != nil {
				return nil, err
			}
			job := json.NewObjectBuilder(j.Len() - 1)
			for it.Next() {
				valPth := path.Join(pth, it.Key())
				switch it.Key() {
				case "_op":
					opName, err := getStringField(it.Value(), valPth)
					if err != nil {
						return nil, err
					}
					op, err := opt.OperatorFromString(opName)
					if err != nil {
						return nil, errors.Wrapf(
							err, "unknown operator at %s: %v", valPth, it.Value(),
						)
					}
					p.op = op
				case "table", "index":
					val, err := getStringField(it.Value(), valPth)
					if err != nil {
						return nil, err
					}
					job.Add(it.Key(), json.FromString(val))
				case "constrained":
					if err := typeCheck(
						it.Value(), valPth, json.FalseJSONType, json.TrueJSONType,
					); err != nil {
						return nil, err
					}
					job.Add(it.Key(), it.Value())
				case "input", "left":
					input, err := buildPheromoneAlternation(it.Value(), valPth)
					if err != nil {
						return nil, err
					}
					p.input[0] = input
				case "right":
					input, err := buildPheromoneAlternation(it.Value(), valPth)
					if err != nil {
						return nil, err
					}
					p.input[1] = input
				case "on":
					input, err := buildPheromoneAlternation(it.Value(), valPth)
					if err != nil {
						return nil, err
					}
					p.input[2] = input
				default:
					return nil, errors.Newf("unknown field at %s: %v", valPth, it.Key())
				}
			}
			p.fields = job.Build()
		} else {
			reference, err := getStringField(j, pth)
			if err != nil {
				return nil, err
			}
			// TODO: translate relative references to absolute references
			references[pth] = reference
		}
		byPath[pth] = p
		return p, nil
	}

	if err := typeCheck(j, "/", json.ArrayJSONType, json.ObjectJSONType); err != nil {
		return nil, err
	}
	p, err := buildPheromoneAlternation(j, "/")
	if err != nil {
		return nil, err
	}

	// 2. fix references to point to other nodes
	for pth, ref := range references {
		// check for reference cycles
		for next, ok := ref, true; ok; next, ok = references[next] {
			if next == pth {
				return nil, errors.Newf("reference cycle at %s", pth)
			}
		}
		// point the softlink at the proper node
		// there's a bug here if the softlink points to an alternate
		target, ok := byPath[ref]
		if !ok {
			return nil, errors.Newf("invalid reference at %s: %s", pth, ref)
		}
		byPath[pth].softlink = target
	}

	return p, nil
}

func PheromoneFromString(s string) (*Pheromone, error) {
	if s == "" {
		return nil, nil
	}
	j, err := json.ParseJSON(s)
	if err != nil {
		return nil, err
	}
	return PheromoneFromJSON(j)
}

func (p *Pheromone) ToJSON() json.JSON {
	if p.Any() {
		return json.EmptyJSONObject
	}
	// 1. walk the tree, figuring out the path of each node
	// 2. walk the tree again, building the JSON
	paths := make(map[*Pheromone]string)
	var visit func(*Pheromone, string)
	visit = func(p *Pheromone, pth string) {
		if p == nil {
			return
		}
		paths[p] = pth

	}
}

func (p *Pheromone) String() string {
	if p.Any() {
		return ""
	}
	return p.ToJSON().String()
}

func (p *Pheromone) Format(buf *bytes.Buffer) {
	if p.Any() {
		return
	}
	p.ToJSON().Format(buf)
}

func (p *Pheromone) Equals(rhs *Pheromone) bool {
}

func (p *Pheromone) Matches(expr opt.Expr) bool {
}

func (p *Pheromone) Child(nth int) *Pheromone {
}

func PheromoneFromJSON(j json.JSON) (Pheromone, error) {

	typeCheck := func(j json.JSON, pth []string, typs ...json.Type) error {
		for _, typ := range typs {
			if j.Type() == typ {
				return nil
			}
		}
		return errors.Newf("wrong type at /%s: %v", path.Join(pth...), j)
	}

	// 1. build the Pheromone structure without cycles, gathering references
	references := make(map[string]Pheromone)

	var buildPheromoneExpr func(json.JSON, []string) (pheromoneExpr, error)

	buildPheromone := func(j json.JSON, pth []string) (Pheromone, error) {
		if a, ok := j.AsArray(); ok {
			p := make([]pheromoneExpr, len(a))
			for i := range a {
				expr, err := buildPheromoneExpr(a[i], append(pth, strconv.Itoa(i)))
				if err != nil {
					return nil, err
				}
				p[i] = expr
			}
			return p, nil
		}
		expr, err := buildPheromoneExpr(j, pth)
		if err != nil {
			return nil, err
		}
		return Pheromone{expr}, nil
	}

	buildPheromoneExpr = func(j json.JSON, pth []string) (pheromoneExpr, error) {
		if err := typeCheck(j, pth, json.StringJSONType, json.ObjectJSONType); err != nil {
			return pheromoneExpr{}, err
		}

		if it, _ := j.ObjectIter(); it != nil {
			var expr pheromoneExpr
			b := json.NewObjectBuilder(j.Len())
			var allowedTypes intsets.Fast
			for it.Next() {
				valPth := append(pth, it.Key())
				switch it.Key() {
				case "_op":
					if err := typeCheck(it.Value(), valPth, json.StringJSONType); err != nil {
						return pheromoneExpr{}, err
					}
					opName, err := it.Value().AsText()
					if err != nil {
						return pheromoneExpr{}, err
					}
					if opName == nil {
						return pheromoneExpr{}, errors.Newf("null operator at /%s", path.Join(valPth...))
					}
					expr.op, err = opt.OperatorFromString(*opName)
					if err != nil {
						return pheromoneExpr{}, errors.Wrapf(
							err, "incorrect operator at /%s: %v", path.Join(valPth...), it.Value(),
						)
					}
				case "table", "index":
					if err := typeCheck(it.Value(), valPth, json.StringJSONType); err != nil {
						return pheromoneExpr{}, err
					}
					// should we look up the table? the index?
					b.Add(it.Key(), it.Value())
				case "constrained":
					if err := typeCheck(it.Value(), valPth, json.FalseJSONType, json.TrueJSONType); err != nil {
						return pheromoneExpr{}, err
					}
					// should we look up the table? the index?
					b.Add(it.Key(), it.Value())
				case "input", "left":
					p, err := buildPheromone(it.Value(), valPth)
					if err != nil {
						return pheromoneExpr{}, err
					}
					expr.input[0] = p
				case "right":
					p, err := buildPheromone(it.Value(), valPth)
					if err != nil {
						return pheromoneExpr{}, err
					}
					expr.input[1] = p
				default:
					return pheromoneExpr{}, errors.Newf(
						"incorrect field at /%s: %v", path.Join(valPth...), it.Key(),
					)
				}
			}
			expr.pattern = b.Build()
			return expr, nil
		}

		// add the reference to the map
		// we ultimately want to replace the whole Pheromone with the one we're referencing

		return nil, nil
	}

	// 2. fix references to point to other nodes
	// we want each reference to point to a pheromone
	//
	var resolve func() (Pheromone, error)
}

// maybe I should define a struct for each op type and do it that way

type Pheromones []Pheromone

type Pheromone struct {
	keys []string
	vals []Pheromones
}

// hmmm. this didn't work very well: json.JSON really doesn't want to be
// modified after creation. we need our own type

type Pheromone struct {
	// pattern is not a normal JSON: it can contain cycles
	pattern json.JSON
}

var AnyPheromone Pheromone

func (p Pheromone) Any() bool {
	return p.pattern == nil
}

func PheromoneFromJSON(j json.JSON) (Pheromone, error) {
	allowedTypes := func(key string) intsets.Fast {
	}

	makeTypeError := func(j json.JSON, pth []string, allowedType intsets.Fast) error {
		return errors.Newf("wrong type at %s: %v", path.Join("/", pth...), j)
	}

	refs := make(map[string]*json.JSON)
	refsAllowedType := make(map[string]intsets.Fast)

	var buildJSON func(json.JSON, []string, bool) (json.JSON, error)
	buildJSON = func(j json.JSON, pth []string, allowedType intsets.Fast) (json.JSON, error) {
		if !allowedType.Contains(j.Type()) {
			return nil, makeTypeError(j, pth, allowedType)
		}
		if a, ok := j.AsArray(); ok {
			b := json.NewArrayBuilder(len(a))
			// Disallow redundant alternation.
			childAllowedType := allowedType.Copy()
			childAllowedType.Remove(json.ArrayJSONType)
			for i := range a {
				c, err := buildJSON(a[i], append(pth, strconv.Itoa(i)), childAllowedType)
				if err != nil {
					return nil, err
				}
				b.Add(c)
			}
			return b.Build(), nil
		}
		if it, _ := j.ObjectIter(); it != nil {
			b := json.NewObjectBuilder(j.Len())
			for it.Next() {
				c, err := buildJSON(it.Value(), append(pth, it.Key()), allowedTypes(it.Key()))
				if err != nil {
					return nil, err
				}
				b.Add(it.Key(), c)
			}
			return b.Build(), nil
		}

	}
}

func PheromoneFromString(s string) (Pheromone, error) {
	if s == "" {
		return AnyPheromone, nil
	}
	j, err := json.ParseJSON(s)
	if err != nil {
		return AnyPheromone, err
	}
	return PheromoneFromJSON(j)
}

func (p Pheromone) ToJSON() json.JSON {
	if p.Any() {
		return json.EmptyJSONObject
	}
	// Use a map to detect cycles and replace them with path references.
	pths := make(map[json.JSON]string)
	var toJSON func(json.JSON, string) json.JSON
	toJSON = func(p json.JSON, pth string) json.JSON {
		if absPth, ok := pths[p]; ok {
			// TODO: use relative path if shorter
			return json.FromString(absPth)
		}
		pths[p] = pth
		if a, ok := p.AsArray(); ok {
			b := json.NewArrayBuilder(len(a))
			for i := range a {
				b.Add(toJSON(a[i], path.Join(pth, strconv.Itoa(i))))
			}
			return b.Build()
		}
		if it, _ := p.ObjectIter(); it != nil {
			b := json.NewObjectBuilder(p.Len())
			for it.Next() {
				b.Add(it.Key(), toJSON(it.Value(), path.Join(pth, it.Key())))
			}
			return b.Build()
		}
		return p
	}
	return toJSON(p.pattern, "/")
}

func (p Pheromone) String() string {
	if p.Any() {
		return ""
	}
	return p.ToJSON().String()
}

func (p Pheromone) Format(buf *bytes.Buffer) {
	if p.Any() {
		return
	}
	p.ToJSON().Format(buf)
}

type Pheromone struct {
	Op       opt.Operator
	Children []Pheromone
}

func (p Pheromone) String() string {
	if p.Any() {
		return ""
	}
	return p.ToJSON().String()
}

func (p Pheromone) Format(buf *bytes.Buffer) {
	p.ToJSON().Format(buf)
}

func (p Pheromone) ToJSON() json.JSON {
	ob := json.NewObjectBuilder(2)
	if !p.Any() {
		ob.Add("op", json.FromString(p.Op.String()))
		if p.Children != nil {
			ab := json.NewArrayBuilder(len(p.Children))
			for i := range p.Children {
				ab.Add(p.Children[i].ToJSON())
			}
			ob.Add("input", ab.Build())
		}
	}
	return ob.Build()
}

func PheromoneFromJSON(j json.JSON) (Pheromone, error) {
	opJSON, err := j.FetchValKey("op")
	if err != nil {
		return Pheromone{}, err
	}
	if opJSON == nil {
		return Pheromone{}, errors.New("no op")
	}
	opName, err := opJSON.AsText()
	if err != nil {
		return Pheromone{}, err
	}
	if opName == nil {
		return Pheromone{}, errors.New("null op")
	}
	op, err := opt.OperatorFromString(*opName)
	if err != nil {
		return Pheromone{}, err
	}
	inputJSON, err := j.FetchValKey("input")
	if err != nil {
		return Pheromone{}, err
	}
	var children []Pheromone
	if inputJSON != nil {
		childrenJSON, arr := inputJSON.AsArray()
		if !arr {
			return Pheromone{}, errors.New("non-array input")
		}
		children = make([]Pheromone, len(childrenJSON))
		for i := range childrenJSON {
			child, err := PheromoneFromJSON(childrenJSON[i])
			if err != nil {
				return Pheromone{}, err
			}
			children[i] = child
		}
	}
	return Pheromone{
		Op:       op,
		Children: children,
	}, nil
}

func (p Pheromone) Any() bool {
	return p.Op == opt.UnknownOp && p.Children == nil
}

func (p Pheromone) Equals(rhs Pheromone) bool {
	if p.Op != rhs.Op {
		return false
	}
	if len(p.Children) != len(rhs.Children) {
		return false
	}
	for i := range p.Children {
		if !p.Children[i].Equals(rhs.Children[i]) {
			return false
		}
	}
	return true
}

func (p Pheromone) Child(nth int) Pheromone {
	if p.Children == nil {
		return Pheromone{}
	}
	fmt.Println("Child", nth, p)
	return p.Children[nth]
}
*/
