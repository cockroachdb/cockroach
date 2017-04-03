// Copyright 2016 The Cockroach Authors.
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
//
// Author: Matt Jibson (mjibson@gmail.com)

package rsg

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/rsg/yacc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// RSG is a random syntax generator.
type RSG struct {
	lock  syncutil.Mutex
	src   *rand.Rand
	seen  map[string]bool
	prods map[string][]*yacc.ExpressionNode
}

// NewRSG creates a random syntax generator from the given random seed and
// yacc file.
func NewRSG(seed int64, y string) (*RSG, error) {
	tree, err := yacc.Parse("sql", y)
	if err != nil {
		return nil, err
	}
	rsg := RSG{
		src:   rand.New(rand.NewSource(seed)),
		seen:  make(map[string]bool),
		prods: make(map[string][]*yacc.ExpressionNode),
	}
	for _, prod := range tree.Productions {
		rsg.prods[prod.Name] = prod.Expressions
	}
	return &rsg, nil
}

// Generate generates a unique random syntax from the root node. At most depth
// levels of token expansion are performed. An empty string is returned on
// error or if depth is exceeded. Generate is safe to call from mulitipule
// goroutines. If Generate is called more times than it can generate unique
// output, it will block forever.
func (r *RSG) Generate(root string, depth int) string {
	for {
		s := strings.Join(r.generate(root, depth), " ")
		r.lock.Lock()
		if !r.seen[s] {
			r.seen[s] = true
		} else {
			s = ""
		}
		r.lock.Unlock()
		if s != "" {
			s = strings.Replace(s, "_LA", "", -1)
			s = strings.Replace(s, " AS OF SYSTEM TIME \"string\"", "", -1)
			return s
		}
	}
}

func (r *RSG) generate(root string, depth int) []string {
	var ret []string
	prods := r.prods[root]
	if len(prods) == 0 {
		return []string{root}
	}
	prod := prods[r.Intn(len(prods))]
	for _, item := range prod.Items {
		switch item.Typ {
		case yacc.TypLiteral:
			v := item.Value[1 : len(item.Value)-1]
			ret = append(ret, v)
		case yacc.TypToken:
			var v []string
			switch item.Value {
			case "IDENT":
				v = []string{"ident"}
			case "c_expr":
				v = r.generate(item.Value, 30)
			case "SCONST":
				v = []string{`'string'`}
			case "ICONST":
				v = []string{fmt.Sprint(r.Intn(1000) - 500)}
			case "FCONST":
				v = []string{fmt.Sprint(r.Float64())}
			case "BCONST":
				v = []string{`b'bytes'`}
			case "substr_from":
				v = []string{"FROM", `'string'`}
			case "substr_for":
				v = []string{"FOR", `'string'`}
			case "overlay_placing":
				v = []string{"PLACING", `'string'`}
			default:
				if depth == 0 {
					return nil
				}
				v = r.generate(item.Value, depth-1)
			}
			if v == nil {
				return nil
			}
			ret = append(ret, v...)
		default:
			panic("unknown item type")
		}
	}
	return ret
}

// Intn returns a random int.
func (r *RSG) Intn(n int) int {
	r.lock.Lock()
	v := r.src.Intn(n)
	r.lock.Unlock()
	return v
}

// Int63 returns a random int64.
func (r *RSG) Int63() int64 {
	r.lock.Lock()
	v := r.src.Int63()
	r.lock.Unlock()
	return v
}

// Float64 returns a random float. It is sometimes +/-Inf, NaN, and attempts to
// be distributed among very small, large, and normal scale numbers.
func (r *RSG) Float64() float64 {
	r.lock.Lock()
	v := r.src.Float64()*2 - 1
	switch r.src.Intn(10) {
	case 0:
		v = 0
	case 1:
		v = math.Inf(1)
	case 2:
		v = math.Inf(-1)
	case 3:
		v = math.NaN()
	case 4, 5:
		i := r.src.Intn(50)
		v *= math.Pow10(i)
	case 6, 7:
		i := r.src.Intn(50)
		v *= math.Pow10(-i)
	}
	r.lock.Unlock()
	return v
}

// GenerateRandomArg generates a random, valid, SQL function argument of
// the spcified type.
func (r *RSG) GenerateRandomArg(typ parser.Type) string {
	if r.Intn(10) == 0 {
		return "NULL"
	}
	var v interface{}
	switch parser.UnwrapType(typ) {
	case parser.TypeInt:
		i := r.Int63()
		i -= r.Int63()
		v = i
	case parser.TypeFloat, parser.TypeDecimal:
		v = r.Float64()
	case parser.TypeString:
		v = `'string'`
	case parser.TypeBytes:
		v = `b'bytes'`
	case parser.TypeTimestamp, parser.TypeTimestampTZ:
		t := time.Unix(0, r.Int63())
		v = fmt.Sprintf(`'%s'`, t.Format(time.RFC3339Nano))
	case parser.TypeBool:
		if r.Intn(2) == 0 {
			v = "false"
		} else {
			v = "true"
		}
	case parser.TypeDate:
		i := r.Int63()
		i -= r.Int63()
		d := parser.NewDDate(parser.DDate(i))
		v = fmt.Sprintf(`'%s'`, d)
	case parser.TypeInterval:
		d := duration.Duration{Nanos: r.Int63()}
		v = fmt.Sprintf(`'%s'`, &parser.DInterval{Duration: d})
	case parser.TypeIntArray,
		parser.TypeStringArray,
		parser.TypeOid,
		parser.TypeRegClass,
		parser.TypeRegNamespace,
		parser.TypeRegProc,
		parser.TypeRegProcedure,
		parser.TypeRegType,
		parser.TypeAnyArray,
		parser.TypeAny:
		v = "NULL"
	default:
		switch typ.(type) {
		case parser.TTuple:
			v = "NULL"
		default:
			panic(fmt.Errorf("unknown arg type: %s (%T)", typ, typ))
		}
	}
	return fmt.Sprintf("%v::%s", v, typ.String())
}
