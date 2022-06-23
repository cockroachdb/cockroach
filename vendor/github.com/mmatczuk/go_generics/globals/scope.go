// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package globals

import (
	"go/token"
)

// SymKind specifies the kind of a global symbol. For example, a variable, const
// function, etc.
type SymKind int

// Constants for different kinds of symbols.
const (
	KindUnknown SymKind = iota
	KindImport
	KindType
	KindVar
	KindConst
	KindFunction
	KindReceiver
	KindParameter
	KindResult
	KindTag
)

type symbol struct {
	kind  SymKind
	pos   token.Pos
	scope *scope
}

type scope struct {
	outer *scope
	syms  map[string]*symbol
}

func newScope(outer *scope) *scope {
	return &scope{
		outer: outer,
		syms:  make(map[string]*symbol),
	}
}

func (s *scope) isGlobal() bool {
	return s.outer == nil
}

func (s *scope) lookup(n string) *symbol {
	return s.syms[n]
}

func (s *scope) deepLookup(n string) *symbol {
	for x := s; x != nil; x = x.outer {
		if sym := x.lookup(n); sym != nil {
			return sym
		}
	}
	return nil
}

func (s *scope) add(name string, kind SymKind, pos token.Pos) {
	s.syms[name] = &symbol{
		kind:  kind,
		pos:   pos,
		scope: s,
	}
}
