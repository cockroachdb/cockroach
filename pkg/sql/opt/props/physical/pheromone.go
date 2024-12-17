// Copyright 2024 The Cockroach Authors.
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

func PheromoneFromString(s string) (Pheromone, error) {
	if s == "" {
		return Pheromone{}, nil
	}
	j, err := json.ParseJSON(s)
	if err != nil {
		return Pheromone{}, err
	}
	return PheromoneFromJSON(j)
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
