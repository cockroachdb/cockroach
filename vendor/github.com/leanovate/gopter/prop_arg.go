package gopter

import (
	"fmt"
	"strings"
)

// PropArg contains information about the specific values for a certain property check.
// This is mostly used for reporting when a property has falsified.
type PropArg struct {
	Arg     interface{}
	OrigArg interface{}
	Label   string
	Shrinks int
}

func (p *PropArg) String() string {
	return fmt.Sprintf("%v", p.Arg)
}

// PropArgs is a list of PropArg.
type PropArgs []*PropArg

// NewPropArg creates a new PropArg.
func NewPropArg(genResult *GenResult, shrinks int, value, origValue interface{}) *PropArg {
	return &PropArg{
		Label:   strings.Join(genResult.Labels, ", "),
		Arg:     value,
		OrigArg: origValue,
		Shrinks: shrinks,
	}
}
