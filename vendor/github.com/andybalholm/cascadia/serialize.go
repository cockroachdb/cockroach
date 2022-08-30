package cascadia

import (
	"fmt"
	"strings"
)

// implements the reverse operation Sel -> string

func (c tagSelector) String() string {
	return c.tag
}

func (c idSelector) String() string {
	return "#" + c.id
}

func (c classSelector) String() string {
	return "." + c.class
}

func (c attrSelector) String() string {
	val := c.val
	if c.operation == "#=" {
		val = c.regexp.String()
	} else if c.operation != "" {
		val = fmt.Sprintf(`"%s"`, val)
	}
	return fmt.Sprintf(`[%s%s%s]`, c.key, c.operation, val)
}

func (c relativePseudoClassSelector) String() string {
	return fmt.Sprintf(":%s(%s)", c.name, c.match.String())
}
func (c containsPseudoClassSelector) String() string {
	s := "contains"
	if c.own {
		s += "Own"
	}
	return fmt.Sprintf(`:%s("%s")`, s, c.value)
}
func (c regexpPseudoClassSelector) String() string {
	s := "matches"
	if c.own {
		s += "Own"
	}
	return fmt.Sprintf(":%s(%s)", s, c.regexp.String())
}
func (c nthPseudoClassSelector) String() string {
	if c.a == 0 && c.b == 1 { // special cases
		s := ":first-"
		if c.last {
			s = ":last-"
		}
		if c.ofType {
			s += "of-type"
		} else {
			s += "child"
		}
		return s
	}
	var name string
	switch [2]bool{c.last, c.ofType} {
	case [2]bool{true, true}:
		name = "nth-last-of-type"
	case [2]bool{true, false}:
		name = "nth-last-child"
	case [2]bool{false, true}:
		name = "nth-of-type"
	case [2]bool{false, false}:
		name = "nth-child"
	}
	return fmt.Sprintf(":%s(%dn+%d)", name, c.a, c.b)
}
func (c onlyChildPseudoClassSelector) String() string {
	if c.ofType {
		return ":only-of-type"
	}
	return ":only-child"
}
func (c inputPseudoClassSelector) String() string {
	return ":input"
}
func (c emptyElementPseudoClassSelector) String() string {
	return ":empty"
}
func (c rootPseudoClassSelector) String() string {
	return ":root"
}

func (c compoundSelector) String() string {
	if len(c.selectors) == 0 && c.pseudoElement == "" {
		return "*"
	}
	chunks := make([]string, len(c.selectors))
	for i, sel := range c.selectors {
		chunks[i] = sel.String()
	}
	s := strings.Join(chunks, "")
	if c.pseudoElement != "" {
		s += "::" + c.pseudoElement
	}
	return s
}

func (c combinedSelector) String() string {
	start := c.first.String()
	if c.second != nil {
		start += fmt.Sprintf(" %s %s", string(c.combinator), c.second.String())
	}
	return start
}

func (c SelectorGroup) String() string {
	ck := make([]string, len(c))
	for i, s := range c {
		ck[i] = s.String()
	}
	return strings.Join(ck, ", ")
}
