// Package quickfix contains analyzes that implement code refactorings.
// None of these analyzers produce diagnostics that have to be followed.
// Most of the time, they only provide alternative ways of doing things,
// requiring users to make informed decisions.
//
// None of these analyzes should fail a build, and they are likely useless in CI as a whole.
package quickfix

import "honnef.co/go/tools/analysis/lint"

var Docs = lint.Markdownify(map[string]*lint.RawDocumentation{
	"QF1001": {
		Title:    "Apply De Morgan's law",
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1002": {
		Title: "Convert untagged switch to tagged switch",
		Text: `
An untagged switch that compares a single variable against a series of
values can be replaced with a tagged switch.`,
		Before: `
switch {
case x == 1 || x == 2, x == 3:
    ...
case x == 4:
    ...
default:
    ...
}`,

		After: `
switch x {
case 1, 2, 3:
    ...
case 4:
    ...
default:
    ...
}`,
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1003": {
		Title: "Convert if/else-if chain to tagged switch",
		Text: `
A series of if/else-if checks comparing the same variable against
values can be replaced with a tagged switch.`,
		Before: `
if x == 1 || x == 2 {
    ...
} else if x == 3 {
    ...
} else {
    ...
}`,

		After: `
switch x {
case 1, 2:
    ...
case 3:
    ...
default:
    ...
}`,
		Since:    "2021.1",
		Severity: lint.SeverityInfo,
	},

	"QF1004": {
		Title:    `Use \'strings.ReplaceAll\' instead of \'strings.Replace\' with \'n == -1\'`,
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1005": {
		Title:    `Expand call to \'math.Pow\'`,
		Text:     `Some uses of \'math.Pow\' can be simplified to basic multiplication.`,
		Before:   `math.Pow(x, 2)`,
		After:    `x * x`,
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1006": {
		Title: `Lift \'if\'+\'break\' into loop condition`,
		Before: `
for {
    if done {
        break
    }
    ...
}`,

		After: `
for !done {
    ...
}`,
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1007": {
		Title: "Merge conditional assignment into variable declaration",
		Before: `
x := false
if someCondition {
    x = true
}`,
		After:    `x := someCondition`,
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1008": {
		Title:    "Omit embedded fields from selector expression",
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1009": {
		Title:    `Use \'time.Time.Equal\' instead of \'==\' operator`,
		Since:    "2021.1",
		Severity: lint.SeverityInfo,
	},

	"QF1010": {
		Title:    "Convert slice of bytes to string when printing it",
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1011": {
		Title:    "Omit redundant type from variable declaration",
		Since:    "2021.1",
		Severity: lint.SeverityHint,
	},

	"QF1012": {
		Title:    `Use \'fmt.Fprintf(x, ...)\' instead of \'x.Write(fmt.Sprintf(...))\'`,
		Since:    "2022.1",
		Severity: lint.SeverityHint,
	},
})
