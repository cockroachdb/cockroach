# optgen README

The Optgen language support extension makes it easier to author optimizer
generator (optgen) files. Optgen is a tool that generates Go code for a
memoizing cost-based optimizer. Optgen files contain a domain-specific language
that defines expression tree nodes, as well as a tree pattern matching and
replacement language. Operator definitions are used by optgen to generate
expression tree constants, structs, and methods. Optgen rules match parts of
the expression tree and can then rewrite that section of the tree in a
logically equivalent way.

## Features

The Optgen language support extension currently provides VS Code colorization
support for Optgen definitions and rules. The colorization uses a TextMate
Language Grammar, and so can be used in TextMate, IntelliJ, or any other editor
that supports it.

## Known Issues

None.

## Release Notes

### 1.0.0
Initial release of Optgen.
