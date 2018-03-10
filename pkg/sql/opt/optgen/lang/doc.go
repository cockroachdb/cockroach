// Copyright 2018 The Cockroach Authors.
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

/*
Package lang implements a language called Optgen, short for "optimizer
generator". Optgen is a domain-specific language (DSL) that provides an
intuitive syntax for defining, matching, and replacing nodes in a target
expression tree. Here is an example:

  [NormalizeEq]
  (Eq
    $left:^(Variable)
    $right:(Variable)
  )
  =>
  (Eq $right $left)

The expression above the arrow is called the "match pattern" and the expression
below the arrow is called the "replace pattern". If a node in the target
expression tree matches the match pattern, then it will be replaced by a node
that is constructed according to the replace pattern. Together, the match
pattern and replace pattern are called a "rule".

In addition to rules, the Optgen language includes "definitions". Each
definition names and describes one of the nodes that the target expression tree
may contain. Match and replace patterns can recognize and construct these
nodes. Here is an example:

  define Eq {
    Left  Expr
    Right Expr
  }

The following sections provide more detail on the Optgen language syntax and
semantics, as well as some implementation notes.

Definitions

Optgen language input files may contain any number of definitions, in any
order. Each definition describes a node in the target expression tree. A
definition has a name and a set of "fields" which describe the node's children.
A definition may have zero fields, in which case it describes a node with zero
children, which is always a "leaf" in the expression tree.

A field definition consists of two parts - the field's name and its type. The
Optgen parser treats the field's type as an opaque identifier; it's up to other
components to interpret it. However, typically the field type refers to either
some primitive type (like string or int), or else refers to the name of some
other operator or group of operators.

Here is the syntax for an operator definition:

  define <name> {
    <field-1-name> <field-1-type>
    <field-2-name> <field-2-type>
    ...
  }

And here is an example:

  define Join {
    Left  Expr
    Right Expr
    On    Expr
  }

Definition Tags

A "definition tag" is an opaque identifier that describes some property of the
defined node. Definitions can have multiple tags or no tags at all, and the
same tag can be attached to multiple definitions. Tags can be used to group
definitions according to some shared property or logical grouping. For example,
arithmetic or boolean operators might be grouped together. Match patterns can
then reference those tags in order to match groups of nodes (see "Matching
Names" section).

Here is the definition tagging syntax:

  [<tag-1-name>, <tag-2-name>, ...]
  define <name> {
  }

And here is an example:

  [Comparison, Inequality]
  define Lt {
    Left  Expr
    Right Expr
  }

Rules

Optgen language input files may contain any number of rules, in any order. Each
rule has a unique name and consists of a match pattern and a corresponding
replace pattern. A rule's match pattern is tested against every node in the
target expression tree, bottom-up. Each matching node is replaced by a node
constructed according to the replace pattern. The replacement node is itself
tested against every rule, and so on, until no further rules match.

Note that this is just a conceptual description. Optgen does not actually do
any of this matching or replacing itself. Other components use the Optgen
library to generate code. These components are free to match however they want,
and to replace nodes or keep the new and old nodes side-by-side (as with a
typical optimizer MEMO structure).

Similar to define statements, a rule may have a set of tags associated with it.
Rule tags logically group rules, and can also serve as directives to the code
generator.

Here is the partial rule syntax (see Syntax section for full syntax):

  [<rule-name>, <tag-1-name>, <tag-2-name>, ...]
  (<match-opname>
    <match-expr>
    <match-expr>
    ...
  )
  =>
  (<replace-opname>
    <replace-expr>
    <replace-expr>
    ...
  )

Match Patterns

The top-level match pattern matches the name and children of one or more nodes
in the target expression tree. For example:

  (Eq * *)

The "*" character is the "wildcard matcher", which matches a child of any kind.
Therefore, this pattern matches any node named "Eq" that has at least two
children. Matchers can be nested within one another in order to match children,
grandchildren, etc. For example:

  (Eq (Variable) (Const))

This pattern matches an "Eq" node with a "Variable" node as its left child
and a "Const" node as its right child.

Binding

Child patterns within a node match pattern can be "bound" to a named variable.
These variables can then be referenced later in the match pattern or in the
replace pattern. This is a critical part of the Optgen language, since
virtually every pattern constructs its replacement pattern based on parts of
the match pattern. For example:

  [EliminateSelect]
  (Select $input:* (True)) => $input

The $input variable is bound to the first child of the "Select" node. If the
second child is a "True" node, then the "Select" node will be replaced by its
input. Variables can also be passed as arguments to custom matchers, which are
described below.

Matching Names

In addition to simple name matching, a node matcher can match tag names. Any
node type which has the named tag is matched. For example:

  [Inequality]
  define Lt {
    Left Expr
    Right Expr
  }

  [Inequality]
  define Gt
  {
    Left Expr
    Right Expr
  }

  (Inequality (Variable) (Const))

This pattern matches either "Lt" or "Gt" nodes. This is useful for writing
patterns that apply to multiple kinds of nodes, without need for duplicate
patterns that only differ in matched node name.

The node matcher also supports multiple names in the match list, separated by
'|' characters. The node's name is allowed to match any of the names in the
list. For example:

  (Eq | Ne | Inequality)

This pattern matches "Eq", "Ne", "Lt", or "Gt" nodes.

Matching Primitive Types

Often, there are leaf nodes in the target expression tree that can be matched
against primitive types, like simple strings. A literal string in a match
pattern is interpreted as a "string matcher" and is tested for equality with
the child node. For example:

  [EliminateConcat]
  (Concat $left:* "") => $left

If Concat's right operand is a literal string expression that's equal to the
empty string, then the pattern matches.

Matching Lists

Nodes can have a child that is a list of nodes rather than a single node. As an
example, a function call node might have two children: the name of the function
and the list of arguments to the function:

  define FuncCall {
    Name Expr
    Args ExprList
  }

There are several kinds of list matchers, each of which uses a variant of the
list matching bracket syntax. The ellipses signify that 0 or more items can
match at either the beginning or end of the list. The item pattern can be any
legal match pattern, and can be bound to a variable.

  [ ... <item pattern> ... ]

- ANY: Matches if any item in the list matches the item pattern. If multiple
items match, then the list matcher binds to the first match.

  [ ... $item:* ... ]

- FIRST: Matches if the first item in the list matches the item pattern (and
there is at least one item in the list).

  [ $item:* ... ]

- LAST: Matches if the last item in the list matches the item pattern (and
there is at least one item).

  [ ... $item:* ]

- SINGLE: Matches if there is exactly one item in the list, and it matches the
item pattern.

  [ $item:* ]

- EMPTY: Matches if there are zero items in the list.

  []

Following is a more complete example. The ANY list matcher in the example
searches through the Filter child's list, looking for a Subquery node. If a
matching node is found, then the list matcher succeeds and binds the node to
the $item variable.

  (Select
    $input:*
    (Filter [ ... $item:(Subquery) ... ])
  )

Custom Matching

When the supported matching syntax is insufficient, Optgen provides an escape
mechanism. Custom matching functions can invoke Go functions, passing
previously bound variables as arguments, and checking the boolean result for a
match. For example:

  [EliminateFilters]
  (Filters $items:* & (IsEmptyList $items)) => (True)

This pattern passes the $items child node to the IsEmptyList function. If that
returns true, then the pattern matches.

Custom matching functions can appear anywhere that other matchers can, and can
be combined with other matchers using boolean operators (see the Boolean
Expressions section for more details). While variable references are the most
common argument, it is also legal to nest function invocations:

  (Project
    $input:*
    $projections:* & ^(IsEmpty (FindUnusedColumns $projections))
  )

Boolean Expressions

Multiple match expressions of any type can be combined using the boolean &
(AND) operator. All must match in order for the overall match to succeed:

  (Not
    $input:(Comparison) & (Inequality) & (CanInvert $input)
  )

The boolean ^ (NOT) operator negates the result of a boolean match expression.
It can be used with any kind of matcher, including custom match functions:

  (JoinApply
    $left:^(Select)
    $right:* & ^(IsCorrelated $right $left)
    $on:*
  )

This pattern matches only if the left child is not a Select node, and if the
IsCorrelated custom function returns false.

Replace Patterns

Once a matching node is found, the replace pattern produces a single
substitution node. The most common replace pattern involves constructing one or
more new nodes, often with child nodes that were bound in the match pattern.
A construction expression specifies the name of the node as its first operand
and its children as subsequent arguments. Construction expressions can be
nested within one another to any depth. For example:

  [HoistSelectExists]
  (Select
    $input:*
    $filter:(Exists $subquery:*)
  )
  =>
  (SemiJoinApply
    $input
    $subquery
    (True)
  )

The replace pattern in this rule constructs a new SemiJoinApply node, with its
first two children bound in the match pattern. The third child is a newly
constructed True node.

The replace pattern can also consist of a single variable reference, in the
case where the substitution node was already present in the match pattern:

  [EliminateAnd]
  (And $left:* (True)) => $left

Literal strings can be part of a replace pattern, or even all of it, if the
intent is to construct a constant string node:

  (Concat "" "") => ""

Custom Construction

When Optgen syntax cannot easily produce a result, custom construction
functions allow results to be derived in Go code. If a construction
expression's name is not recognized as a node name, then it is assumed to be
the name of a custom function. For example:

  [MergeSelectJoin]
  (Select
    (InnerJoin $r:* $s:* $on:*)
    $filter:*
  )
  =>
  (InnerJoin
    $r
    $s
    (ConcatFilters $on $filter)
  )

Here, the ConcatFilters custom function is invoked in order to concatenate two
filter lists together. Function parameters can include nodes, lists (see the
Constructing Lists section), operator names (see the Name parameters section),
and the results of nested custom function calls. While custom functions
typically return a node, they can return other types if they are parameters to
other custom functions.

Constructing Lists

Lists can be constructed and passed as parameters to node construction
expressions or custom replace functions. A list consists of multiple items that
can be of any parameter type, including nodes, strings, custom function
invocations, or lists. Here is an example:

  [MergeSelectJoin]
  (Select
    (InnerJoin $left:* $right:* $on:*)
    $filters:*
  )
  =>
  (InnerJoin
    $left
    $right
    (And [$on $filters])
  )

Dynamic Construction

Sometimes the name of a constructed node can be one of several choices, and may
not even be known at compile-time. The built-in "OpName" function can be used
to dynamically construct the right kind of node. For example:

  [NormalizeVar]
  (Eq | Ne
    $left:^(Variable)
    $right:(Variable)
  )
  =>
  ((OpName) $right $left)

In this pattern, the name of the constructed result is either Eq or Ne,
depending on which is matched. When the OpName function has no arguments, then
it is bound to the name of the node matched at the top-level. The OpName
function can also take a single variable reference argument. In that case, it
refers to the name of the node bound to that variable:

  [PushDownSelect]
  (Select
    $input:(Join $left:* $right:* $on:*)
    $filter:* & ^(IsCorrelated $filter $right)
  )
  =>
  ((OpName $input)
    (Select $left $filter)
    $right
    $on
  )

In this pattern, Join is a tag that refers to a group of nodes. The replace
expression will construct a node having the same name as the matched join node.

Name Parameters

The OpName built-in function can also be a parameter to a custom match or
replace function which needs to know which name matched. For example:

  [FoldBinaryNull]
  (Binary $left:* (Null) & ^(HasNullableArgs (OpName)))
  =>
  (Null)

The name of the matched Binary node (e.g. Plus, In, Contains) is passed to the
HasNullableArgs function as a symbolic identifier. Here is an example that uses
a custom replace function and the OpName function with an argument:

  [NegateComparison]
  (Not $input:(Comparison $left:* $right:*))
  =>
  (InvertComparison (OpName $input) $left $right)

As described in the previous section, adding the argument enables OpName to
return a name that was matched deeper in the pattern.

In addition to a name returned by the OpName function, custom match and replace
functions can accept literal operator names as parameters. The Minus operator
name is passed as a parameter to two functions in this example:

  [FoldMinus]
  (UnaryMinus
    (Minus $left $right) & (OverloadExists Minus $right $left)
  )
  =>
  (ConstructBinary Minus $right $left)

Syntax

This section describes the Optgen language syntax in a variant of extended
Backus-Naur form. The non-terminals correspond to methods in the parser. The
terminals correspond to tokens returned by the scanner. Whitespace and
comment tokens can be freely interleaved between other tokens in the
grammar.

  root                = tags (define | rule)
  tags                = '[' IDENT (',' IDENT)* ']'

  define              = 'define' define-name '{' define-field* '}'
  define-name         = IDENT
  define-field        = field-name field-type
  field-name          = IDENT
  field-type          = IDENT

  rule                = match '=>' replace
  match               = '(' match-names match-child* ')'
  match-names         = name ('|' name)*
  match-child         = bind | ref | match-and
  bind                = '$' label ':' match-and
  ref                 = '$' label
  match-and           = match-item ('&' match-and)
  match-item          = match | match-not | match-list | match-any | name |
                        STRING
  match-not           = '^' match-item
  match-list          = match-list-any | match-list-first | match-list-last |
                        match-list-single | match-list-empty
  match-list-any      = '[' '...' match-child '...' ']'
  match-list-first    = '[' match-child '...' ']'
  match-list-last     = '[' '...' match-child ']'
  match-list-single   = '[' match-child ']'
  match-list-empty    = '[' ']'
  match-any           = '*'

  replace             = construct | construct-list | ref | STRING
  construct           = '(' construct-name replace* ')'
  construct-name      = name | construct
  construct-list      = '[' replace* ']'

  name                = IDENT
  label               = IDENT

Here are the pseudo-regex definitions for the lexical tokens that aren't
represented as single-quoted strings above:

  STRING     = " [^"\n]* "
  IDENT      = UnicodeLetter (UnicodeLetter | UnicodeNumber)*
  COMMENT    = '#' .* \n
  WHITESPACE = UnicodeSpace+

The support directory contains syntax coloring files for several editors,
including Vim, TextMate, and Visual Studio Code. JetBrains editor (i.e. GoLand)
can also import TextMate bundles to provide syntax coloring.

Components

The lang package contains a scanner that breaks input files into lexical
tokens, a parser that assembles an abstract syntax tree (AST) from the tokens,
and a compiler that performs semantic checks and creates a rudimentary symbol
table.

The compiled rules and definitions become the input to a separate code
generation package which generates parts of the Cockroach DB SQL optimizer.
However, the Optgen language itself is not Cockroach or SQL specific, and can
be used in other contexts. For example, the Optgen language parser generates
its own AST expressions using itself (compiler bootstrapping).
*/
package lang
