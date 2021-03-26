// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
tested against every rule, and so on, until no further rules match. The order
that rules are applied depends on the order of the rules in each file, the
lexicographical ordering of files, and whether or not a rule is marked as high
or low priority as it is depicted below:

[InlineConstVar, Normalize, HighPriority]


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

Child patterns within match and replace patterns can be "bound" to a named
variable. These variables can then be referenced later in the match pattern or
in the replace pattern. This is a critical part of the Optgen language, since
virtually every pattern constructs its replacement pattern based on parts of
the match pattern. For example:

  [EliminateSelect]
  (Select $input:* (True)) => $input

The $input variable is bound to the first child of the "Select" node. If the
second child is a "True" node, then the "Select" node will be replaced by its
input. Variables can also be passed as arguments to custom matchers, which are
described below.

Let Expression

A let expression can be used for binding multiple variables to the result of a
custom function with multiple return values. This expression consists of two
elements, a binding and a result. The binding includes a list of variables to
bind followed by a custom function to produce the bind values. The result is a
variable reference which is the value of the expression when evaluated.

For example:

  [SplitSelect]
  (Select
    $input:*
    $filters:* &
      (Let ($filterA $filterB $ok):(SplitFilters $filters) $ok)
  )
  =>
  (Select (Select $input $filterA) $filterB)

The "($filtersA $filtersB $ok):(SplitFilters $filters)" part indicates that
$filtersA $filtersB and $ok are bound to the three return values of
(SplitFilters $filters). The let expression evaluates to the value of $ok.

A let expression can also be used in a replace pattern. For example:

  [AlterSelect]
  (Select $input:* $filters:*)
  =>
  (Select
    (Let ($newInput $newFilters):(AlterSelect $input $filters) $newInput)
    $newFilters
  )

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

String and numeric constant nodes in the tree can be matched against literals.
A literal string or number in a match pattern is interpreted as a matcher of
that type, and will be tested for equality with the child node. For example:

  [EliminateConcat]
  (Concat $left:* (Const "")) => $left

If Concat's right operand is a constant expression with the empty string as its
value, then the pattern matches. Similarly, a constant numeric expression can be
matched like this:

  [LimitScan]
  (Limit (Scan $def:*) (Const 1)) => (ScanOneRow $def)

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

Sometimes the name of a constructed node can be one of several choices. The
built-in "OpName" function can be used to dynamically construct the right kind
of node. For example:

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

Type Inference

Expressions in both the match and replace patterns are assigned a data type
that describes the kind of data that will be returned by the expression. These
types are inferred using a combination of top-down and bottom-up type inference
rules. For example:

  define Select {
    Input  Expr
    Filter Expr
  }

  (Select $input:(LeftJoin | RightJoin) $filter:*) => $input

The type of $input is inferred as "LeftJoin | RightJoin" by bubbling up the type
of the bound expression. That type is propagated to the $input reference in the
replace pattern. By contrast, the type of the * expression is inferred to be
"Expr" using a top-down type inference rule, since the second argument to the
Select operator is known to have type "Expr".

When multiple types are inferred for an expression using different type
inference rules, the more restrictive type is assigned to the expression. For
example:

  (Select $input:* & (LeftJoin)) => $input

Here, the left input to the And expression was inferred to have type "Expr" and
the right input to have type "LeftJoin". Since "LeftJoin" is the more
restrictive type, the And expression and the $input binding are typed as
"LeftJoin".

Type inference detects and reports type contradictions, which occur when
multiple incompatible types are inferred for an expression. For example:

  (Select $input:(InnerJoin) & (LeftJoin)) => $input

Because the input cannot be both an InnerJoin and a LeftJoin, Optgen reports a
type contradiction error.

Syntax

This section describes the Optgen language syntax in a variant of extended
Backus-Naur form. The non-terminals correspond to methods in the parser. The
terminals correspond to tokens returned by the scanner. Whitespace and
comment tokens can be freely interleaved between other tokens in the
grammar.

  root         = tags (define | rule)
  tags         = '[' IDENT (',' IDENT)* ']'

  define       = 'define' define-name '{' define-field* '}'
  define-name  = IDENT
  define-field = field-name field-type
  field-name   = IDENT
  field-type   = IDENT

  rule         = func '=>' replace
  match        = func
  replace      = func | ref
  func         = '(' func-name arg* ')'
  func-name    = names | func
  names        = name ('|' name)*
  arg          = bind and | ref | and
  and          = expr ('&' and)
  expr         = func | not | let | list | any | name | STRING | NUMBER
  not          = '^' expr
  list         = '[' list-child* ']'
  list-child   = list-any | arg
  list-any     = '...'
  bind         = '$' label ':' and
  let          = '(' 'Let' '(' '$' label ('$' label)* ')' ':' func ref ')'
  ref          = '$' label
  any          = '*'
  name         = IDENT
  label        = IDENT

Here are the pseudo-regex definitions for the lexical tokens that aren't
represented as single-quoted strings above:

  STRING     = " [^"\n]* "
  NUMBER     = UnicodeDigit+
  IDENT      = (UnicodeLetter | '_') (UnicodeLetter | '_' | UnicodeNumber)*
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
