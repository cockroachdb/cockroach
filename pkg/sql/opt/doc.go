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
Package opt contains the Cockroach SQL optimizer. The optimizer transforms the
AST of a SQL query into a physical query plan for execution. Naive execution of
a SQL query can be prohibitively expensive, because SQL specifies the desired
results and not how to achieve them. A given SQL query can have thousands of
equivalent query plans with vastly different execution times. The Cockroach
optimizer is cost-based, meaning that it enumerates some or all of these
alternate plans and chooses the one with the lowest estimated cost.

Overview

SQL query planning is often described in terms of 8 modules:

1. Properties

2. Stats

3. Cost Model

4. Memo

5. Transforms

6. Prep

7. Rewrite

8. Search

Note Prep, Rewrite and Search could be considered phases, though this document
will refer to all 8 uniformly as modules. Memo is a technique for compactly
representing the forest of trees generated during Search. Stats, Properties,
Cost Model and Transformations are modules that power the Prep, Rewrite and
Search phases.

                       SQL query text
                             |
                       +-----v-----+  - parse SQL text according to grammar
                       |   Parse   |  - report syntax errors
                       +-----+-----+
                             |
                           (ast)
                             |
                       +-----v-----+  - fold constants, check types, resolve
                       |  Analyze  |    names
                       +-----+-----+  - annotate tree with semantic info
                             |        - report semantic errors
                           (ast+)
         +-------+           |
         | Stats +----->-----v-----+  - normalize tree with cost-agnostic
         +-------+     |   Prep    |    transforms (placeholders present)
                    +-->-----+-----+  - compute initial properties
                    |        |        - retrieve and attach stats
                    |     (expr)      - done once per PREPARE
                    |        |
    +------------+  |  +-----v-----+  - capture placeholder values / timestamps
    | Transforms |--+-->  Rewrite  |  - normalize tree with cost-agnostic
    +------------+  |  +-----+-----+    transforms (placeholders not present)
                    |        |        - done once per EXECUTE
                    |     (expr)
                    |        |
                    +-->-----v-----+  - generate equivalent expression trees
    +------------+     |  Search   |  - find lowest cost physical plan
    | Cost Model +----->-----+-----+  - includes DistSQL physical planning
    +------------+           |
                      (physical plan)
                             |
                       +-----v-----+
                       | Execution |
                       +-----------+

The opt-related packages implement portions of these modules, while other parts
are implemented elsewhere. For example, other sql packages are used to perform
name resolution and type checking which are part of the Analyze phase.

Parse

The parse phase is not discussed in this document. It transforms the SQL query
text into an abstract syntax tree (AST).

Analyze

The analyze phase ensures that the AST obeys all SQL semantic rules, and
annotates the AST with information that will be used by later phases. In
addition, some simple transforms are applied to the AST in order to simplify
handling in later phases. Semantic rules are many and varied; this document
describes a few major categories of semantic checks and rewrites.

"Name resolution" binds table, column, and other references. Each name must be
matched to the appropriate schema object, and an error reported if no matching
object can be found. Name binding can result in AST annotations that make it
easy for other components to find the target object, or rewrites that replace
unbound name nodes with new nodes that are easier to handle (e.g. IndexedVar).

"Constant folding" rewrites expressions that have constant inputs. For example,
1+1 would be folded to 2. Cockroach's typing rules assume that constants have
been folded, as there are some expressions that would otherwise produce a
semantic error if they are not first folded.

"Type inference" automatically determines the return data type of various SQL
expressions, based on the types of inputs, as well as the context in which the
expression is used. The AST is annotated with the resolved types for later use.

"Type checking" ensures that all inputs to SQL expressions and statements have
legal static types. For example, the CONCAT function only accepts zero or more
arguments that are statically typed as strings. Violation of the typing rules
produces a semantic error.

Properties

Properties are meta-information that are computed (and sometimes stored) for
each node in an expression. Properties power transformations and optimization.

"Logical properties" describe the structure and content of data returned by an
expression, such as whether relational output columns can contain nulls, or the
data type of a scalar expression. Two expressions which are logically
equivalent according to the rules of the relational algebra will return the
same set of rows and columns, and will have the same set of logical properties.
However, the order of the rows, naming of the columns, and other presentational
aspects of the result are not governed by the logical properties.

"Physical properties" are interesting characteristics of an expression that
impact its layout, presentation, or location, but not its logical content.
Examples include row order, column naming, and data distribution (physical
location of data ranges). Physical properties exist outside of the relational
algebra, and arise from both the SQL query itself (e.g. the non-relational
ORDER BY operator) and by the selection of specific implementations during
optimization (e.g. a merge join requires the inputs to be sorted in a
particular order).

Properties can be "required" or "derived". A required property is one specified
by the SQL query text. For example, a DISTINCT clause is a required property on
the set of columns of the corresponding projection -- that the tuple of columns
forms a key (unique values) in the results. A derived property is one derived
by the optimizer for an expression based on the properties of the child
expressions. For example:

  SELECT k+1 FROM kv

Once the ordering of "k" is known from kv's descriptor, the same ordering
property can be derived for k+1. During optimization, for each expression with
required properties, the optimizer will look at child expressions to check
whether their actual properties (which can be derived) match the requirement.
If they don't, the optimizer must introduce an "enforcer" operator in the plan
that provides the required property. For example, an ORDER BY clause creates a
required ordering property that can cause the optimizer to add a Sort operator
as an enforcer of that property.

Stats

Table statistics power both the cost model and the search of alternate query
plans. A simple example of where statistics guide the search of alternate query
plans is in join ordering:

	SELECT * FROM a JOIN b

In the absence of other opportunities, this might be implemented as a hash
join. With a hash join, we want to load the smaller set of rows (either from a
or b) into the hash table and then query that table while looping through the
larger set of rows. How do we know whether a or b is larger? We keep statistics
about the cardinality of a and b, i.e. the (approximate) number of different
values.

Simple table cardinality is sufficient for the above query but fails in other
queries. Consider:

	SELECT * FROM a JOIN b ON a.x = b.x WHERE a.y > 10

Table statistics might indicate that a contains 10x more data than b, but the
predicate a.y > 10 is filtering a chunk of the table. What we care about is
whether the result of the scan *after* filtering returns more rows than the
scan of b. This can be accomplished by making a determination of the
selectivity of the predicate a.y > 10 (the % of rows it will filter) and then
multiplying that selectivity by the cardinality of a. The common technique for
estimating selectivity is to collect a histogram on a.y.

The collection of table statistics occurs prior to receiving the query. As
such, the statistics are necessarily out of date and may be inaccurate. The
system may bound the inaccuracy by recomputing the stats based on how fast a
table is being modified. Or the system may notice when stat estimations are
inaccurate during query execution.

Cost Model

The cost model takes an expression as input and computes an estimated "cost"
to execute that expression. The unit of "cost" can be arbitrary, though it is
desirable if it has some real world meaning such as expected execution time.
What is required is for the costs of different query plans to be comparable.
The optimizer seeks to find the shortest expected execution time for a query
and uses cost as a proxy for execution time.

Cost is roughly calculated by estimating how much time each node in the
expression tree will use to process all results and modeling how data flows
through the expression tree. Table statistics are used to power cardinality
estimates of base relations which in term power cardinality estimates of
intermediate relations. This is accomplished by propagating histograms of
column values from base relations up through intermediate nodes (e.g. combining
histograms from the two join inputs into a single histogram). Operator-specific
computations model the network, disk and CPU costs. The cost model should
include data layout and the specific operating environment. For example,
network RTT in one cluster might be vastly different than another.

Because the cost for a query plan is an estimate, there is an associated error.
This error might be implicit in the cost, or could be explicitly tracked. One
advantage to explicitly tracking the expected error is that it can allow
selecting a higher cost but lower expected error plan over a lower cost but
higher expected error plan. Where does the error come from? One source is the
innate inaccuracy of stats: selectivity estimation might be wildly off due to
an outlier value. Another source is the accumulated build up of estimation
errors the higher up in the query tree. Lastly, the cost model is making an
estimation for the execution time of an operation such as a network RTT. This
estimate can also be wildly inaccurate due to bursts of activity.

Search finds the lowest cost plan using dynamic programming. That imposes a
restriction on the cost model: it must exhibit optimal substructure. An optimal
solution can be constructed from optimal solutions of its sub-problems.

Memo

Memo is a data structure for efficiently storing a forest of query plans.
Conceptually, the memo is composed of a numbered set of equivalency classes
called groups where each group contains a set of logically equivalent
expressions. The different expressions in a single group are called memo
expressions (memo-ized expressions). A memo expression has a list of child
groups as its children rather than a list of individual expressions. The
forest is composed of every possible combination of parent expression with
its children, recursively applied.

Memo expressions can be relational (e.g. join) or scalar (e.g. <). Operators
are always both logical (specify results) and physical (specify results and a
particular implementation). This means that even a "raw" unoptimized expression
tree can be executed (naively). Both relational and scalar operators are
uniformly represented as nodes in memo expression trees, which facilitates tree
pattern matching and replacement.

Because memo groups contain logically equivalent expressions, all the memo
expressions in a group share the same logical properties. However, it's
possible for two logically equivalent expressions to be placed in different
memo groups. This occurs because determining logical equivalency of two
relational expressions is too complex to perform 100% correctly. A correctness
failure (i.e. considering two expressions logically equivalent when they are
not) results in invalid transformations and invalid plans. But placing two
logically equivalent expressions in different groups has a much gentler failure
mode: the memo and transformations are less efficient. Expressions within the
memo may have different physical properties. For example, a memo group might
contain both hash join and merge join expressions which produce the same set of
output rows, but produce them in different orders.

Expressions are inserted into the memo by the factory, which ensure that
expressions have been fully normalized before insertion (see the Prep section
for more details). A new group is created only when unique normalized
expressions are created by the factory during construction or rewrite of the
tree. Uniqueness is determined by computing the fingerprint for a memo
expression, which is simply the expression operator and its list of child
groups. For example, consider this query:

	SELECT * FROM a, b WHERE a.x = b.x

After insertion into the memo, the memo would contain these six groups:

	6: [inner-join [1 2 5]]
	5: [eq [3 4]]
	4: [variable b.x]
	3: [variable a.x]
	2: [scan b]
	1: [scan a]

The fingerprint for the inner-join expression is [inner-join [1 2 5]]. The
memo maintains a map from expression fingerprint to memo group which allows
quick determination of whether the normalized form of an expression already
exists in the memo.

The normalizing factory will never add more than one expression to a memo
group. But the explorer (see Search section for more details) does add
denormalized expressions to existing memo groups, since oftentimes one of these
equivalent, but denormalized expressions will have a lower cost than the
initial normalized expression added by the factory. For example, the join
commutativity transformation expands the memo like this:

	6: [inner-join [1 2 5]] [inner-join [2 1 5]]
	5: [eq [3 4]]
	4: [variable b.x]
	3: [variable a.x]
	2: [scan b]
	1: [scan a]

Notice that there are now two expressions in memo group 6. The coster (see Cost
Model section for more details) will estimate the execution cost of each
expression, and the optimizer will select the lowest cost alternative.

Transforms

Transforms convert an input expression tree into zero or more logically
equivalent trees. Transforms consist of two parts: a "match pattern" and a
"replace pattern". Together, the match pattern and replace pattern are called a
"rule". Transform rules are categorized as "normalization" or "exploration"
rules.

If an expression in the tree matches the match pattern, then a new expression
will be constructed according to the replace pattern. Note that "replace" means
the new expression is a logical replacement for the existing expression, not
that the existing expression needs to physically be replaced. Depending on the
context, the existing expression may be discarded, or it may be retained side-
by-side with the new expression in the memo group.

Normalization rules are cost-agnostic, as they are always considered to be
beneficial. All normalization rules are implemented by the normalizing factory,
which does its best to map all logically equivalent expression trees to a
single canonical form from which searches can branch out. See the Prep section
for more details.

Exploration rules generate equivalent expression trees that must be costed in
order to determine the lowest cost alternative. All exploration rules are
implemented by the explorer, which is optimized to efficiently enumerate all
possible expression tree combinations in the memo in order to look for rule
matches. When it finds a match, the explorer applies the rule and adds an
equivalent expression to the existing memo group. See the Search section for
more details.

Some examples of transforms:

	Join commutativity
	Swaps the order of the inputs to an inner join.
		SELECT * FROM a, b => SELECT * FROM b, a

	Join associativity
	Reorders the children of a parent and child join
		SELECT * FROM (SELECT * FROM a, b), c
		=>
		SELECT * FROM (SELECT * FROM a, c), b

	Predicate pushdown
	Moves predicates below joins
		SELECT * FROM a, b USING (x) WHERE a.x < 10
		=>
		SELECT * FROM (SELECT * FROM a WHERE a.x < 10), b USING (x)

	Join elimination
	Removes unnecessary joins based on projected columns and foreign keys.
		SELECT a.x FROM a, b USING (x)
		=>
		SELECT a.x FROM a

	Distinct/group-by elimination
	Removes unnecessary distinct/group-by operations based on keys.
		SELECT DISTINCT a.x FROM a
		=>
		SELECT a.x FROM a

	Predicate inference
	Adds predicates based on filter conditions.
		SELECT * FROM a, b USING (x)
		=>
		SELECT * FROM a, b USING (x) WHERE a.x IS NOT NULL AND b.x IS NOT NULL

	Decorrelation
	Replaces correlated subqueries with semi-join, anti-join and apply ops.

	Scan to index scan
	Transforms scan operator into one or more index scans on covering indexes.

	Inner join to merge join
	Generates alternate merge-join operator from default inner-join operator.

Much of the optimizer's rule matching and application code is generated by a
tool called Optgen, short for "optimizer generator". Optgen is a domain-
specific language (DSL) that provides an intuitive syntax for defining
transform rules. Here is an example:

  [NormalizeEq]
  (Eq
    $left:^(Variable)
    $right:(Variable)
  )
  =>
  (Eq $right $left)

The expression above the arrow is the match pattern and the expression below
the arrow is the replace pattern. This example rule will match Eq expressions
which have a left input which is not a Variable operator and a right input
which is a Variable operator. The replace pattern will trigger a replacement
that reverses the two inputs. In addition, custom match and replace functions
can be defined in order to run arbitrary Go code.

Prep

Prep (short for "prepare") is the first phase of query optimization, in which
the annotated AST is transformed into a single normalized "expression tree".
The optimizer directly creates the expression tree in the memo data structure
rather than first constructing an intermediate data structure. A forest of
equivalent trees will be generated in later phases, but at the end of the prep
phase, the memo contains just one normalized tree that is logically equivalent
to the SQL query.

During the prep phase, placeholder values are not yet known, so normalization
cannot go as far as it can during later phases. However, this also means that
the resulting expression tree can be cached in response to a PREPARE statement,
and then be reused as a starting point each time an EXECUTE statement provides
new placeholder values.

The memo expression tree is constructed by the normalizing factory, which does
its best to map all logically equivalent expression trees to a single canonical
form from which searches can branch out. The factory has an interface similar
to this:

	ConstructConst(value PrivateID) GroupID
	ConstructAnd(conditions ListID) GroupID
	ConstructInnerJoin(left GroupID, right GroupID, on GroupID) GroupID

The factory methods construct a memo expression tree bottom-up, with each memo
group becoming an input to operators higher in the tree.

As each expression is constructed by the factory, it transitively applies
normalization rules defined for that expression type. This may result in the
construction of a different type of expression than what was requested. If,
after normalization, the expression is already part of the memo, then
construction is a no-op. Otherwise, a new memo group is created, with the
normalized expression as its first and only expression.

By applying normalization rules as the expression tree is constructed, the
factory can avoid creating intermediate expressions; often, "replacement" of
an existing expression means it's never created to begin with.

During Prep, all columns used by the SQL query are given a numeric index that
is unique across the query. Column numbering involves assigning every base
column and non-trivial projection in a query a unique, query-specific index.
Giving each column a unique index allows the expression nodes mentioned above
to track input and output columns, or really any set of columns during Prep and
later phases, using a bitmap (FastIntSet). The bitmap representation allows
fast determination of compatibility between expression nodes and is utilized by
transforms to determine the legality of such operations.

The Prep phase also computes logical properties, such as the input and output
columns of each (sub-)expression, equivalent columns, not-null columns and
functional dependencies. These properties are computed bottom-up as part of
constructing the expression tree.

Rewrite

Rewrite is the second phase of query optimization. Placeholder values are
available starting at this phase, so new normalization rules will typically
match once constant values are substituted for placeholders. As mentioned in
the previous section, the expression tree produced by the Prep phase can be
cached and serve as the starting point for the Rewrite phase. In addition, the
Rewrite phase takes a set of physical properties that are required from the
result, such as row ordering and column naming.

The Rewrite and Search phases have significant overlap. Both phases perform
transformations on the expression tree. However, Search preserves the matched
expression side-by-side with the new expression, while Rewrite simply discards
the matched expression, since the new expression is assumed to always be
better. In addition, the application of exploration rules may trigger
additional normalization rules, which may in turn trigger additional
exploration rules.

Together, the Rewrite and Search phases are responsible for finding the
expression that can provide the required set of physical properties at the
lowest possible execution cost. That mandate is recursively applied; in other
words, each subtree is also optimized with respect to a set of physical
properties required by its parent, and the goal is to find the lowest cost
equivalent expression. An example of an "interior" optimization goal is a merge
join that requires its inner child to return its rows in a specific order. The
same group can be (and sometimes is) optimized multiple times, but with
different required properties each time.

Search

Search is the final phase of optimization. Search begins with a single
normalized tree that was created by the earlier phases. For each group, the
"explorer" component generates alternative expressions that are logically
equivalent to the normalized expression, but which may have very different
execution plans. The "coster" component computes the estimated cost for each
alternate expression. The optimizer remembers the "best expression" for each
group, for each set of physical properties required of that group.

Optimization of a group proceeds in two phases:

1. Compute the cost of any previously generated expressions. That set initially
contains only the group's normalized expression, but exploration may yield
additional expressions. Costing a parent expression requires that the children
first be costed, so costing triggers a recursive traversal of the memo groups.

2. Invoke the explorer to generate new equivalent expressions for the group.
Those new expressions are costed once the optimizer loops back to the first
phase.

In order to avoid a combinatorial explosion in the number of expression trees,
the optimizer utilizes the memo structure. Due to the large number of possible
plans for some queries, the optimizer cannot always explore all of them.
Therefore, it proceeds in multiple iterative "passes", until either it hits
some configured time or resource limit, or until an exhaustive search is
complete. As long as the search is allowed to complete, the best plan will be
found, just as in Volcano and Cascades.

The optimizer uses several techniques to maximize the chance that it finds the
best plan early on:

- As with Cascades, the search is highly directed, interleaving exploration
with costing in order to prune parts of the tree that cannot yield a better
plan. This contrasts with Volcano, which first generates all possible plans in
one global phase (exploration), and then determines the lowest cost plan in
another global phase (costing).

- The optimizer uses a simple hill climbing heuristic to make greedy progress
towards the best plan. During a given pass, the optimizer visits each group and
performs costing and exploration for that group. As long as doing that yields a
lower cost expression for the group, the optimizer will repeat those steps.
This finds a local maxima for each group during the current pass.

In order to avoid costing or exploring parts of the search space that cannot
yield a better plan, the optimizer performs aggressive "branch and bound
pruning". Each group expression is optimized with respect to a "budget"
parameter. As soon as this budget is exceeded, optimization of that expression
terminates. It's not uncommon for large sections of the search space to never
be costed or explored due to this pruning. Example:

	innerJoin
		left:  cost = 50
		right: cost = 75
		on:    cost = 25

If the current best expression for the group has a cost of 100, then the
optimizer does not need to cost or explore the "on" child of the join, and
does not need to cost the join itself. This is because the combined cost of
the left and right children already exceeds 100.
*/
package opt
