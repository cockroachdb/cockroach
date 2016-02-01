:Feature Name: SQL typing
:Status: draft
:Authors: Andrei, knz, Nathan
:Start date: 2016-01-29
:RFC PR: TBD
:Cockroach Issue: #4024, #4026, #3633

Overview
========

We need a better typing system.

Why: some things currently do not work that should really work. Some
other things behave incongruously and are difficult to understand, and
this runs counter to our design ideal to "make data easy".

How: let's look at a few examples, understand what goes Really Wrong,
propose some reasonable expected behavior(s) and see how to get there.

What typing is about
====================

There are 4 different roles for typing in SQL:

1. **soundness analysis**, the most important is shared with other
   languages: check that the code is semantically sound -- that the
   operations given are actually computable. Typing tells you
   e.g. that ``3 + "foo"`` does not make sense.

2. **implementation selection** deciding what meaning to give
   to overloaded constructs in the language. For example some
   operators behave differently when given ``int`` or ``float``
   arguments (+, - etc) and there are also overloaded functions
   (``length`` is different for ``text`` and ``bytes``). This is also a
   feature shared with other languages, when overloading exists.

3. **inferring implicit conversions**, ie. determine where to insert
   implicit casts, when your flavor SQL supports this (this is like in
   a few other languages, like C).

4. **typing placeholders** inferring the type of
   placeholders (``$1``..., sometimes also noted ``?``), because the
   client needs to know this after a ``prepare`` and before an
   ``execute``.

What we see in CockroachDB at this time, as well as in some other SQL
products, is that SQL engines have issues for all 4 aspects.

There are often applicable reasons why this is so, for example
1) lack of specification of the SQL language itself 2) lack of
interest for this issue 3) organic growth of the machinery and 4)
general developer ignorance about typing.

Problems considered
===================

This RFC considers specifically the following issues:

- expressions involving only untyped literals
- expressions involving only untyped literals and placeholders
- overload resolution in calls with untyped literals or placeholders as arguments

The following issues are related to typing but fall outside of the scope of t his RFC:
  
- prepare reports type X to client, client does not *know* X (and thus
  unable to send the proper format byte in subsequent execute)

  This issue can be addressed by extending/completing the client
  Postgres driver.

- program/client uses a string literal in a position of another type,
  expects a coercion like in pg.

  For this issue one can argue the client is wrong; this issue may be
  addressed at a later stage if real-world use shows that demand for
  legacy compatibility here is real.
  
- prepare reports type "int" to client, client feeds "string" during execute

  Same as previous point.

Examples that go wrong (arguably)
=================================

It's rather difficult to find examples where soundness goes wrong
because people tend to care about this most. That said, it is
reasonably easy to find example SQL code that really seems to make
sense, but which engines reject as being unsound. For example::

    prepare a as select 3 + case (4) when 4 then $1 end

this fails in Postgres because ``$1`` is typed as ``text`` always and
you can't add text to int (this is a soundness error). What we'd
rather want is to infer ``$1`` either as ``int`` (or numeric) and let
the operation succeed, or fail with a type inference error ("can't
decide the type"). In CockroachDB this does not even compile, there is
no inference available within ``CASE``.

Next to this, there are a number of situations where existing engines
have chosen a behavior that makes the implementation of the engine
easy, but may irk / surprise the SQL user. And Surprise is Bad.

For example:

1) inconsistent results across engines, non-intuitive results.

   For example::

        create table t (x float);
	insert into t(x) values (3 / 2)

   This inserts 1 in Postgres (this is slightly surprising) and 1.5 in
   CockroachDB (this looks and feels OK). However 
   if the example is turned around, we get a result that looks
   strange and invalid in CockroachDB::

        create table u (x int);
	insert into u(x) values (((9 / 3) * (1 / 3))::int)

   This populates ``u`` with 0 in Postgres (as it should) but 1 in
   CockroachDB (surprise! there is no interpretation of the expression
   with integer arithmetic that can yield 1 as a result).

   Of course here the case can be made that the two engines differ on
   their semantics for division, nevertheless a case can be made that
   the Postgres behavior looks more homogeneous / predictable /
   symmetrical (no arithmetic operator has a special typing there, unlike
   CockroachDB's division).

   (Arguably this specific example is more a concern about the
   definition of the arithmetic division and not a typing issue.)

2) pessimistic typing for numeric literals.

   For example::

      create tabe t (x float);
      insert into t(x) values (1e10000 * 1e-9999);

   This fails on both Postgres and CockroachDB with a complaint that
   the numbers do not fit in either int or float, despite the fact the
   result would.

3) incorrect typing for literals.

   For example::

      select length(E'\\000a'::bytea || 'b'::text)

   Succeeds (wrongly!) in Postgres and reports 6 as result.  This
   should have failed with either "cannot concatenate bytea and text",
   or created a bytearray of 3 bytes (\x00ab), or a string with a
   single character (b), or a 0-sized string.

   (CockroachDB does not yet support byte arrays)

4) engine throws hands up in the air and abandons something that could
   otherwise look perfectly fine::

       select floor($1 + $2)

   This fails in Postgres with "can't infer the types" whereas the
   context suggests that inferring ``numeric`` would be perfectly
   fine.

5) failure to use context information to infer types where this
   information is available.

   To simplify the explanation let's construct a simple example by
   hand. Consider a library containing the following functions::

        f(int) -> int
	f(float) -> float
	g(int) -> int

   Then consider the following statement::

        prepare a as select g(f($1))

   This fails with ambiguous/untypable $1, whereas one could argue (as
   is implemented in other languages) that ``g`` asking for ``int`` is
   sufficient to select the 1st overload for ``f`` and thus fully
   determine the type of $1.
   

Things that look wrong but really aren't
========================================

1) loss of equivalence between prepared and direct statements::

     prepare a as select ($1 + 2)
     execute a(1.5)

     # reports 3 (in Postgres)

   The issue here is that the + operator is overloaded, and the
   engine performs typing on $1 only considering the 2nd operand to
   the +, and not the fact that $1 may have a richer type.

   One may argue that a typing algorithm that only performs "locally"
   is sufficient, and that this statement can be reliabily understood
   to perform an integer operation in all cases, with a forced cast of
   the value filled in the placeholder. The problem with this argument
   is that this interpretation loses the equivalence between a direct
   statement and a prepared statement, that is, the substitution of::

      select 1.5 + 2

   is not equivalent to::

      prepare a as select $1 + 2; execute a(1.5)

   The real issue however is that SQL's typing is essentially
   monomorphic and that prepare statements are evaluated independently
   of subsequent queries: there is simply no SQL type that can be
   inferred for the placeholder in a way that provides sensible
   behavior for all subsequent queries. And introducing
   polymorphic types (or type families) just for this purpose
   doesn't seem sufficiently justified, since an easy workaround is available::

     prepare a as select $1::float + 2;
     execute a(1.5)
   
   


Pitfalls
========

Postgres uses casts as a way to indicate type hints on
placeholders. Note that this is not intuitive, because
a user may legitimately want to
use a value of a given type in a context where another type is needed,
without restricting the type of the placeholder. For example::

  create table t (x int, s text);
  insert into t (x, s)  values ($1, "hello " + $1::text)

Here intuition says we want this to infer "int" for $1, not get a type error.

If we use casts as type hints, this needs to be properly documented, so that
the user wanting to express the exampe above is guided to write instead::

  create table t (x int, s text);
  insert into t (x, s)  values ($1::int, "hello " + ($1::int)::text)

(Possible notice in documentation: "if you intend to cast, explain
the intended source type of your placeholder inside your cast first")

Strategy
========

We use the following notations below::

   E :: T  => the regular SQL cast, equivalent to CAST(E as T)
   E [T]   => a AST node representing E with an annotation that indicates it has type T

Each concrete SQL type belongs to one category or "kind". The Kind
must be annotated alongside the type in nodes, because the kind may be
determined for a node before its type. (We can also say that "the
unknown type belongs to all kinds").

======== =================
Type     Kind
======== =================
numeric  Number-like (N)
float    Number-like (N)
int      Number-like (N)
text     String-like (S)
varchar  String-like (S)
bytea    String-like (S)
bool     Boolean (B)
======== =================
    
We also thus denote::

   E [T]      E has type T specifically
   E [*K]     E has an unknown type in category K


We assume that an initial/earlier phase has performed
the reduction of casted placeholders (but only placeholders!), that is, folding::

     $1::T      => $1[T]
     x::T       => x :: T  (for any x that is not a placeholder)

     $1::T :: U => $1[T] :: U

Then we type using the following types

A. Constant folding.

   This reduces complex expressions without losing information (like
   in Go!). Literal constants are evaluated using either their type if
   intrinsically known (for unambiguous literals like true/false), or
   an internal exact implementation type for ambiguous literals. This
   is performed for all expressions involving only untyped literals
   and functions applications applied only to such expressions.
   
   Which exact types are used:
   - for literals that look like numbers, the type from the ... library
   - for literals that look like strings, use bytea internally
   
   While the constant expressions are folded, the results must be
   typed using either the intrinsic type if all operands had one; or
   the unknown type for a specific kind when the operands did not have
   a single intrinsic type.
   
   For example::
   
     true and false               => false[bool]
     'a' + 'b'                    => "ab"[*S]
     E'a\\000' + 'b'              => "a\0b"[*S]
     12 + 3.5                     => 15.5[*N]
     case 1 when 1 then x         => x[?]
     case 1 when 1 then 2         => 2[*N]
     3 + case 1 when 1 then 2     => 5[*N]
     abs(-2)                      => 2[*N]
     abs(-2e10000)                => 2e10000[*N]

   Note that folding does not take place for functions/operators that are overloaded
   and when the operands have different types (we resolve type coercions at a later phase)::

     23 + 'abc'                   => 23[*N] + 'abc'[*S]
     23 + sin(23)                 => 23[*N] + -0.8462204041751706[float]

   Folding does "as much work as possible", for example::

     case x when 1 + 2 then 3 - 4 => (case x[?] when 3[*N] then -1[*N])[*N]

   Note that casts select a specific type, but may stop the fold because the surrounding
   operation becomes applied to different types::

     true::bool and false         => false[bool] (both operands of "and" are bool)
     1::int + 23                  => 1[int] + 23[*N]
     (2 + 3)::int + 23            => 5[int] + 23[*N]

   The optimization for functions only takes place for a limited subset
   of supported functions, they need to be pure and have an
   implementation for the exact type.

B. Culling and candidate type collection.

   This phase collects candidate types for AST nodes, does a
   pre-selection of candidates for overloaded calls and computes
   intersections.

   This is a depth-first, post-order traversal. At every node:
   
   i.   the candidate types of the children are computed first
   
   ii.  the current node is looked at, some candidate overloads may be filtered out
   
   iii. in case of call to an overloaded op/fun, the argument types are used to restrict the candidate set
        of the direct child nodes (set intersection).
	
   iv.  if the steps above determine more than 1 possible type for a node, and that node
        is neither a constant nor a placeholder, typing fails as ambiguous. If the step determines
	there are no possible types for a node, fail as a typing error.

        (Note: this is probably a point where we can look at implicit coercions)

   For this step we expand all the "unknown type in kind K" notations into the actual
   set of possible types in that kind.

   Simple example::

      5[int] + 23[*N]

   This filters the candidates for + to only the one taking int and int (rule ii).  Then by rule iii
   the annotation on  23 is changed, and we obtain::

      ( 5[int] + 23[int] )[int]
      
   Another example::

     'abc' + $1

   In this expression constant folding/typing has given us type [text,bytea]
   (all types in kind S) for the literal 'abc' and "unknown" (any
   type) for $1.

   The addition has has many overloads, but the 1st argument's candidate types ([text,bytea])
   restricts the overload to those candidates (rule ii)::

         text x text -> text
	 bytea x bytea -> bytea

   Given this information (restriction of the overload) we change the
   type annotation on the 2nd argument to intersect with the possible
   types at that location::
     
         'abc'[text,bytea] + $1[text,bytea]

   And given these arguments, we resolve the set of possible types
   for the addition as a whole::

         ('abc'[text,bytea] + $1[text,bytea] )[text,bytea]

   Another example::
   
       f:int->int
       f:float->float
       f:text->text
       (12 + $1) + f($1)

   We type as follows::

       (12[*N] + $1) + f($1)
          .
	  
       (12[*N] + $1[*N]) + f($1[*N])
                   .
                   Note that the placeholders in the AST share
		   their type annotation between all their occurrences
		   (this is unique to them, e.g. literals have
		   separate type annotations)

       (12[*N] + $1[*N])[*N] + f($1[*N])
                        .

       (12[*N] + $1[*N])[*N] + f($1[*N])
                                 .
				 (nothing to do anymore)

       (12[*N] + $1[*N])[*N] + f($1[*N])
                               .

    At this point, we are looking at ``f($1[int,float,numeric,...])``.
    Yet f is only overloaded for int and float, therefore, we restrict
    the set of candidates to those allowed by the type of $1 at that point,
    and that reduces us to::

        f:int->int
	f:float->float
   
    And the typing continues, restricting the type of $1::

       (12[*N] + $1[int,float])[*N] + f($1[int,float])
                                      .

       (12[*N] + $1[int,float])[*N] + f($1[int,float])[int,float]
                                      .

       (12[*N] + $1[int,float])[*N] + f($1[int,float])[int,float]
                                    .

    Aha! Now the plus sees an operand on the right more restricted than the one on the left,
    so it filters out all the unapplicable candidates, and only the following are left over::

       +: int,int->int
       +: float,float->float

    And thus this phase completes with::

       ((12[*N] + $1[int,float])[int,float] + f($1[int,float])[int,float])[int,float]
                                            .

    Notice how the restrictions only apply to the direct children
    nodes when there is a call and not pushed further down (e.g. to
    ``12[*N]`` in this example).

C. Repeat B as long as there is at least one candidate set with more
   than 1 type, and until the candidate sets do not evolve any more.

   This simplifies the example above to::

     ((12[int,float] + $1[int,float])[int,float] + f($1[int,float])[int,float])[int,float]
     
D. Refine the type of constants. 

   This is a depth-first, post-order traversal.

   For every constant with more than one type in its candidate type
   set, pick the best type that can represent the constant.

   - for numeric types, we use the order int, float, numeric
   - for strings and bytea, we use string if possible (no nul byte nor
     invalid unicode encoding), otherwise bytea

   For example::

     12[int,float] + $1[int,float] => 12[int] + $1[int, float]


   The reason why we consider constants here (and not placeholders) is that
   the programmers express an intent about typing in the form of their literals.
   That is, there is a special meaning expressed by writing "2.0" instead of "2".

E. Run B-C again. This will refine the type of placeholders
   automatically.

F. If there is any remaining candidate type set with more than one
   candidate, fail with ambiguous.


Revisiting the examples from earlier with this strategy
=======================================================

::

    prepare a as select 3 + case (4) when 4 then $1 end
                        3[*N] + $1[?]     A
                        3[*N] + $1[*N]    B
                        3[int] + $1[*N]   D
			3[int] + $1[int]  B

    OK
 
    prepare a as select ($1 + 2)
                         $1[*N] + 2[*N]   B
			 $1[*N] + 2[int]  D
			 $1[int] + 2[int] B
    execute a(1.5)
    (Casualty, but recoverable by explicit type hints in the prepare
    statement)


    select floor($1 + $2)
                 $1[*N] + $2[*N]  B
    => failure
    (Casualty, can't push demanded types yet)


    f(int) -> int
    f(float) -> float
    g(int) -> int
    prepare a as select g(f($1))
                            $1[int,float]  B
    => failure
    (Casualty, can't push demanded types yet)

    create table t (x decimal);
    insert into t(x) values (3/2)
                             (3/2)[*N]        A
			     (3/2)[decimal]   B

    OK
    
    create table u (x int);
    insert into u(x) values (((9 / 3) * (1 / 3))::int)
                               3 * (1/3)::int   A
                               1::int           A
			       1[int]           A
			       
    OK

    create tabe t (x float);
    insert into t(x) values (1e10000 * 1e-9999)
                             10[*N]    A
                             10[float] B
			     
    OK

    select length(E'\\000' + 'a'::bytea)
                  E'\\000'[*S] + 'a'[bytea]  
		  E'\\000'[bytea] + 'a'[bytea]  B
		 
    OK

    select length(E'\\000a'::bytea || 'b'::text)
                  E'\\000a'[bytea] || 'b'[text]
		  then failure, no overload for || found
		  
    OK		  

    f:int,float->int
    f:string,string->int
    g:float,numeric->int
    g:string,string->int
    h:numeric,float->int
    h:string,string->int
    prepare a as select  f($1,$2) + g($2,$3) + h($3,$1)
              f($1[int,text],$2[float,text]) + ....
	      .
	      f(...)+g($2[float,text],$3[numeric,text]) + ...
	                                .
              f(...)+g(...)+h($3[numeric,text],$1[text])
	                                         .

              (B re-iterates)

              f($1[text],$2[text]) + ...
	                   .    
	      f(...)+g($2[text],$3[text]) + ...
	                          .
              f(...)+g(...)+h($3[text],$1[text])
	                                 .

              (B stops, all types have been resolved)

     => $1, $2, $3 must be texts
     

     select (3 + $1) + ($1 + 3.5)
             3[*N] + $1[*N] + $1[*N] + 3.5[*N]       B
             3[int] + $1[*N] + $1[*N] + 3.5[float]   D
             3[int] + $1[int] + ...                  B
                      .
             3[int] + $1[int] + $1[int] + 3.5[float] B
		                       .  failure, unknown overload

     (Casualty? Postgres infers numeric)

