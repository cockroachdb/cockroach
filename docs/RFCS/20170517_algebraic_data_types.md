-   Feature Name: Go infrastructure for algebraic data types
-   Status: in-progress
-   Start Date: 2017-05-31
-   Authors: David Eisenstat <eisen@cockroachlabs.com>, with much input
    from Raphael Poss <knz@cockroachlabs.com> and Peter Mattis
    <peter@cockroachlabs.com>. All errors and omissions are my own.
-   RFC PR: [\#16240]

Summary: this RFC explores the implementation of algebraic data types in
Go. So far, we have a proposal for a low-level interface and some
candidate implementations.

Note: this document is a literate Go program. To extract the Go samples
from this document (for, e.g., comparing the assembly output):

``` {.shell}
sed -ne '/^```.*go/,/^```/{s/^```.*//
p
}' algebraic_data_types.md >algebraic_data_types.go
```

Here is the program header.

``` {.go}
package main

import (
    "fmt"
    "strconv"
    "unsafe"
)
```

Context
-------

The current pipeline for executing a SQL statement is:

1.  Parse (SQL text to abstract syntax tree (AST)),
2.  Resolve, type check (AST to annotated AST),
3.  Simplify (annotated AST to annotated AST),
4.  Plan, optimize (annotated AST to logical plan),
5.  Distribute, run, transform (logical plan to result data)

In compiler terms, phases 1 and 2 comprise the front end, phases 3 and 4
comprise the middle end, and phase 5 comprises the back end.

At present, ASTs can be serialized to SQL text only, which is expensive
and error prone. Middle end transformations operate on ASTs and must
either handle every syntactic construct or be carefully sequenced.

A [separate RFC] proposes that we introduce an intermediate
representation (IR) of SQL to address these problems. This RFC is about
creating Go infrastructure for algebraic data types and pattern matching
(as in, e.g., ML, Haskell, and Rust) with an eye toward supporting an
IR.

Why algebraic data types? They have a proven track record in compiler
implementation, and compilers written in languages that lack pattern
matching often implement a comparable facility (e.g., [Clang]).

High-level interface aspirations
--------------------------------

-   Make it easy to write (what are essentially) compiler passes.
-   Convert existing Go code incrementally.
-   Generate boilerplate:
    -   Tree walks,
    -   Serialization code for protocol buffer messages, and
    -   Formatting code (to SQL text).

High-level requirements
-----------------------

-   Reduced allocation overhead via bulk allocation for ADT nodes
-   Reasonable compute overhead
-   No unsafe storage of pointers (garbage collection would be unsafe)
-   Memory usage tracking for SQL queries
-   Reasonable expressiveness for programmers

Proposed design
---------------

We propose a layered approach. The bottom layer provides basic
operations on algebraic data types: allocation, access, and mutation.
The higher layers provide tree walks, serialization, formatting, and
more complex mutation patterns that recur often in CockroachDB.

We propose further to write a code generator. The input describes the
algebraic data types that CockroachDB needs. The output is Go code that
provides the different API layers. The system will also provide support
for pattern matching. TODO(eisen): How?

Functions that create new ADT references will need to take the allocator
as an explicit argument. To partially address this need, we will add an
allocator field to existing contexts as appropriate.

ADT nodes will be serializable to and deserializable from protocol
buffer messages. We will use `gogoproto` if we can, but it would not be
hard to generate our own serialization and deserialization code.
Abstract syntax tree (AST) nodes will be stored on disk, and we can
write database migrations on the rare occasions that a
backward-incompatible change is necessary. DistSQL will need to send
intermediate representation (IR) nodes over the network, but we can use
DistSQL version numbers to ensure interoperability. (AST nodes are a
subset of all of the IR nodes, and this subset will be more stable.)

Detailed interface design for the bottom layer
----------------------------------------------

The generated code defines a Go value type and reference type for each
algebraic data type. These Go types define several methods: access,
mutate, walk, serialize, and format. To allow safe aliasing, the
reference type provides no mutators (though there will be a hole for
mutable types). Instead, there are methods to dump a reference to a
value (`.V()`) and to allocate a new reference from a value (`.R()`).
The value types have fluent update methods to allow mutation in an
expression context.

To prevent unexpected allocations, a linter detects Go code that takes
the address of a value type.

### Example

The input to the code generator is:

``` {.adt}
sum Expr {
  ConstExpr = 1
  BinExpr = 2
}

struct ConstExpr {
  int64 Datum = 1
}

struct BinExpr {
  Expr Left = 1
  BinOp Op = 2
  Expr Right = 3
}

enum BinOp {
  Add = 1
  Mul = 2
}
```

Here is an example of manually walking an expression tree to reverse all
binary expressions. In production code, we would use an automatically
generated walker instead (details to be decided later).

``` {.go}
func Reverse(ref Expr, a Allocator) Expr {
    if ref.Tag() != ExprBinExpr {
        return ref
    }
    b := ref.MustBeBinExpr()
    rl := Reverse(b.Left(), a)
    rr := Reverse(b.Right(), a)
    return b.V().WithLeft(rr).WithRight(rl).R(a).Expr()
}
```

Here are more examples. `Format` and `DeepEqual` would be automatically
generated too (details to decided later).

``` {.go}
func Format(ref Expr) string {
    switch ref.Tag() {
    case ExprConstExpr:
        c := ref.MustBeConstExpr()
        return strconv.FormatInt(c.Datum(), 10)
    case ExprBinExpr:
        b := ref.MustBeBinExpr()
        var op string
        switch b.Op() {
        case BinOpAdd:
            op = "+"
        case BinOpMul:
            op = "*"
        default:
            panic("unknown BinOp")
        }
        return fmt.Sprintf("(%s %s %s)", Format(b.Left()), op, Format(b.Right()))
    default:
        panic("unknown Expr tag")
    }
}

func DeepEqual(ref1 Expr, ref2 Expr) bool {
    if ref1.Tag() != ref2.Tag() {
        return false
    }
    switch ref1.Tag() {
    case ExprConstExpr:
        return ref1.MustBeConstExpr().Datum() == ref2.MustBeConstExpr().Datum()
    case ExprBinExpr:
        b1 := ref1.MustBeBinExpr()
        b2 := ref2.MustBeBinExpr()
        return b1.Op() == b2.Op() &&
            DeepEqual(b1.Left(), b2.Left()) && DeepEqual(b1.Right(), b2.Right())
    default:
        panic("unknown Expr tag")
    }
}

func main() {
    a := NewAllocator()
    c1 := ConstExprValue{1}.R(a).Expr()
    c2 := ConstExprValue{2}.R(a).Expr()
    c3 := ConstExprValue{3}.R(a).Expr()
    b4 := BinExprValue{c1, BinOpAdd, c2}.R(a).Expr()
    b5 := BinExprValue{c3, BinOpMul, b4}.R(a).Expr()
    println(Format(b5))
    e6 := Reverse(b5, a)
    println(Format(e6))
    println(DeepEqual(e6, b5))
    e7 := Reverse(e6, a)
    println(Format(e7))
    println(DeepEqual(e7, b5))
}
```

Implementation designs for the bottom layer
-------------------------------------------

tl;dr: we’re not really ready to discuss implementation details yet.

We have a chicken and egg problem: we do not have enough information to
commit to an implementation right now, yet we have to build something to
get that information. Below are some of the alternatives that we have
considered, in case there are implications for the interface design. Our
current plan is to implement whatever seems easiest and then revisit
later.

We use a small definition to explore alternative designs.

``` {.adt}
sum ListUint64 {
  Empty = 1
  Pair = 2
}

struct Empty {
}

struct Pair {
  uint64 Head = 1
  ListUint64 Tail = 2
}
```

For each design, we give a simplification of the Go type definition for
`Pair`. We also give its `.R()` method, because that method may have
unexpected compute overhead.

### One Go type per ADT node type

Pro: conventional. Con: allocation overhead – we must either allocate
numerous slices or rely on Go’s allocator.

``` {.go}
type ListA interface {
    isListA()
}

type PairA struct {
    Head uint64
    Tail ListA
}

func (*PairA) isListA() {}

var _ ListA = &PairA{}

func (x PairA) R(ref *PairA) *PairA {
    ref.Head = x.Head
    ref.Tail = x.Tail
    return ref
}
```

### One common Go type for all ADT nodes with all possible attributes

Pro: bulk allocations. Con: space overhead (the current max size is
close to 200, while the min is 40, and each arbitrary node will be
larger than the current max).

``` {.go}
type arb struct {
    tag uint64
    a   uint64
    b   *arb
}

type ListRefB *arb

type PairRefB *arb

type PairB struct {
    Head uint64
    Tail ListRefB
}

func (x PairB) R(ref PairRefB) PairRefB {
    ref.tag = 2
    ref.a = x.Head
    ref.b = x.Tail
    return ref
}
```

### Small common Go type with slices and an interface for everything else

Pro: bulk allocations. Con: bounds checking overhead, space overhead
(six words per small node compared to the previous implementation).

``` {.go}
type arbRefC struct {
    refs []arbRefC
    vals []uint64
    arb  interface{}
}

type ListRefC struct {
    arbRefC
}

type PairRefC struct {
    arbRefC
}

type PairC struct {
    Head uint64
    Tail ListRefC
}

func (x PairC) R(ref PairRefC) PairRefC {
    ref.refs[0] = x.Tail.arbRefC
    ref.vals[0] = 2
    ref.vals[1] = x.Head
    return ref
}
```

### Unsafe storage

Writing our own allocator as in C is out of the question because Go
forbids us to store pointers in non-pointer fields and vice versa (the
Go garbage collector needs to distinguish the two).

Accordingly, we allocate a slice of `unsafe.Pointer`s and another slice
of `int64`s. We split ADT objects into pointer and non-pointer fields
and store each half contiguously in the appropriate slice. A reference
to an ADT object consists of a pointer to each half of the array.

Pro: efficient. Con: uses `unsafe`.

Here is the Go code for the proposal:

``` {.go}
type arbRef struct {
    ptrBase *unsafe.Pointer
    valBase *uint64
}

type ListRef struct {
    arbRef
}

type PairRef struct {
    arbRef
}

type Pair struct {
    Head uint64
    Tail ListRef
}

func (x Pair) R(ref arbRef) arbRef {
    ptrBase := (*[2]unsafe.Pointer)(unsafe.Pointer(ref.ptrBase))
    ptrBase[0] = unsafe.Pointer(x.Tail.ptrBase)
    ptrBase[1] = unsafe.Pointer(x.Tail.valBase)
    valBase := (*[2]uint64)(unsafe.Pointer(ref.valBase))
    valBase[0] = 2
    valBase[1] = x.Head
    return ref
}
```

### Cap’n Proto

See <https://github.com/zombiezen/go-capnproto2>.

Higher layers
-------------

TODO(eisen): the initial discussion should focus on the bottom layer.

Appendix: hand-translated code for unsafe implementation
--------------------------------------------------------

The output will be something like (hand translation):

``` {.go}
type Allocator struct {
    a *allocatorValue
}

func NewAllocator() Allocator {
    return Allocator{&allocatorValue{}}
}

type allocatorValue struct {
    ptrs []unsafe.Pointer
    vals []uint64
}

type arbitraryRef struct {
    ptrBase *unsafe.Pointer
    valBase *uint64
}

const bulkAllocationLen = 256

func (aptr Allocator) new(numPtrs, numVals int) arbitraryRef {
    a := aptr.a
    if numPtrs > cap(a.ptrs)-len(a.ptrs) {
        a.ptrs = make([]unsafe.Pointer, 0, bulkAllocationLen)
    }
    if numVals > cap(a.vals)-len(a.vals) {
        a.vals = make([]uint64, 0, bulkAllocationLen)
    }
    var ref arbitraryRef
    if numPtrs > 0 {
        a.ptrs = a.ptrs[:len(a.ptrs)+numPtrs]
        ref.ptrBase = &a.ptrs[len(a.ptrs)-numPtrs]
    }
    if numVals > 0 {
        a.vals = a.vals[:len(a.vals)+numVals]
        ref.valBase = &a.vals[len(a.vals)-numVals]
    }
    return ref
}

func (ref arbitraryRef) ptr(i uintptr) *unsafe.Pointer {
    return (*unsafe.Pointer)(unsafe.Pointer(uintptr(unsafe.Pointer(ref.ptrBase)) +
        i*unsafe.Sizeof(*ref.ptrBase)))
}

func (ref arbitraryRef) ptrs4() *[4]unsafe.Pointer {
    return (*[4]unsafe.Pointer)(unsafe.Pointer(ref.ptrBase))
}

func (ref arbitraryRef) val(i uintptr) *uint64 {
    return (*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(ref.valBase)) +
        i*unsafe.Sizeof(*ref.valBase)))
}

func (ref arbitraryRef) vals3() *[3]uint64 {
    return (*[3]uint64)(unsafe.Pointer(ref.valBase))
}

// ---- Expr ---- //

type ExprTag int

const (
    ExprConstExpr ExprTag = 1
    ExprBinExpr           = 2
)

type Expr struct {
    arbitraryRef
    tag ExprTag
}

func (ref Expr) Tag() ExprTag {
    return ref.tag
}

func (ref Expr) MustBeConstExpr() ConstExpr {
    if ref.tag != ExprConstExpr {
        panic("receiver is not a ConstExpr")
    }
    return ConstExpr{ref.arbitraryRef}
}

func (ref Expr) MustBeBinExpr() BinExpr {
    if ref.tag != ExprBinExpr {
        panic("receiver is not a BinExpr")
    }
    return BinExpr{ref.arbitraryRef}
}

func (ref ConstExpr) Expr() Expr {
    return Expr{ref.arbitraryRef, ExprConstExpr}
}

func (ref BinExpr) Expr() Expr {
    return Expr{ref.arbitraryRef, ExprBinExpr}
}

// ---- ConstExpr ---- //

type ConstExpr struct {
    arbitraryRef
}

func (ref ConstExpr) Datum() int64 {
    return *(*int64)(unsafe.Pointer(ref.val(0)))
}

// ConstExprValue is used for mutations.
type ConstExprValue struct {
    Datum int64
}

func (ref ConstExpr) V() ConstExprValue {
    return ConstExprValue{ref.Datum()}
}

func (x ConstExprValue) WithDatum(datum int64) ConstExprValue {
    x.Datum = datum
    return x
}

func (x ConstExprValue) R(a Allocator) ConstExpr {
    ref := a.new(0, 1)
    *(*int64)(unsafe.Pointer(ref.val(0))) = x.Datum
    return ConstExpr{ref}
}

// ---- BinExpr ---- //

type BinExpr struct {
    arbitraryRef
}

func (ref BinExpr) Left() Expr {
    return Expr{arbitraryRef{(*unsafe.Pointer)(*ref.ptr(0)), (*uint64)(*ref.ptr(1))},
        ExprTag(*ref.val(0))}
}

func (ref BinExpr) Op() BinOp {
    return BinOp(*ref.val(1))
}

func (ref BinExpr) Right() Expr {
    return Expr{arbitraryRef{(*unsafe.Pointer)(*ref.ptr(2)), (*uint64)(*ref.ptr(3))},
        ExprTag(*ref.val(2))}
}

// BinExprValue is used for mutations.
type BinExprValue struct {
    Left  Expr
    Op    BinOp
    Right Expr
}

func (ref BinExpr) V() BinExprValue {
    return BinExprValue{ref.Left(), ref.Op(), ref.Right()}
}

func (x BinExprValue) WithLeft(left Expr) BinExprValue {
    x.Left = left
    return x
}

func (x BinExprValue) WithOp(op BinOp) BinExprValue {
    x.Op = op
    return x
}

func (x BinExprValue) WithRight(right Expr) BinExprValue {
    x.Right = right
    return x
}

func (x BinExprValue) R(a Allocator) BinExpr {
    ref := a.new(4, 3)
    ptrs4 := ref.ptrs4()
    ptrs4[0] = unsafe.Pointer(x.Left.arbitraryRef.ptrBase)
    ptrs4[1] = unsafe.Pointer(x.Left.arbitraryRef.valBase)
    ptrs4[2] = unsafe.Pointer(x.Right.arbitraryRef.ptrBase)
    ptrs4[3] = unsafe.Pointer(x.Right.arbitraryRef.valBase)
    vals3 := ref.vals3()
    *(*ExprTag)(unsafe.Pointer(&vals3[0])) = x.Left.tag
    *(*BinOp)(unsafe.Pointer(&vals3[1])) = x.Op
    *(*ExprTag)(unsafe.Pointer(&vals3[2])) = x.Right.tag
    return BinExpr{ref}
}

// ---- BinOp ---- //

type BinOp int

const (
    BinOpAdd BinOp = 1
    BinOpMul       = 2
)
```

  [\#16240]: https://github.com/cockroachdb/cockroach/pull/16240
  [separate RFC]: https://github.com/cockroachdb/cockroach/pull/10055
  [Clang]: https://clang.llvm.org/docs/LibASTMatchers.html
