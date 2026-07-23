// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rel provides mechanisms to model and query go structs pointers
// using a declarative, relational paradigm.
//
// The package provides a means to map a struct fields to a set of Attrs
// to form a Schema. This schema can be used to construct a database which may
// index these entities (struct pointers) by those attributes. These entities
// can be queried using a declarative query language exposed by the library.
//
// # Why rel
//
// The key motivations end up being explainability, maintainability, and
// observability. There's a runtime consideration we'll get to later, but
// a driving motivation here is the ability to present domain complexity
// outside the considerations of runtime performance and imperative code.
//
// # Uniformity dealing with heterogeneous data
//
// The schema is fundamentally full of heterogeneous data with a number of cross
// references. Having a uniform mechanism to describe and interact with this
// complexity will yield benefits over time as we need to migrate and support
// different versions with different representations of these structures.
//
// Go lacks both generic and pattern-matching which is demanded by this style
// of program. In the specific context of the schemachanger, the dependency
// rules define much of the complexity of the problem domain. They are hard
// to get right and hard to reason about.
//
// Design Goals
//
//   - Observable: The library's rules should be trivially serialized in
//     an easy to consume format.
//   - Declarative: The library should separate the logic of the domain from
//     the process of the evaluation.
//   - Ergonomic: The library should feel comfortable to use for go programmers
//     looking to model relational graph problems for which it was intended.
//   - Reasonable efficiency: The library should provide mechanisms to index
//     data to accelerate queries out of band such that the big-O runtime can
//     be made sane and the constant overheads aren't too many orders of
//     magnitude off of specialized imperative code as to make its use
//     unacceptable.
//
// # Terminology
//
// The basic design of the library is that we want to index and find tuples of
// struct pointers, which are termed entities. These entities have attribute
// values which can be queries and compared. Readers familiar with RDF triples
// should feel comfortable with these concepts.
//
// The library allows users to craft Queries which bind Vars to values. A query
// is composed of a conjunction of Clauses, each of which corresponds to facts
// which, during execution, will be unified such that for all results there is
// a valid binding for all Vars in the query. Some Vars are correspond
// explicitly to entities. The query model is to iterate the database such that
// all variables are bound and all entity variables are independently iterated.
//
// A Query is used to iterate a Database which store entities. A Database may
// be constructed
//
// Before we can construct a database, we need to define a mapping from entity
// type fields to attributes. The NewSchema constructor will infer types based
// on the fields which carry the given attributes. The user of the library will
// define the set of attributes and how they map to fields. There are some
// useful features already in play like the ability to have multiple fields
// correspond to the same attribute as an implicit one-of; the library enforces
// that there is at most one non-nil value populated for that attribute.
//
// There are a couple of builtin attributes which all entities carry: Self and
// Type.
//
// Internally, we can think of a database as being a set of facts which is just
// the set of these (Entity, Attr, Value) 3-tuples. The query language
// provides a mechanism to iterate all assignments of entities named by
// variables in the query such that all of the constraints of the query are
// upheld. The language is inspired by but is a good deal simpler (and probably
// less elegant) than datomic. The language does not permit any recursion or
// runtime creation of facts.
//
// # Query Language
//
// The query language provides a mechanism to reason relationally about data
// stored in regular structs which may themselves have hierarchy between them.
// The structure of the query language is motivated by datomic which is itself
// motivated by datalog. However, the implementation requirements are simpler
// than datomic. We don't need durability and we know that we're embedded in a
// running program. The language is not a true datalog: it does not really
// have the notion of a rule and it certainly doesn't have a means to express
// recursion during the execution of queries. This means that the queries can
// only represent fixed depth joins between relations. Of course, users of
// libraries can generate queries of an arbitrary depth. Furthermore, users
// can implement their own forms of recursion.
//
// # Runtime considerations
//
// An early primary motivation for this package was the relatively
// straightforward problem of determining the set of dependency edges which
// need to exist in the graph for scplan.
//
// Imagine we have k queries we're going to write, one for each rule we want
// and that we have N elements and that we each query has a depth of d. A
// naive implementation will be O(k*N*d) as we filter the cross product.
// However, queries generally let you filter the to something much smaller than
// N as you go down in depth, usually the query is easy to perfectly constrain
// if you do it based on just a couple of attributes like descriptor ID or
// types.
//
// For example, imagine we have a rule to make all types in some status depend on
// tables which have columns or expressions which use that type. We might want
// to write a query that says: for all types, for all type references that use
// that type, for all tables which are a part of that type reference, add an
// edge. This is a depth 3 query. Now imagine we do a drop cascade such that we
// drop 1000 tables. If we had zero indexing structures, we'd need to scan all
// of the elements just to find the types and then again to find the references
// and again to find the tables. Now, of course, we could make per type hash
// maps or something like that. That would be even more efficient, but less
// expressive and much more verbose.
//
// At the end of the day, for each statement we're going to evaluate some
// constant number of clauses, each of which is going to apply to some subset of
// the elements and is going to need to explore some other constrained subset of
// the elements. If we assume that our queries are generally depth 2 (i.e. just
// direct references), then maybe this isn't so bad, it'd mean that we'd do at
// most N^2 work for each statement. However, it gets worse when you think about
// transactions which contain many statements (think big migrations). In that
// case, we'd have to do N^2 work potentially N times (N^3). N^3 is starting to
// get bad even if each step only takes a microsecond. Again, all of this could
// be defeated with maps on the right attributes. However, I had a hard time
// coming up with a nice way to reason about writing clauses declaratively
// which allows us to utilizing map-based indexing structures, at least,
// without generating code and writing a DSL. This library seems less complex
// than code generation and hopefully quite a bit more flexible.
//
// To demonstrate the importance of this, see the benchmarks that created
// lists+depth = N entities which are tuples (id, next) and the data just points
// to some other element element. Then define consider the problem of querying
// for the start of a list of some finite depth given the end. We benchmark how
// long it takes to execute each individual query. We see that the runtime of the
// indexed version runs in O(depth*log(N)) whereas the unindexed version runs in
// O(depth*N). We also see that around 8k entries, this indexed approach, at time
// of writing is faster than the much simpler O(N^2) looping over the list of
// nodes.
//
// What this ultimately means is that we can execute queries defined with proper
// indexes in O(N*log(N)) per statement meaning at worst O(N^2 log(N)) which is
// acceptable for an N of ~1000 as opposed to O(N^3) which isn't really.
//
// # Slices
//
// Rel supports inverted indexes over slices. In order to use them, you create
// an attribute referencing a slice. Internally, rel will create a new
// element internally for each member. The containment operator can be used
// to perform an inverted lookup.
//
// # Future work
//
// Below find a listing of features not yet done.
//
//   - It would be nice to have a mechanism to talk about decomposing
//     data stored in these collections. One approach would be to define
//     some more system attributes and some exported structs which act to
//     bind a slice member to the entity holding the slice. Consider:
//
//   - More ergonomic iteration with reflection.
//
//   - The current Result interface requires type assertions.
//     Given we know about the types of the variables, we could
//     accept a function with an appropriate signature and use
//     reflection to make sure that the corresponding variables
//     are properly typed.
//
//   - Variable bindings.
//
//   - If we wanted to make recursion more sane, it'd be better to plan a
//     query with some input parameters and then be able to invoke it on those
//     parameters. In that way, we could imagine invoking a query recursively.
//
//   - More generalized disjunction.
//
// TODO(ajwerner): Note that arrays of bytes can probably be used as slice but
// that would probably be unfortunate. We'd probably prefer to shove them into
// a string using some unsafe magic.
package rel
