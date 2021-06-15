// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// FuncDepSet is a set of functional dependencies (FDs) that encode useful
// relationships between columns in a base or derived relation. Given two sets
// of columns A and B, a functional dependency A-->B holds if A fully determines
// B. In other words, if two different rows have equal values for columns in A,
// then those two rows will also have equal values for columns in B. For
// example, where columns (a1, a2) are in set A, and column (b1) is in set B:
//
//   a1 a2 b1
//   --------
//   1  2  5
//   1  2  5
//   3  4  6
//   3  4  6
//
// The left side of a functional dependency is called the "determinant", and
// the right side is called the "dependant". Each side can contain zero or more
// columns, though the FuncDepSet library will fold away certain combinations
// that don't provide useful information, like A-->A and A-->(), since every
// column trivially determines itself, as well as the empty set.
//
// When a dependant contains multiple columns, it is equivalent to splitting
// the single FD into multiple FDs, each with a single column dependant:
//
//   (a)-->(b,c)
//
// is equivalent to these two FDs:
//
//   (a)-->(b)
//   (a)-->(c)
//
// When a determinant contains zero columns, as in ()-->A, then A is fully
// determined without reference to any other columns. An equivalent statement is
// that any arbitrary combination of determinant columns trivially determines A.
// And both of these statements are just another way of saying that columns in A
// are constant:
//
//   a1 a2    b1 c1
//   ----------------
//   1  NULL  3  3
//   1  NULL  3  NULL
//   1  NULL  4  NULL
//
// When a determinant contains multiple columns, then the functional dependency
// holds for the *composite* value of those columns. For example:
//
//   a1 a2 b1
//   --------
//   1  2  5
//   1  2  5
//   1  3  4
//
// These are valid values, even though a1 has the same values for all three
// rows, because it is only the combination of (a1,a2) that determines (b1).
//
// Multiple FDs can be transitively applied in order to compute the "closure" of
// a set of input columns. The closure includes the input columns plus all
// columns that are functionally dependent on those columns, either directly or
// indirectly. Consider this set of FD's:
//
//   (a)-->(b,c,d)
//   (b,c,e)-->(f)
//   (d)-->(e)
//
// The transitive closure of (a) is (a,b,c,d,e,f). To start, (a) determines
// (b,c,d). From there, (d) transitively determines (e). And now that (b,c,e)
// have been determined, they in turn determine (f). Because (a) determines all
// other columns, if two rows have the same value for (a), then the rows will be
// duplicates, since all other columns will be equal. And if there are no
// duplicate rows, then (a) is a key for the relation.
//
// Deriving FD Sets
//
// Base table primary keys can be trivially mapped into an FD set, since the
// primary key always uniquely determines the other columns:
//
//   CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT)
//   (a)-->(b,c)
//
// Each SQL relational operator derives its own FD set from the FD sets of its
// inputs. For example, the Select operator augments the FD set of its input,
// based on its filter condition:
//
//   SELECT * FROM t WHERE a=1
//
// Equating a column to a constant value constructs a new FD with an empty
// determinant, so that the augmented FD set becomes:
//
//   (a)-->(b,c)
//   ()-->(a)
//
// Since the value of column "a" is always the same, and since "a" functionally
// determines "b" and "c", the values of all columns are constants. Furthermore,
// because "a" is known to be a key, the result set can have at most one row.
//
// This is but one example of how FDs can assist the optimizer in proving useful
// properties about query results. This information powers many optimizations,
// including eliminating unnecessary DISTINCT operators, simplifying ORDER BY
// columns, removing Max1Row operators, and mapping semi-joins to inner-joins.
//
// NULL Values
//
// FDs become more complex when the possibility of NULL values is introduced.
// SQL semantics often treat a NULL value as an "unknown" value that is not
// equal to any other value, including another NULL value. For example, SQL
// unique indexes exhibit this behavior:
//
//   CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, UNIQUE (b))
//
// Here, "b" column values are unique...except for the case of multiple NULL
// values, which are allowed because each NULL is treated as if it was a
// different value. Contrast this with the different NULL handling rules used
// by SQL's GROUP BY and DISTINCT operators. Those operators treat multiple NULL
// values as duplicates, because each NULL is treated as if it was the same
// value.
//
// The functional dependencies described up until now always use the "NULLs are
// equal" semantics (denoted as NULL= hereafter) in order to answer the question
// "are these two columns equal". The semantics are identical to what this SQL
// expression returns:
//
//   ((c1 = c2) OR (c1 IS NULL AND c2 IS NULL)) IS True
//
// And here are some examples:
//
//   c1    c2    NULL=
//   -----------------
//   1     1     true
//   NULL  NULL  true
//   1     2     false
//   1     NULL  false
//   NULL  1     false
//
// So now for the definition of A-->B that incorporates NULL values:
//
//   for any two rows r1 and r2 in the relation:
//   A(r1) NULL= A(r2) ==> B(r1) NULL= B(r2)
//
// Intuitively, if two different rows have equal values for A using "NULLs are
// equal" semantics, then those rows will also have equal values for B using
// those same semantics. As an example, the following sets of rows would be
// valid for the dependency (b)-->(c):
//
//   b     c
//   ----------
//   1     NULL
//   1     NULL
//   NULL  1
//   NULL  1
//   2     3
//   2     3
//
//   b     c
//   ----------
//   NULL  NULL
//   NULL  NULL
//
// but these sets of rows would be invalid:
//
//   b     c
//   ----------
//   NULL  1
//   NULL  NULL
//
//   b     c
//   ----------
//   NULL  1
//   NULL  2
//
// Unique constraints allow the latter cases, however, and therefore it is
// desirable to somehow encode these weaker dependencies as FDs, because they
// can be strengthened later on if NULL values are filtered from determinant
// columns (more on that below).
//
// The solution is to store an extra "strict" bit on each FD. If true, then the
// the FD is a "strict" dependency, and behaves as described above. However, if
// false, then the FD is a "lax" dependency. Lax dependencies use "squiggly"
// arrow notation to differentiate them from the strict variant:
//
//   A~~>B
//
// In contrast to strict dependencies, lax dependencies treat NULLs on
// determinant columns as distinct from one another, with equality semantics
// identical to this SQL expression:
//
//   (c1 = c2) IS True
//
// In other words, if either c1 or c2 is NULL, or both are NULL, then c1 is
// considered not equal to c2. The definition for A~~>B follows from that:
//
//   for any two rows r1 and r2 in the relation:
//   (A(r1) = A(r2)) IS True ==> B(r1) NULL= B(r2)
//
// In other words, if two different non-NULL rows have equal values for A, then
// those rows will also have equal values for B using NULL= semantics. Note that
// both strict and lax equality definitions collapse to the same semantics when
// the columns of A are not-NULL. The example row sets shown above that were
// invalid for a strict dependency are valid for a lax dependency:
//
//   b     c
//   ----------
//   NULL  1
//   NULL  NULL
//
//   b     c
//   ----------
//   NULL  1
//   NULL  2
//
// To continue the CREATE TABLE example shown above, another FD can now be
// derived from that statement, in addition to the primary key FD:
//
//   (a)-->(b,c)
//   (b)~~>(a,c)
//
// Lax dependencies are *not* transitive, and have limited usefulness as-is.
// However, some operators (like Select) can "reject" NULL values, which means
// that they filter out rows containing the troublesome NULL values. That makes
// it possible for the operator to "upgrade" a lax dependency to a strict
// dependency (recall that the both have identical semantics when NULLs are not
// present), as in this example:
//
//   SELECT * FROM t WHERE b>5
//
// The ">" operator rejects NULL values, which means that the Select operator
// can convert the lax dependency to a strict dependency:
//
//   (a)-->(b,c)
//   (b)-->(a,c)
//
// Now, either the "a" or "b" column determines the values of all other columns,
// and both are keys for the relation.
//
// Another thing to note is that a lax dependency with an empty determinant is
// the same as the corresponding strict dependency:
//
//   ()~~>(a,b)
//   ()-->(a,b)
//
// As described above, a strict dependency differs from a lax dependency only in
// terms of what values are allowed in determinant columns. Since the
// determinant has no columns in these cases, the semantics will be identical.
// For that reason, this library automatically maps lax constant dependencies to
// strict constant dependencies.
//
// Keys
//
// A key is a set of columns that have a unique composite value for every row in
// the relation. There are two kinds of keys, strict and lax, that parallel the
// two kinds of functional dependencies. Strict keys treat NULL values in key
// columns as equal to one another:
//
//   b     c
//   --------
//   1     10
//   2     20
//   NULL  30
//
// Here, "b" is a key for the relation, even though it contains a NULL value,
// because there is only one such value. Multiple NULL values would violate the
// strict key, because they would compare as equal, and therefore would be
// considered duplicates. The SQL GROUP BY operator uses the same semantics for
// grouping (it's no coincidence that the definition for strict keys follows
// that lead).
//
// By contrast, lax keys treat NULL values in key columns as distinct from one
// another, and so considers column "b" as unique in the following example:
//
//   b     c
//   --------
//   1     10
//   2     20
//   NULL  30
//   NULL  40
//
// Note that both strict and lax keys treat non-NULL values identically; values
// from two different rows must never compare equal to one another. In addition,
// the presence of a strict or lax key always implies a functional dependency
// with the key as determinant and all other columns in the relation as
// dependants. Here is an example assuming a table with columns (a,b,c,d):
//
//   lax-key(a,b)    => (a,b)~~>(c,d)
//   strict-key(a,b) => (a,b)-->(c,d)
//
// The "empty key" is a special key that has zero columns. It is used when the
// relation is guaranteed to have at most one row. In this special case, every
// column is constant. Every combination of columns is a trivial key for the
// relation and determines every other column. Because the lax and strict key
// designations are equivalent when there is a single row, empty keys are always
// normalized to be strict for convenience.
//
// FuncDepSet tracks whether at least one key (whether it be strict or lax)
// exists for the relation. If this is true, then all possible keys for the
// relation can be enumerated using the FD set. This is because any subset of
// columns forms a key if its FD closure contains every column in the relation.
// Therefore, all keys can be brute force enumerated by checking the closure of
// each combination in the power set. Again, this is only possible when the
// relation is known to have a key; otherwise, knowing the closure contains all
// columns is not a sufficient condition to identify a key, because of the
// possibility of duplicate rows.
//
// In practice, it is never necessary to enumerate all possible keys (fortunate,
// since there can be O(2**N) of them), since the vast majority of them turn out
// to have redundant columns that can be functionally determined from other
// columns in the key. Of more value is the set of "candidate keys", which are
// keys that contain no redundant columns. Removing any column from such a key
// causes it to no longer be a key. It is possible to enumerate the set of
// candidate keys in polynomial rather than exponential time (see Wikipedia
// "Candidate key" entry).
//
// However, since even polynomial time can be problematic, this library tries to
// avoid enumerating keys by storing and maintaining a single candidate key for
// the relation. And while it is not always successful, the library tries to
// keep the candidate key that has the fewest number of columns. In most cases,
// this single key is enough to satisfy the requirements of the optimizer. But
// when it is not enough, or the existing key is no longer valid, then a new
// candidate key can always be generated.
//
// It turns out that the most common key-related question that must be answered
// is not "what are the list of keys for this relation?", but instead, "does
// this set of columns contain a key for the relation?". The latter question can
// be easily answered by computing the closure of the columns, and checking
// whether the closure contains the key maintained by FuncDepSet. And when a
// relatively short key is needed (e.g. during decorrelation), FuncDepSet has
// one ready to go.
//
// Equivalent Columns
//
// FD sets encode "equivalent columns", which are pairs of columns that always
// have equal values using the SQL equality operator with NULL= semantics. Two
// columns a and b are equivalent if the following expression returns true:
//
//   ((a = b) OR (a IS NULL AND b IS NULL)) IS True
//
// Equivalent columns are typically derived from a Select filter condition, and
// are represented as two FDs with each column acting as both determinant and
// dependant:
//
//   SELECT * FROM t WHERE b=c
//   (a)-->(b,c)
//   (b)~~>(a,c)
//   (b)==(c)
//   (c)==(b)
//
// In the common case shown above, the WHERE clause rejects NULL values, so the
// equivalency will always be strict, which means it retains all the same
// properties of a strict dependency. While lax equivalencies are theoretically
// possible, the library currently maps them into regular lax dependencies to
// simplify implementation.
//
// Theory to Practice
//
// For a more rigorous examination of functional dependencies and their
// interaction with various SQL operators, see the following Master's Thesis:
//
//   Norman Paulley, Glenn. (2000).
//   Exploiting Functional Dependence in Query Optimization.
//   https://cs.uwaterloo.ca/research/tr/2000/11/CS-2000-11.thesis.pdf
//
// While this paper served as the inspiration for this library, a number of
// details differ, including (but not limited to):
//
//   1. Most importantly, the definition of "lax" used in the paper differs from
//      the definition used by this library. For a lax dependency A~~>B, the
//      paper allows this set of rows:
//
//        a  b
//        -------
//        1  1
//        1  NULL
//
//      This library disallows that, since it requires that if the determinant
//      of a lax dependency is not-null, then it is equivalent to a strict
//      dependency. This alternate definition is briefly covered in section
//      2.5.3.2 of the paper (see definition 2.19). The reason for this change
//      is to allow a lax dependency to be upgraded to a strict dependency more
//      readily, needing only the determinant columns to be not-null rather than
//      both determinant and dependant columns.
//
//   2. The paper simplifies FD sets so that dependants never contain more than
//      one column. This library allows multiple dependent columns, since they
//      can be so efficiently stored and processed as ColSets.
//
//   3. The paper deliberately avoids all simplifications when a SQL operator
//      adds new FDs to an existing FD set, in order to avoid unneeded work and
//      expensive reductions. This library performs quite a few simplifications
//      in order to keep the FD set more manageable and understandable.
//
//   4. The paper "colors" columns black when they are no longer part of a
//      derived relation. Rather than marking removed columns, this library
//      actually removes them from the FD set.
//
//   5. In order to ensure a unique key for every relation, the paper uses a
//      special "tuple identifier" that acts like a virtual column and can be
//      both a determinant and a dependant. If the transitive closure of any set
//      of columns includes the tuple identifier column, then that set of
//      columns is a super key for the relation. As described in the Keys
//      section above, this library takes a simplified approach so that it
//      doesn't need to allocate virtual columns in property derivation code.
//
type FuncDepSet struct {
	// deps contains the functional dependencies that have a non-trivial
	// determinant and dependant (i.e. not empty, with no overlapping columns):
	//
	//   (a)-->(b,c)
	//   (b,c)~~>(a,d)
	//   (d)==(e)
	//   (e)==(d)
	//
	// See the above comments for more details.
	//
	// This slice is owned by this FuncDepSet and shouldn't be shared unless
	// all referencing sets are treated as immutable.
	deps []funcDep

	// hasKey is:
	//  - strictKey if the relation has no duplicate rows, which means at least
	//    one subset of its columns form a key (all columns, if no other subset).
	//    The key field contains one such key. See the "Keys" section above for
	//    more details. A strict key can be empty.
	//  - laxKey if there is a at least one subset of columns that form a lax key.
	//    The key field contains one such key. A lax key cannot be empty.
	//
	// See the "Keys" section above for more details.
	hasKey keyType

	// key contains a set of columns that form a key or a lax key for the
	// relation, depending on hasKey; empty if hasKey is noKey.
	//
	// There is no guarantee that the key has the minimum possible number of
	// columns, but a best effort is made to keep it as short as possible.
	//
	// See the "Keys" section above for more details.
	//
	// This set is immutable; to update it, replace it with a different set
	// containing the desired columns.
	key opt.ColSet
}

type keyType int8

const (
	noKey keyType = iota
	laxKey
	strictKey
)

// funcDep stores a single functional dependency. See the comment for FuncDepSet
// for more details.
type funcDep struct {
	// from is the determinant of the functional dependency (easier to read the
	// code when "from" is used rather than "determinant"). If equiv is true,
	// from may only consist of a single column.
	//
	// This set is immutable; to update it, replace it with a different set
	// containing the desired columns.
	from opt.ColSet

	// to is the dependant of the functional dependency (easier to read the code
	// when "to" is used rather than "dependant").
	//
	// This set is immutable; to update it, replace it with a different set
	// containing the desired columns.
	to opt.ColSet

	// strict is true if NULL values in the determinant are treated as if they are
	// equal to other NULL values. Every NULL determinant must therefore map to
	// the same dependant value. If strict is false, then two NULL determinants
	// can map to different dependant values. See the NULL Values section in the
	// FuncDeps comment for more details.
	strict bool

	// equiv is true if the value of the determinant equals the value of each of
	// the dependant columns, and false if there's no known equality relationship.
	// If equiv is true, the determinant may only consist of a single column.
	equiv bool
}

// StrictKey returns a strict key for the relation, if there is one.
// A best effort is made to return a candidate key that has the fewest columns.
func (f *FuncDepSet) StrictKey() (_ opt.ColSet, ok bool) {
	if f.hasKey == strictKey {
		return f.key, true
	}
	return opt.ColSet{}, false
}

// LaxKey returns a lax key for the relation, if there is one.
// Note that strict keys are implicitly also lax keys, so if the relation has a
// strict key, this returns the same key as StrictKey().
// A best effort is made to return a lax key that has the fewest columns.
func (f *FuncDepSet) LaxKey() (_ opt.ColSet, ok bool) {
	if f.hasKey != noKey {
		return f.key, true
	}
	return opt.ColSet{}, false
}

// Empty is true if the set contains no FDs and no key.
func (f *FuncDepSet) Empty() bool {
	return len(f.deps) == 0 && f.hasKey == noKey
}

// ColSet returns all columns referenced by the FD set.
func (f *FuncDepSet) ColSet() opt.ColSet {
	var cols opt.ColSet
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]
		cols.UnionWith(fd.from)
		cols.UnionWith(fd.to)
	}
	if f.hasKey != noKey {
		// There are cases where key columns don't show up in FDs. For example:
		//   lax-key(2,3); ()-->(1)
		cols.UnionWith(f.key)
	}
	return cols
}

// HasMax1Row returns true if the relation has zero or one rows.
func (f *FuncDepSet) HasMax1Row() bool {
	return f.hasKey == strictKey && f.key.Empty()
}

// CopyFrom copies the given FD into this FD, replacing any existing data.
func (f *FuncDepSet) CopyFrom(fdset *FuncDepSet) {
	// Make certain to copy FDs to the slice owned by this set.
	f.deps = f.deps[:0]
	f.deps = append(f.deps, fdset.deps...)
	f.key = fdset.key
	f.hasKey = fdset.hasKey
}

// RemapFrom copies the given FD into this FD, remapping column IDs according to
// the from/to lists. Specifically, column from[i] is replaced with column
// to[i] (see TranslateColSet).
// Any columns not in the from list are removed from the FDs.
func (f *FuncDepSet) RemapFrom(fdset *FuncDepSet, fromCols, toCols opt.ColList) {
	f.CopyFrom(fdset)
	colSet := f.ColSet()
	fromSet := fromCols.ToSet()
	if !colSet.SubsetOf(fromSet) {
		f.ProjectCols(colSet.Intersection(fromSet))
	}
	for i := range f.deps {
		f.deps[i].from = opt.TranslateColSetStrict(f.deps[i].from, fromCols, toCols)
		f.deps[i].to = opt.TranslateColSetStrict(f.deps[i].to, fromCols, toCols)
	}
	f.key = opt.TranslateColSetStrict(f.key, fromCols, toCols)
}

// ColsAreStrictKey returns true if the given columns contain a strict key for the
// relation. This means that any two rows in the relation will never have the
// same values for this set of columns. If the columns are nullable, then at
// most one row could have NULL values for all of the columns. For example,
// (a,b) is a strict key for the following relation, but (a) is not (because
// there are multiple rows where a=1 and a=NULL):
//
//   a     b     c
//   ----------------
//   NULL  NULL  NULL
//   NULL  1     1
//   1     NULL  1
//   1     1     1
//
func (f *FuncDepSet) ColsAreStrictKey(cols opt.ColSet) bool {
	return f.colsAreKey(cols, strictKey)
}

// ColsAreLaxKey returns true if the given columns contain a lax key for the
// relation. This means that any two rows in the relation will never have the
// same values for this set of columns, except potentially in the case where at
// least one of the columns is NULL. For example, (a,b) is a lax key for the
// following relation, but (a) is not (because there are multiple rows where
// a=1):
//
//   a     b     c
//   ----------------
//   NULL  NULL  NULL
//   NULL  NULL  1
//   NULL  NULL  2
//   NULL  1     1
//   NULL  1     2
//   1     NULL  1
//   1     NULL  2
//   1     1     1
//
func (f *FuncDepSet) ColsAreLaxKey(cols opt.ColSet) bool {
	return f.colsAreKey(cols, laxKey)
}

// ConstantCols returns the set of columns that will always have the same value
// for all rows in the relation.
func (f *FuncDepSet) ConstantCols() opt.ColSet {
	if len(f.deps) > 0 && f.deps[0].isConstant() {
		return f.deps[0].to
	}
	return opt.ColSet{}
}

// ReduceCols removes redundant columns from the given set. Redundant columns
// can be functionally determined from the remaining columns. If the columns
// contain a key for the relation, then the reduced columns will form a
// candidate key for the relation.
//
// The reduction algorithm removes one column at a time (in an arbitrary order),
// and then tests to see if the closure still includes the removed column. If
// so, then the column is redundant. This algorithm has decent running time, but
// will not necessarily find the candidate key with the fewest columns.
func (f *FuncDepSet) ReduceCols(cols opt.ColSet) opt.ColSet {
	var removed opt.ColSet
	cols = cols.Copy()
	for i, ok := cols.Next(0); ok; i, ok = cols.Next(i + 1) {
		cols.Remove(i)
		removed.Add(i)
		if !f.inClosureOf(removed, cols, true /* strict */) {
			// The column is not functionally determined by the other columns, so
			// retain it in the set.
			cols.Add(i)
		}
		removed.Remove(i)
	}
	return cols
}

// InClosureOf returns true if the given columns are functionally determined by
// the "in" column set.
func (f *FuncDepSet) InClosureOf(cols, in opt.ColSet) bool {
	return f.inClosureOf(cols, in, true /* strict */)
}

// ComputeClosure returns the strict closure of the given columns. The closure
// includes the input columns plus all columns that are functionally dependent
// on those columns, either directly or indirectly. Consider this set of FD's:
//
//   (a)-->(b,c,d)
//   (b,c,e)-->(f)
//   (d)-->(e)
//
// The strict closure of (a) is (a,b,c,d,e,f), because (a) determines all other
// columns. Therefore, if two rows have the same value for (a), then the rows
// will be duplicates, since all other columns will be equal.
func (f *FuncDepSet) ComputeClosure(cols opt.ColSet) opt.ColSet {
	cols = cols.Copy()
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]

		if fd.strict && fd.from.SubsetOf(cols) && !fd.to.SubsetOf(cols) {
			cols.UnionWith(fd.to)

			// Restart iteration to get transitive closure.
			i = -1
		}
	}
	return cols
}

// AreColsEquiv returns true if the two given columns are equivalent.
func (f *FuncDepSet) AreColsEquiv(col1, col2 opt.ColumnID) bool {
	for i := range f.deps {
		fd := &f.deps[i]

		if fd.equiv && fd.strict {
			if (fd.from.Contains(col1) && fd.to.Contains(col2)) ||
				(fd.from.Contains(col2) && fd.to.Contains(col1)) {
				return true
			}
		}
	}
	return false
}

// ComputeEquivClosure returns the equivalence closure of the given columns. The
// closure includes the input columns plus all columns that are equivalent to
// any of these columns, either directly or indirectly. For example:
//
//   (a)==(b)
//   (b)==(c)
//   (a)==(d)
//
// The equivalence closure for (a) is (a,b,c,d) because (a) is transitively
// equivalent to all other columns. Therefore, all columns must have equal
// non-NULL values, or else all must be NULL (see definition for NULL= in the
// comment for FuncDepSet).
func (f *FuncDepSet) ComputeEquivClosure(cols opt.ColSet) opt.ColSet {
	// Don't need to get transitive closure, because equivalence closures are
	// already maintained for every column.
	cols = cols.Copy()
	for i := range f.deps {
		fd := &f.deps[i]
		if fd.equiv && fd.from.SubsetOf(cols) && !fd.to.SubsetOf(cols) {
			cols.UnionWith(fd.to)
		}
	}
	return cols
}

// AddStrictKey adds an FD for a new key. The given key columns are reduced to a
// candidate key, and that becomes the determinant for the allCols column set.
// The resulting FD is strict, meaning that a NULL key value always maps to the
// same set of values in the rest of the relation's columns. For key columns
// (a,b) and relation columns (a,b,c,d), an FD like this is created:
//
//   (a,b)-->(c,d)
//
// If the resulting candidate key has fewer columns than the current key, then
// the new key is adopted in its place.
func (f *FuncDepSet) AddStrictKey(keyCols, allCols opt.ColSet) {
	if !keyCols.SubsetOf(allCols) {
		panic(errors.AssertionFailedf("allCols does not include keyCols"))
	}

	// Ensure we have candidate key (i.e. has no columns that are functionally
	// determined by other columns).
	keyCols = f.ReduceCols(keyCols)
	f.addDependency(keyCols, allCols, true /* strict */, false /* equiv */)

	// Try to use the new FD to reduce any existing key first.
	f.tryToReduceKey(opt.ColSet{} /* notNullCols */)

	if f.hasKey != strictKey || keyCols.Len() < f.key.Len() {
		f.setKey(keyCols, strictKey)
	}
}

// AddLaxKey is similar to AddStrictKey, except that it creates a lax FD rather
// than a strict FD. This means that two rows with NULL key values might not
// have the same values in other non-key columns. For key columns (a,b) and
// relation columns (a,b,c,d), and FD like this is created:
//
//   (a,b)~~>(c,d)
//
func (f *FuncDepSet) AddLaxKey(keyCols, allCols opt.ColSet) {
	if !keyCols.SubsetOf(allCols) {
		panic(errors.AssertionFailedf("allCols does not include keyCols"))
	}
	if keyCols.Empty() {
		panic(errors.AssertionFailedf("lax key cannot be empty"))
	}

	// Ensure we have candidate key (i.e. has no columns that are functionally
	// determined by other columns).
	f.addDependency(keyCols, allCols, false /* strict */, false /* equiv */)

	// TODO(radu): without null column information, we cannot reduce lax keys (see
	// tryToReduceKey). Consider passing that information (or storing it with the
	// FDs to begin with). In that case we would need to reduce both the given key
	// and the existing key, similar to AddStrictKey.

	if f.hasKey == noKey || (f.hasKey == laxKey && keyCols.Len() < f.key.Len()) {
		f.setKey(keyCols, laxKey)
	}
}

// MakeMax1Row initializes the FD set for a relation containing either zero or
// one rows, and with the given columns. In this special case, the value of
// every column is trivially considered a constant, and the key is the empty
// set, because no columns are required to ensure uniqueness of rows. This
// special case may seem trivial, but it is quite important to detect during
// optimization. For a relation with columns (a, b), the following FD is
// created in the set:
//
//   ()-->(a,b)
//
// If f has equivalence dependencies of columns that are a subset of cols, those
// dependencies are retained in f. This prevents losing additional information
// about the columns, which a single FD with an empty key cannot describe. For
// example:
//
//   f:      (a)-->(b,c), (a)==(b), (b)==(a), (a)==(c), (c)==(a)
//   cols:   (a,c)
//   result: ()-->(a,c), (a)==(c), (c)==(a)
//
func (f *FuncDepSet) MakeMax1Row(cols opt.ColSet) {
	// Remove all FDs except for equivalency FDs with columns that are a subset
	// of cols.
	copyIdx := 0
	for i := range f.deps {
		fd := &f.deps[i]

		// Skip non-equivalence dependencies.
		if !fd.equiv {
			continue
		}

		// If fd is an equivalence dependency, from has a single column.
		fromCol, ok := fd.from.Next(0)
		if !ok {
			panic(errors.AssertionFailedf("equivalence dependency from cannot be empty"))
		}

		// Add a new equivalence dependency with the same from column and the to
		// columns that are present in cols.
		if cols.Contains(fromCol) && fd.to.Intersects(cols) {
			f.deps[copyIdx] = funcDep{
				from:   fd.from,
				to:     fd.to.Intersection(cols),
				strict: true,
				equiv:  true,
			}
			copyIdx++
		}
	}
	f.deps = f.deps[:copyIdx]

	if !cols.Empty() {
		f.deps = append(f.deps, funcDep{to: cols, strict: true})
		// The constant FD must be the first in the set.
		f.deps[0], f.deps[len(f.deps)-1] = f.deps[len(f.deps)-1], f.deps[0]
	}
	f.setKey(opt.ColSet{}, strictKey)
}

// MakeNotNull modifies the FD set based on which columns cannot contain NULL
// values. This often allows upgrading lax dependencies to strict dependencies,
// and lax keys to strict keys.
//
// Note: this function should be called with all known null columns; it won't do
// as good of a job if it's called multiple times with different subsets.
func (f *FuncDepSet) MakeNotNull(notNullCols opt.ColSet) {
	// We have to collect all the FDs that can be made strict. We avoid allocation
	// for the case where there is at most one such FD.
	var firstLaxFD *funcDep
	var otherLaxFDs []funcDep
	for i := range f.deps {
		fd := &f.deps[i]
		if fd.strict {
			continue
		}

		// FD can be made strict if all determinant columns are not null.
		if fd.from.SubsetOf(notNullCols) {
			if firstLaxFD == nil {
				firstLaxFD = fd
			} else {
				otherLaxFDs = append(otherLaxFDs, *fd)
			}
		}
	}

	if firstLaxFD != nil {
		f.addDependency(firstLaxFD.from, firstLaxFD.to, true /* strict */, false /* equiv */)
		for i := range otherLaxFDs {
			f.addDependency(otherLaxFDs[i].from, otherLaxFDs[i].to, true /* strict */, false /* equiv */)
		}
	}

	f.tryToReduceKey(notNullCols)
}

// AddEquivalency adds two FDs to the set that establish a strict equivalence
// between the given columns. Either "a" equals "b" according to SQL equality
// semantics, or else "a" is NULL and "b" is NULL. The following FDs are
// created in the set:
//
//   (a)==(b)
//   (b)==(a)
//
func (f *FuncDepSet) AddEquivalency(a, b opt.ColumnID) {
	if a == b {
		return
	}

	var equiv opt.ColSet
	equiv.Add(a)
	equiv.Add(b)
	f.addEquivalency(equiv)
}

// AddConstants adds a strict FD to the set that declares the given column as
// having the same constant value for all rows. If the column is nullable, then
// its value may be NULL, but then the column must be NULL for all rows. For
// column "a", the FD looks like this:
//
//   ()-->(a)
//
// Since it is a constant, any set of determinant columns (including the empty
// set) trivially determines the value of "a".
func (f *FuncDepSet) AddConstants(cols opt.ColSet) {
	if cols.Empty() {
		return
	}

	// Determine complete set of constants by computing closure.
	cols = f.ComputeClosure(cols)

	// Ensure that first FD in the set is a constant FD and make sure the
	// constants are part of it.
	if len(f.deps) == 0 || !f.deps[0].isConstant() {
		deps := make([]funcDep, len(f.deps)+1)
		deps[0] = funcDep{to: cols, strict: true}
		copy(deps[1:], f.deps)
		f.deps = deps
	} else {
		// Update existing constant FD to include all constant columns in the set.
		f.deps[0].to = cols
	}

	// Remove any other FDs made redundant by adding the constants.
	n := 1
	for i := 1; i < len(f.deps); i++ {
		fd := &f.deps[i]

		// Always retain equivalency information, even for constants.
		if !fd.equiv {
			if fd.strict {
				// Constant columns can be removed from the determinant of a strict
				// FD. If all determinant columns are constant, then the entire FD
				// can be removed, since this means that the dependant columns must
				// also be constant (and were part of constant closure added to the
				// constant FD above).
				if !fd.removeFromCols(cols) {
					continue
				}
			}

			// Dependant constants are redundant, so remove them.
			if !fd.removeToCols(cols) {
				continue
			}
		}

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}
	f.deps = f.deps[:n]

	f.tryToReduceKey(opt.ColSet{} /* notNullCols */)
}

// AddSynthesizedCol adds an FD to the set that is derived from a synthesized
// column in a projection list. The synthesized column is often derived from
// other columns, in which case AddSynthesizedCol creates a new FD like this:
//
//   (a,b)-->(c)
//
// Or it may be a constant column, like this:
//
//   ()-->(c)
//
func (f *FuncDepSet) AddSynthesizedCol(from opt.ColSet, col opt.ColumnID) {
	if from.Contains(col) {
		panic(errors.AssertionFailedf("synthesized column cannot depend upon itself"))
	}

	var colSet opt.ColSet
	colSet.Add(col)
	f.addDependency(from, colSet, true /* strict */, false /* equiv */)

	f.tryToReduceKey(opt.ColSet{} /* notNullCols */)
}

// ProjectCols removes all columns that are not in the given set. It does this
// by replacing any un-projected dependants by their closures, and then removing
// all FDs containing un-projected columns. While this algorithm may cause some
// loss of information in edge cases, it does a good job of preserving the most
// important dependency information.
func (f *FuncDepSet) ProjectCols(cols opt.ColSet) {
	// Ensure that any existing key contains only projected columns. Do this
	// before removing any FDs from the set, in order to take advantage of all
	// existing transitive relationships.
	if f.hasKey != noKey && !f.key.SubsetOf(cols) {
		// Derive new candidate key (or key is no longer possible).
		if f.hasKey == strictKey && f.ColsAreStrictKey(cols) {
			f.setKey(cols, strictKey)
			f.tryToReduceKey(opt.ColSet{} /* notNullCols */)
		} else if f.ColsAreLaxKey(cols) {
			f.setKey(cols, laxKey)
			f.tryToReduceKey(opt.ColSet{} /* notNullCols */)
		} else {
			f.clearKey()
		}
	}

	// Special case of <= 1 row.
	if f.hasKey == strictKey && f.key.Empty() {
		f.MakeMax1Row(cols)
		return
	}

	// During first pass, add closures of un-projected columns in dependants.
	// This will ensure that transitive relationships between remaining columns
	// won't be lost. Also, track list of un-projected columns that are part of
	// non-equivalent determinants. It's possible these can be mapped to
	// equivalent columns.
	var constCols, detCols, equivCols opt.ColSet
	for i := range f.deps {
		fd := &f.deps[i]

		// Remember constant columns.
		if fd.isConstant() {
			constCols = fd.to
		}

		// Add closures to dependants containing un-projected columns.
		if !fd.to.SubsetOf(cols) {
			// Equivalence dependencies already maintain closure, so skip them.
			if !fd.equiv {
				fd.to = f.ComputeClosure(fd.to)
				// Maintain the invariant that from and to columns don't overlap.
				fd.to.DifferenceWith(fd.from)
			}
		}

		// Track list of un-projected columns that can possibly be mapped to
		// equivalent columns.
		if !fd.equiv && !fd.from.SubsetOf(cols) {
			detCols.UnionWith(fd.from)
			detCols.DifferenceWith(cols)
		}

		// Track all columns that have equivalent alternates that are part of the
		// projection.
		if fd.equiv && fd.to.Intersects(cols) {
			equivCols.UnionWith(fd.from)
		}
	}

	// Construct equivalence map that supports substitution of an equivalent
	// column in place of a removed column.
	detCols.IntersectionWith(equivCols)
	equivMap := f.makeEquivMap(detCols, cols)

	// If constants were found, then normalize constants to preserve FDs in a
	// case like this where (2) is removed:
	//
	//   ()-->(2), (2,3)-->(4)
	//
	// Rather than removing both FDs, the second FD should be preserved in this
	// form:
	//
	//   (3)-->(4)
	//
	if !constCols.Empty() {
		f.AddConstants(constCols)
	}

	// During second pass, remove all dependencies with un-projected columns.
	var newFDs []funcDep
	n := 0
	for i := range f.deps {
		fd := &f.deps[i]

		// Subtract out un-projected columns from dependants. Also subtract strict
		// constant columns from dependants for nicer presentation.
		if !fd.to.SubsetOf(cols) {
			fd.to = fd.to.Intersection(cols)
			if !fd.isConstant() {
				fd.to.DifferenceWith(constCols)
			}
			if !fd.removeToCols(fd.from) {
				continue
			}
		}

		// Try to substitute equivalent columns for removed determinant columns.
		if !fd.from.SubsetOf(cols) {
			if fd.equiv {
				// Always discard equivalency with removed determinant, since other
				// equivalencies will already include this column.
				continue
			}

			// Start with "before" list of columns that need to be mapped, and try
			// to find an "after" list containing equivalent columns.
			var afterCols opt.ColSet
			beforeCols := fd.from.Difference(cols)
			foundAll := true
			for c, ok := beforeCols.Next(0); ok; c, ok = beforeCols.Next(c + 1) {
				var id opt.ColumnID
				if id, foundAll = equivMap[c]; !foundAll {
					break
				}
				afterCols.Add(id)
			}
			if foundAll {
				// Dependency can be remapped using equivalencies.
				from := fd.from.Union(afterCols)
				from.DifferenceWith(beforeCols)
				newFDs = append(newFDs, funcDep{from: from, to: fd.to, strict: fd.strict, equiv: fd.equiv})
			}
			continue
		}

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}
	f.deps = f.deps[:n]

	for i := range newFDs {
		fd := &newFDs[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
	}

	// Ensure that key still determines all other columns.
	f.ensureKeyClosure(cols)
}

// AddFrom merges two FD sets by adding each FD from the given set to this set.
// While this requires O(N**2) time, it's useful when the two FD sets may
// overlap one another and substantial simplifications are possible (as with
// IndexJoin). It is up to the caller to ensure that the two FD sets are
// "compatible", meaning that they operate on the same relations, with the same
// keys, same columns, etc.
func (f *FuncDepSet) AddFrom(fdset *FuncDepSet) {
	for i := range fdset.deps {
		fd := &fdset.deps[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
	}
}

// AddEquivFrom is similar to AddFrom, except that it only adds equivalence
// dependencies from the given set to this set.
func (f *FuncDepSet) AddEquivFrom(fdset *FuncDepSet) {
	for i := range fdset.deps {
		fd := &fdset.deps[i]
		if fd.equiv {
			f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
		}
	}
}

// MakeProduct modifies the FD set to reflect the impact of a cartesian product
// operation between this set and the given set. The result is a union of the
// FDs from each set, as well as a union of their keys. The two FD sets are
// expected to operate on disjoint columns, so the FDs from each are simply
// concatenated, rather than simplified via calls to addDependency (except for
// case of constant columns).
func (f *FuncDepSet) MakeProduct(inner *FuncDepSet) {
	for i := range inner.deps {
		fd := &inner.deps[i]
		if fd.isConstant() {
			f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
		} else {
			f.deps = append(f.deps, *fd)
		}
	}

	if f.hasKey != noKey && inner.hasKey != noKey {
		// If both sides have a strict key, the union of keys is a strict key.
		// If one side has a lax key and the other has a lax or strict key, the
		// union is a lax key.
		typ := laxKey
		if f.hasKey == strictKey && inner.hasKey == strictKey {
			typ = strictKey
		}
		f.setKey(f.key.Union(inner.key), typ)
	} else {
		f.clearKey()
	}
}

// MakeApply modifies the FD set to reflect the impact of an apply join. This
// FD set reflects the properties of the outer query, and the given FD set
// reflects the properties of the inner query. Constant FDs from inner set no
// longer hold and some other dependencies need to be augmented in order to be
// valid for the apply join operator. Consider this example:
//
//   SELECT *
//   FROM a
//   INNER JOIN LATERAL (SELECT * FROM b WHERE b.y=a.y)
//   ON True
//
// 1. The constant dependency created from the outer column reference b.y=a.y
//    does not hold for the Apply operator, since b.y is no longer constant at
//    this level. In general, constant dependencies cannot be retained, because
//    they may have been generated from outer column equivalencies.
// 2. If a strict dependency (b.x,b.y)-->(b.z) held, it would have been reduced
//    to (b.x)-->(b.z) because (b.y) is constant in the inner query. However,
//    (b.x)-->(b.z) does not hold for the Apply operator, since (b.y) is not
//    constant in that case. However, the dependency *does* hold as long as its
//    determinant is augmented by the left input's key columns (if key exists).
// 3. Lax dependencies follow the same rules as #2.
// 4. Equivalence dependencies in the inner query still hold for the Apply
//    operator.
// 5. If both the outer and inner inputs of the apply join have keys, then the
//    concatenation of those keys is a key on the apply join result.
//
func (f *FuncDepSet) MakeApply(inner *FuncDepSet) {
	for i := range inner.deps {
		fd := &inner.deps[i]
		if fd.equiv {
			f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
		} else if !fd.isConstant() && f.hasKey == strictKey {
			f.addDependency(f.key.Union(fd.from), fd.to, fd.strict, fd.equiv)
		}
		// TODO(radu): can we use a laxKey here?
	}

	if f.hasKey == strictKey && inner.hasKey == strictKey {
		f.setKey(f.key.Union(inner.key), strictKey)
		f.ensureKeyClosure(inner.ColSet())
	} else {
		// TODO(radu): can we use a laxKey here?
		f.clearKey()
	}
}

// MakeLeftOuter modifies the cartesian product FD set to reflect the impact of
// adding NULL-extended rows to the results of an inner join. An inner join can
// be modeled as a cartesian product + ON filtering, and an outer join is
// modeled as an inner join + union of NULL-extended rows. MakeLeftOuter enacts
// the filtering and null-extension of the cartesian product. If it is possible
// to prove that there is a key over the join result that consists only of
// columns from the left input, that key will be used.
//
// This same logic applies for right joins as well (by reversing sides).
//
// See the "Left outer join" section on page 84 of the Master's Thesis for the
// impact of outer joins on FDs.
func (f *FuncDepSet) MakeLeftOuter(
	leftFDs, filtersFDs *FuncDepSet, leftCols, rightCols, notNullInputCols opt.ColSet,
) {
	// The columns from the left input form a key over the result of the LeftJoin
	// if the following conditions are met:
	//
	//  1. The left input has a strict key.
	//
	//  2. The left columns form a strict key over the filtered cartesian product.
	//     (In other words, the left columns would form a key over an inner join
	//      with the same filters).
	//
	// The above conditions are sufficient because a strict key (over the filtered
	// cartesian product) that only contains columns from the left side implies
	// that no left rows were duplicated. This is because even a single duplicated
	// row would prohibit a strict key containing only those columns. And if there
	// was already a strict key in the original left input, adding back filtered
	// left rows will not create any duplicates. This means that the LeftJoin will
	// not duplicate any of the left rows. Therefore, a key over the left input
	// must also be a key over the result of the join.
	//
	// If the conditions are not met, a key over the unfiltered cartesian product
	// (if one exists) is used. Why is this key valid to use?
	//
	//   * A left join can filter rows and null-extend rows from the cartesian
	//     product.
	//   * Filtering rows does not affect the presence of a key.
	//   * Null-extending rows does not affect the presence of a key because the
	//     cartesian product could only have a key if the left and right inputs
	//     also had keys (see FuncDepSet.MakeProduct). Therefore, null-extended
	//     left rows that are added back by the left join must be unique.
	//
	// As an example, consider this data and this query:
	//
	//   a      b
	//   -      -
	//   1      1
	//   2      2
	//   3
	//   4
	//
	//   SELECT * FROM a_tab LEFT JOIN b_tab ON a < 3
	//
	// Both tables a and b have a strict key. If we take their cartesian product,
	// we get something like this:
	//
	//   a  b
	//   ----
	//   1  1
	//   1  2
	//   2  1
	//   2  2
	//   3  1
	//   3  2
	//   4  1
	//   4  2
	//
	// Now, columns a and b together form a strict key over the cartesian product.
	// If either a or b had duplicate rows to begin with, a key over the cartesian
	// product would not be possible. Now, the left join's "a < 3" on condition is
	// applied:
	//
	//   a  b
	//   ----
	//   1  1
	//   1  2
	//   2  1
	//   2  2
	//
	// Finally, the left join adds back the rows of a, null-extending b:
	//
	//   a  b
	//   ----
	//   1  1
	//   1  2
	//   2  1
	//   2  2
	//   3  NULL
	//   4  NULL
	//
	// Since a had a key to begin with, the "3" and "4" rows that are added back
	// are unique. Therefore, a and b are a strict key for the left join.
	//
	// TODO(drewk): support for lax keys/dependencies from the right input can be
	//  added if it turns out to be useful.

	// Save a strict key from the left input, if one exists.
	leftKey, leftHasKey := leftFDs.StrictKey()

	// Save a key from the unfiltered cartesian product, if one exists.
	oldKey := f.key
	oldKeyType := f.hasKey

	// If the left input has a key, add the FDs from the join filters to a copy of
	// the cartesian product FD set. Next, check whether the columns of the left
	// input form a strict key over the result of applying the join filters to the
	// cartesian product.
	//
	// We have to apply the filters to a copy because filter FDs are often not
	// valid after null-extended rows are added. For example:
	//
	//   a  b  c     d     e
	//   ----------------------
	//   1  1  1     NULL  1
	//   1  2  NULL  NULL  NULL
	//   2  1  NULL  NULL  NULL
	//
	// Let's say this table is the result of a join between 'ab' and 'cde'. The
	// join condition might have included e = 1, but it would not be correct to
	// add the corresponding constant FD to the final join FD set because the e
	// column has been null extended, and therefore the condition doesn't hold for
	// the final outer join result.
	leftColsAreInnerJoinKey := false
	if leftHasKey {
		c := FuncDepSet{}
		c.CopyFrom(f)
		c.AddFrom(filtersFDs)
		leftColsAreInnerJoinKey = c.ColsAreStrictKey(leftCols)
	}

	// Modify the cartesian product FD set to reflect the impact of adding
	// NULL-extended rows to the results of the filtered cartesian product (or, in
	// other words, the results of an inner join).
	f.nullExtendRightRows(rightCols, notNullInputCols)

	// If the conditions have been met, use the key from the left side. Otherwise,
	// use the key from the cartesian product.
	if leftHasKey && leftColsAreInnerJoinKey {
		f.setKey(leftKey, strictKey)
	} else {
		// See the comment at the top of the function for why it is valid to use the
		// key from the cartesian product.
		f.setKey(oldKey, oldKeyType)
		// Call tryToReduceKey with only the left columns from notNullInputCols
		// because the right columns may have been null-extended.
		f.tryToReduceKey(leftCols.Intersection(notNullInputCols))
	}
	// ensureKeyClosure must be called when oldKey is used as well as the new
	// leftKey because nullExtendRightRows can remove FDs, such that the closure
	// of oldKey ends up missing some columns from the right.
	f.ensureKeyClosure(leftCols.Union(rightCols))
}

// MakeFullOuter modifies the cartesian product FD set to reflect the impact of
// adding NULL-extended rows to the results of an inner join. An inner join can
// be modeled as a cartesian product + ON filtering, and an outer join is
// modeled as an inner join + union of NULL-extended rows. MakeFullOuter
// performs the final step for a full join, given the set of columns on each
// side, as well as the set of input columns from both sides of the join that
// are not null.
func (f *FuncDepSet) MakeFullOuter(leftCols, rightCols, notNullInputCols opt.ColSet) {
	if f.hasKey == strictKey {
		if f.key.Empty() {
			// The cartesian product has an empty key when both sides have an empty key;
			// but the outer join can have two rows so the empty key doesn't hold.
			f.hasKey = noKey
			f.key = opt.ColSet{}
		} else if !f.key.Intersects(notNullInputCols) {
			// If the cartesian product has a strict key, the key holds on the full
			// outer result only if one of the key columns is known to be not-null in
			// the input. Otherwise, a row where all the key columns are NULL can
			// "conflict" with a row where these columns are NULL because of
			// null-extension. For example:
			//   -- t1 and t2 each have one row containing NULL for column x.
			//   SELECT * FROM t1 FULL JOIN t2 ON t1.x=t2.x
			//
			//   t1.x  t2.x
			//   ----------
			//   NULL  NULL
			//   NULL  NULL
			f.hasKey = laxKey
		}
	}
	f.nullExtendRightRows(leftCols, notNullInputCols)
	f.nullExtendRightRows(rightCols, notNullInputCols)
	f.ensureKeyClosure(leftCols.Union(rightCols))
}

// nullExtendRightRows is used by MakeLeftOuter and MakeFullOuter to modify the
// cartesian product FD set to reflect the impact of adding NULL-extended rows
// to the results of an inner join. See the MakeLeftOuter comment for more
// information.
func (f *FuncDepSet) nullExtendRightRows(rightCols, notNullInputCols opt.ColSet) {
	var newFDs []funcDep

	n := 0
	for i := range f.deps {
		fd := &f.deps[i]

		if fd.isConstant() {
			// Null-extended constant columns are no longer constant, because they
			// now may contain NULL values.
			if fd.to.Intersects(rightCols) {
				constCols := fd.to.Intersection(rightCols)
				if !fd.removeToCols(constCols) {
					continue
				}
			}
		} else {
			// The next several rules depend on whether the dependency's determinant
			// and dependants are on the null-supplying or row-supplying sides of
			// the join (or both). The rules will use the following join and set of
			// result rows to give examples:
			//
			//   CREATE TABLE ab (a INT, b INT, PRIMARY KEY(a, b))
			//   CREATE TABLE cde (c INT PRIMARY KEY, d INT, e INT)
			//   SELECT * FROM ab LEFT OUTER JOIN cde ON a=c AND b=1
			//
			//   a  b  c     d     e
			//   ----------------------
			//   1  1  1     NULL  1
			//   1  2  NULL  NULL  NULL
			//   2  1  NULL  NULL  NULL
			//
			// Here are the rules:
			//
			// 1. A strict dependency with determinant on the null-supplying side of
			//    the join becomes lax for any dependants on the row-supplying side
			//    of the join. In the example above, null-extending the (c) column
			//    violates the (a)==(c) equivalence dependency. Even the strict
			//    (a)-->(c) and (c)-->(a) dependencies no longer hold. The only
			//    dependency that still holds is (c)~~>(a), and even that is only
			//    one way, since (a)~~>(c) is not valid.
			//
			// 2. A strict dependency with both determinant and dependants on the
			//    null-supplying side of join becomes lax if all determinant columns
			//    are nullable. In the example above, null-extending the (c,d,e)
			//    columns violates a strict (d)-->(e) dependency, because the NULL
			//    "d" value now maps to both 1 and NULL. So it must be weakened to
			//    a lax dependency. But if at least one non-NULL column is part of
			//    the determinant, such as (c,d)-->(e), then the (NULL,NULL)
			//    determinant will be unique, thus preserving a strict FD.
			//
			// 3. A dependency with determinant columns drawn from both sides of
			//    the join is discarded, unless the determinant is a key for the
			//    relation. Null-extending one side of the join does not disturb
			//    the relation's keys, and keys always determine all other columns.
			//
			if fd.from.Intersects(rightCols) {
				if !fd.from.SubsetOf(rightCols) {
					// Rule #3, described above.
					if !f.ColsAreStrictKey(fd.from) {
						continue
					}
				} else {
					// Rule #1, described above (determinant is on null-supplying side).
					if !fd.to.SubsetOf(rightCols) {
						// Split the dependants by which side of the join they're on.
						laxCols := fd.to.Difference(rightCols)
						newFDs = append(newFDs, funcDep{from: fd.from, to: laxCols})
						if !fd.removeToCols(laxCols) {
							continue
						}
					}

					// Rule #2, described above. Note that this rule does not apply to
					// equivalence FDs, which remain valid.
					if fd.strict && !fd.equiv && !fd.from.Intersects(notNullInputCols) {
						newFDs = append(newFDs, funcDep{from: fd.from, to: fd.to})
						continue
					}
				}
			} else {
				// Rule #1, described above (determinant is on row-supplying side).
				if !fd.removeToCols(rightCols) {
					continue
				}
			}
		}

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}
	f.deps = f.deps[:n]

	for i := range newFDs {
		fd := &newFDs[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
	}
}

// EquivReps returns one "representative" column from each equivalency group in
// the FD set. ComputeEquivGroup can be called to obtain the remaining columns
// from each equivalency group.
func (f *FuncDepSet) EquivReps() opt.ColSet {
	var reps opt.ColSet

	// Equivalence closures are already maintained for every column.
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]
		if fd.equiv && !fd.to.Intersects(reps) {
			reps.UnionWith(fd.from)
		}
	}
	return reps
}

// ComputeEquivGroup returns the group of columns that are equivalent to the
// given column. See ComputeEquivClosure for more details.
func (f *FuncDepSet) ComputeEquivGroup(rep opt.ColumnID) opt.ColSet {
	return f.ComputeEquivClosure(opt.MakeColSet(rep))
}

// ensureKeyClosure checks whether the closure for this FD set's key (if there
// is one) includes the given columns. If not, then it adds a dependency so that
// the key determines the columns.
func (f *FuncDepSet) ensureKeyClosure(cols opt.ColSet) {
	if f.hasKey != noKey {
		closure := f.ComputeClosure(f.key)
		if !cols.SubsetOf(closure) {
			cols = cols.Difference(closure)

			// If we have a strict key, we add a strict dependency; otherwise we add a
			// lax dependency.
			strict := f.hasKey == strictKey
			f.addDependency(f.key, cols, strict, false /* equiv */)
		}
	}
}

// Verify runs consistency checks against the FD set, in order to ensure that it
// conforms to several invariants:
//
//   1. An FD determinant should not intersect its dependants.
//   2. If a constant FD is present, it's the first FD in the set.
//   3. A constant FD must be strict.
//   4. Lax equivalencies should be reduced to lax dependencies.
//   5. Equivalence determinant should be exactly one column.
//   6. The dependants of an equivalence is always its closure.
//   7. If FD set has a key, it should be a candidate key (already reduced).
//   8. Closure of key should include all known columns in the FD set.
//   9. If FD set has no key then key columns should be empty.
//
func (f *FuncDepSet) Verify() {
	for i := range f.deps {
		fd := &f.deps[i]

		if fd.from.Intersects(fd.to) {
			panic(errors.AssertionFailedf("expected FD determinant and dependants to be disjoint: %s (%d)", log.Safe(f), log.Safe(i)))
		}

		if fd.isConstant() {
			if i != 0 {
				panic(errors.AssertionFailedf("expected constant FD to be first FD in set: %s (%d)", log.Safe(f), log.Safe(i)))
			}
			if !fd.strict {
				panic(errors.AssertionFailedf("expected constant FD to be strict: %s", log.Safe(f)))
			}
		}

		if fd.equiv {
			if !fd.strict {
				panic(errors.AssertionFailedf("expected equivalency to be strict: %s (%d)", f, i))
			}

			if fd.from.Len() != 1 {
				panic(errors.AssertionFailedf("expected equivalence determinant to be single col: %s (%d)", log.Safe(f), log.Safe(i)))
			}

			if !f.ComputeEquivClosure(fd.from).Equals(fd.from.Union(fd.to)) {
				panic(errors.AssertionFailedf("expected equivalence dependants to be its closure: %s (%d)", log.Safe(f), log.Safe(i)))
			}
		}
	}

	if f.hasKey != noKey {
		if f.hasKey == strictKey {
			if reduced := f.ReduceCols(f.key); !reduced.Equals(f.key) {
				panic(errors.AssertionFailedf("expected FD to have candidate key %s: %s", reduced, f))
			}

			allCols := f.ColSet()
			allCols.UnionWith(f.key)
			if !f.ComputeClosure(f.key).Equals(allCols) {
				panic(errors.AssertionFailedf("expected closure of FD key to include all known cols: %s", log.Safe(f)))
			}
		}

		if f.hasKey == laxKey && f.key.Empty() {
			panic(errors.AssertionFailedf("expected lax key to be not empty"))
		}
	} else {
		if !f.key.Empty() {
			panic(errors.AssertionFailedf("expected empty key columns since no key: %s", f))
		}
	}
}

// StringOnlyFDs returns a string representation of the FDs (without the key
// information).
func (f FuncDepSet) StringOnlyFDs() string {
	var b strings.Builder
	f.formatFDs(&b)
	return b.String()
}

func (f FuncDepSet) String() string {
	var b strings.Builder

	if f.hasKey != noKey {
		// The key shows up as key(1,2) or lax-key(1,2).
		if f.hasKey == laxKey {
			b.WriteString("lax-")
		}
		fmt.Fprintf(&b, "key%s", f.key)
		if len(f.deps) > 0 {
			b.WriteString("; ")
		}
	}

	f.formatFDs(&b)
	return b.String()
}

func (f FuncDepSet) formatFDs(b *strings.Builder) {
	for i := range f.deps {
		if i != 0 {
			b.WriteString(", ")
		}
		f.deps[i].format(b)
	}
}

// colsAreKey returns true if the given columns contain a strict or lax key for
// the relation.
func (f *FuncDepSet) colsAreKey(cols opt.ColSet, typ keyType) bool {
	switch f.hasKey {
	case strictKey:
		// Determine whether the key is in the closure of the given columns. The
		// closure is necessary in the general case since it's possible that the
		// columns form a different key. For example:
		//
		//   f.key = (a)
		//   cols  = (b,c)
		//
		// and yet both column sets form keys for the relation.
		return f.inClosureOf(f.key, cols, typ == strictKey)

	case laxKey:
		if typ == strictKey {
			// We have a lax key but we need a strict key.
			return false
		}

		// For a lax key, we cannot use the strict closure, because the columns we
		// bring in from the closure might be null. For example, say that
		//   - column a is constant but (always) null: ()-->(a)
		//   - (a,b) is the known lax key.
		// The strict closure of (b) is the lax key (a,b), but (b) is not a lax
		// key.
		//
		// We can however use the equivalent closure, because those columns are null
		// only if one of the initial cols is null.
		//
		// Note: if we had information, we could use just the not-null columns from
		// the strict closure.
		return f.key.SubsetOf(f.ComputeEquivClosure(cols))

	default:
		return false
	}
}

// inClosureOf computes the strict or lax closure of the "in" column set, and
// returns true if the "cols" columns are all contained in the resulting
// closure.
func (f *FuncDepSet) inClosureOf(cols, in opt.ColSet, strict bool) bool {
	// Short-circuit if the "in" set already contains all the columns.
	if cols.SubsetOf(in) {
		return true
	}

	in = in.Copy()

	// Lax dependencies are not transitive (see figure 2.1 in the paper for
	// properties that hold for lax dependencies), so only include them if they
	// are reachable in a single lax dependency step from the input set.
	if !strict {
		// Keep track of all columns reached through a lax or strict dependency.
		laxIn := in.Copy()
		for i := 0; i < len(f.deps); i++ {
			fd := &f.deps[i]
			if fd.from.SubsetOf(in) && !fd.to.SubsetOf(in) {
				laxIn.UnionWith(fd.to)

				// Equivalencies are always transitive.
				if fd.equiv {
					in.UnionWith(fd.to)

					// Restart iteration to get transitive closure.
					i = -1
				}

				// Short-circuit if the "laxIn" set now contains all the columns.
				if cols.SubsetOf(laxIn) {
					return true
				}
			}
		}

		// Use the set that includes columns reached via lax dependencies.
		in = laxIn
	}

	// Now continue with full transitive closure of strict dependencies.
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]

		if fd.strict && fd.from.SubsetOf(in) && !fd.to.SubsetOf(in) {
			in.UnionWith(fd.to)

			// Short-circuit if the "in" set now contains all the columns.
			if cols.SubsetOf(in) {
				return true
			}

			// Restart iteration to get transitive closure.
			i = -1
		}
	}
	return false
}

// addDependency adds a new dependency into the set. If another FD implies the
// new FD, then it's not added. If it can be merged with an existing FD, that is
// done. Otherwise, a brand new FD is added to the set.
func (f *FuncDepSet) addDependency(from, to opt.ColSet, strict, equiv bool) {
	// Fast-path for trivial no-op dependency.
	if to.SubsetOf(from) {
		return
	}

	// Delegate equivalence dependency.
	if equiv {
		f.addEquivalency(from.Union(to))
		return
	}

	if strict {
		// Reducing the columns yields a "stronger" dependency.
		from = f.ReduceCols(from)
	}

	// Delegate constant dependency.
	if from.Empty() {
		if !strict {
			panic(errors.AssertionFailedf("expected constant FD to be strict: %s", log.Safe(f)))
		}
		f.AddConstants(to)
		return
	}

	// Any column in the "from" set is already an implied "to" column, so no
	// need to include it.
	if to.Intersects(from) {
		to = to.Difference(from)
	}

	newFD := funcDep{from: from, to: to, strict: strict, equiv: equiv}

	// Merge the new dependency into the existing set.
	n := 0
	added := false
	for i := range f.deps {
		fd := &f.deps[i]

		if newFD.implies(fd) {
			// The new FD is >= the existing FD, so can replace it.
			if added {
				// New FD is already part of the set, so discard this existing FD.
				continue
			}

			// Update the existing FD.
			fd.from = from
			fd.to = to
			fd.strict = strict
			fd.equiv = equiv

			// Keep searching, in case there's another implied FD.
			added = true
		} else if !added {
			if fd.implies(&newFD) {
				// The new FD does not add any additional information.
				added = true
			} else if fd.strict == strict && fd.equiv == equiv && fd.from.Equals(from) {
				// The new FD can at least add its determinant to an existing FD.
				fd.to = fd.to.Union(to)
				added = true
			}
		}

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}

	f.deps = f.deps[:n]

	if !added {
		// Add a new FD.
		f.deps = append(f.deps, newFD)
	}
}

func (f *FuncDepSet) addEquivalency(equiv opt.ColSet) {
	var addConst bool
	var found opt.ColSet

	// Start by finding complete set of all columns that are equivalent to the
	// given set.
	equiv = f.ComputeEquivClosure(equiv)

	n := 0
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]

		if fd.isConstant() {
			// If any equivalent column is a constant, then all are constants.
			if fd.to.Intersects(equiv) && !equiv.SubsetOf(fd.to) {
				addConst = true
			}
		} else if fd.from.SubsetOf(equiv) {
			// All determinant columns are equivalent to one another.
			if fd.equiv {
				// Ensure that each equivalent column directly maps to all other
				// columns in the group.
				fd.to = fd.to.Union(equiv)
				fd.to.DifferenceWith(fd.from)
				found.UnionWith(fd.from)
			} else {
				// Remove dependant columns that are equivalent, because equivalence
				// is a stronger relationship than a strict or lax dependency.
				if !fd.removeToCols(equiv) {
					continue
				}
			}
		}

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}
	f.deps = f.deps[:n]

	if addConst {
		// Ensure that all equivalent columns are marked as constant.
		f.AddConstants(equiv)
	}

	if !equiv.SubsetOf(found) {
		add := equiv.Difference(found)
		deps := make([]funcDep, 0, len(f.deps)+add.Len())
		deps = append(deps, f.deps...)

		for id, ok := add.Next(0); ok; id, ok = add.Next(id + 1) {
			fd := funcDep{strict: true, equiv: true}
			fd.from.Add(id)
			fd.to = equiv.Copy()
			fd.to.Remove(id)
			deps = append(deps, fd)
		}
		f.deps = deps
	}

	f.tryToReduceKey(opt.ColSet{} /* notNullCols */)
}

// setKey updates the key that the set is currently maintaining.
func (f *FuncDepSet) setKey(key opt.ColSet, typ keyType) {
	f.hasKey = typ
	f.key = key
	if f.hasKey == laxKey && f.key.Empty() {
		// An empty lax key is by definition equivalent to an empty strict key; we
		// normalize it to be strict.
		f.hasKey = strictKey
	}
}

// clearKey removes any strict or lax key.
func (f *FuncDepSet) clearKey() {
	f.setKey(opt.ColSet{}, noKey)
}

// tryToReduceKey tries to reduce any set key, used after new FDs are added.
func (f *FuncDepSet) tryToReduceKey(notNullCols opt.ColSet) {
	switch f.hasKey {
	case laxKey:
		if !notNullCols.Empty() {
			// We can only remove columns from a lax key if we know they are
			// not null; other columns must be retained.
			nullableKeyCols := f.key.Difference(notNullCols)
			if nullableKeyCols.Empty() {
				// All key columns are not-null; we can upgrade the key to strict.
				f.AddStrictKey(f.key, f.ColSet())
			} else {
				reduced := f.ReduceCols(f.key)
				reduced.UnionWith(nullableKeyCols)
				f.key = reduced
			}
		}

	case strictKey:
		f.key = f.ReduceCols(f.key)
	}
}

// makeEquivMap constructs a map with an entry for each column in the "from" set
// that is equivalent to a column in the "to" set. When there are multiple
// equivalent columns, then makeEquivMap arbitrarily chooses one of the
// alternatives. Note that some from columns may not have a mapping. If none of
// them do, then makeEquivMap returns nil.
func (f *FuncDepSet) makeEquivMap(from, to opt.ColSet) map[opt.ColumnID]opt.ColumnID {
	var equivMap map[opt.ColumnID]opt.ColumnID
	for i, ok := from.Next(0); ok; i, ok = from.Next(i + 1) {
		var oneCol opt.ColSet
		oneCol.Add(i)
		closure := f.ComputeEquivClosure(oneCol)
		closure.IntersectionWith(to)
		if !closure.Empty() {
			if equivMap == nil {
				equivMap = make(map[opt.ColumnID]opt.ColumnID)
			}
			id, _ := closure.Next(0)
			equivMap[i] = id
		}
	}
	return equivMap
}

// isConstant returns true if this FD contains the set of constant columns. If
// it exists, it must always be the first FD in the set.
func (f *funcDep) isConstant() bool {
	return f.from.Empty()
}

// implies returns true if this FD is at least as strong as the given FD. This
// is true when:
//   - the determinant is a subset of the given FD's determinant
//   - the dependant is a superset of the given FD's dependant
//   - the FD is at least as strict and equivalent as the given FD
func (f *funcDep) implies(fd *funcDep) bool {
	if f.from.SubsetOf(fd.from) && fd.to.SubsetOf(f.to) {
		if (f.strict || !fd.strict) && (f.equiv || !fd.equiv) {
			return true
		}
	}
	return false
}

// removeFromCols removes columns in the given set from this FD's determinant.
// If removing columns results in an empty determinant, then removeFromCols
// returns false.
func (f *funcDep) removeFromCols(remove opt.ColSet) bool {
	if f.from.Intersects(remove) {
		f.from = f.from.Difference(remove)
	}
	return !f.isConstant()

}

// removeToCols removes columns in the given set from this FD's dependant set.
// If removing columns results in an empty dependant set, then removeToCols
// returns false.
func (f *funcDep) removeToCols(remove opt.ColSet) bool {
	if f.to.Intersects(remove) {
		f.to = f.to.Difference(remove)
	}
	return !f.to.Empty()
}

func (f *funcDep) format(b *strings.Builder) {
	if f.equiv {
		if !f.strict {
			panic(errors.AssertionFailedf("lax equivalent columns are not supported"))
		}
		fmt.Fprintf(b, "%s==%s", f.from, f.to)
	} else {
		if f.strict {
			fmt.Fprintf(b, "%s-->%s", f.from, f.to)
		} else {
			fmt.Fprintf(b, "%s~~>%s", f.from, f.to)
		}
	}
}

func (f *funcDep) String() string {
	var b strings.Builder
	f.format(&b)
	return b.String()
}
