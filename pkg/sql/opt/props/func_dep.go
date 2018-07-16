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

package props

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// FuncDepSet is a set of functional dependencies (FDs) that encode useful
// relationships between columns in a base or derived relation. Given two sets
// of columns A and B, a functional dependency A-->B holds if A uniquely
// determines B. In other words, if two different rows have equal values for
// columns in A, then those two rows will also have equal values for columns in
// B. For example:
//
//   a1 a2 b1
//   --------
//   1  2  5
//   1  2  5
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
// those same semantics. As an example, the following rows would be be valid for
// the dependency (b)-->(c):
//
//   b     c
//   ----------
//   1     NULL
//   1     NULL
//   NULL  1
//   NULL  1
//   NULL  NULL
//   NULL  NULL
//
// but these rows would be invalid:
//
//   b     c
//   ----------
//   NULL  1
//   NULL  NULL
//
// Unique constraints allow the latter case, however, and therefore it is
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
// In contrast to strict dependencies, lax dependencies treat NULLs as distinct
// from one another, with equality semantics identical to this SQL expression:
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
// the columns of A are not-NULL. The example rows shown above that were invalid
// for a strict dependency are valid for a lax dependency:
//
//   b     c
//   ----------
//   NULL  1
//   NULL  NULL
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
// Keys
//
// A key is a set of columns that have a unique composite value for every row in
// the relation. When this library uses the term "key", it always refers to a
// strict key, in which case NULL values are treated as equal to one another:
//
//   b     c
//   --------
//   1     10
//   2     20
//   NULL  30
//
// Here, "b" is a key for the relation, even though it contains a NULL value,
// because there is only one such value. The SQL GROUP BY operator uses the same
// semantics for grouping (it's no coincidence that the definition for strict
// keys follows that lead).
//
// FuncDepSet tracks whether at least one key exists for the relation. If this
// is true, then all possible keys for the relation can be enumerated using the
// FD set. This is because any subset of columns forms a key if its FD closure
// contains every column in the relation. Therefore, all keys can be brute force
// enumerated by checking the closure of each combination in the power set.
//
// In practice, it is never necessary to enumerate all possible keys (fortunate,
// since there can be O(2**N) of them), since the vast majority of them turn out
// to have redundant columns that can be functionally determined from other
// columns in the key. Of more value is the set of "candidate keys", which are
// keys that contain no redundant columns. Removing any column from such a key
// causes it to longer be a key. It is possible to enumerate the set of
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

	// hasKey is true if the relation has no duplicate rows, which means at least
	// one subset of its columns form a key (all columns, if no other subset).
	// The key field contains one such key. See the "Keys" section above for more
	// details.
	hasKey bool

	// key contains a set of columns that form a key for the relation, as long as
	// hasKey is true. There is no guarantee that the key has the minimum possible
	// number of columns, or even that it's a candidate key, but a best effort is
	// made to keep it as short as possible. See the "Keys" section above for
	// more details.
	//
	// This set is immutable; to update it, replace it with a different set
	// containing the desired columns.
	key opt.ColSet
}

// funcDep stores a single functional dependency. See the comment for FuncDepSet
// for more details.
type funcDep struct {
	// from is the determinant of the functional dependency (easier to read the
	// code when "from" is used rather than "determinant").
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

// Key returns a boolean indicating whether a key exists for the relation, as
// well as one of the keys if the boolean is true. A best effort is made to
// return a candidate key that has the fewest columns.
func (f *FuncDepSet) Key() (_ opt.ColSet, ok bool) {
	return f.key, f.hasKey
}

// Empty is true if the set contains no FDs and no key.
func (f *FuncDepSet) Empty() bool {
	return len(f.deps) == 0 && !f.hasKey && f.key.Empty()
}

// ColSet returns all columns referenced by the FD set.
func (f *FuncDepSet) ColSet() opt.ColSet {
	var cols opt.ColSet
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]
		cols.UnionWith(fd.from)
		cols.UnionWith(fd.to)
	}
	return cols
}

// HasMax1Row returns true if the relation has zero or one rows.
func (f *FuncDepSet) HasMax1Row() bool {
	return f.hasKey && f.key.Empty()
}

// ClearKey marks the FD set as having no key.
func (f *FuncDepSet) ClearKey() {
	f.hasKey = false
	f.key = opt.ColSet{}
}

// CopyFrom copies the given FD into this FD, replacing any existing data.
func (f *FuncDepSet) CopyFrom(fdset *FuncDepSet) {
	// Make certain to copy FDs to the slice owned by this set.
	f.deps = f.deps[:0]
	f.deps = append(f.deps, fdset.deps...)
	f.key = fdset.key
	f.hasKey = fdset.hasKey
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
	return f.colsAreKey(cols, true /* strict */)
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
	return f.colsAreKey(cols, false /* strict */)
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
		if !f.InClosureOf(removed, cols, true /* strict */) {
			// The column is not functionally determined by the other columns, so
			// retain it in the set.
			cols.Add(i)
		}
		removed.Remove(i)
	}
	return cols
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
	for i := 0; i < len(f.deps); i++ {
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
		panic("allCols does not include keyCols")
	}

	// Ensure we have candidate key (i.e. has no columns that are functionally
	// determined by other columns).
	keyCols = f.ReduceCols(keyCols)
	f.addDependency(keyCols, allCols, true /* strict */, false /* equiv */)

	if !f.hasKey || keyCols.Len() < f.key.Len() {
		f.setKey(keyCols)
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
		panic("allCols does not include keyCols")
	}

	// Ensure we have candidate key (i.e. has no columns that are functionally
	// determined by other columns).
	keyCols = f.ReduceCols(keyCols)
	f.addDependency(keyCols, allCols, false /* strict */, false /* equiv */)
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
func (f *FuncDepSet) MakeMax1Row(cols opt.ColSet) {
	f.deps = f.deps[:0]
	if !cols.Empty() {
		f.deps = append(f.deps, funcDep{to: cols, strict: true})
	}
	f.setKey(opt.ColSet{})
}

// MakeNotNull modifies the FD set based on which columns cannot contain NULL
// values. This often allows upgrading lax dependencies to strict dependencies.
func (f *FuncDepSet) MakeNotNull(notNullCols opt.ColSet) {
	var constCols opt.ColSet
	var laxFDs util.FastIntSet
	for i := range f.deps {
		fd := &f.deps[i]
		if fd.strict {
			continue
		}

		if fd.from.Empty() {
			// Constant value FD can be made strict if the constant itself is
			// not NULL. A lax constant FD means that the value can be either
			// a single value or else NULL, so eliminating the NULL possibility
			// means it has a single definite value (i.e. is strict).
			constCols.UnionWith(fd.to)
			constCols.IntersectionWith(notNullCols)
		} else {
			// Non-constant FD can be made strict if all determinant columns are
			// not null.
			if fd.from.SubsetOf(notNullCols) {
				laxFDs.Add(i)
			}
		}
	}

	if !constCols.Empty() {
		f.AddConstants(constCols)
	}

	for i, ok := laxFDs.Next(0); ok; i, ok = laxFDs.Next(i + 1) {
		fd := &f.deps[i]
		f.addDependency(fd.from, fd.to, true /* strict */, false /* equiv */)
	}

	// Try to reduce the key based on any new strict FDs.
	if f.hasKey {
		f.key = f.ReduceCols(f.key)
	}
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
	equiv.Add(int(a))
	equiv.Add(int(b))
	f.addEquivalency(equiv)
}

// AddConstants adds a strict FD to the set that declares the given column as
// having the same constant value for all rows. If the column is nullable, then
// its value may be NULL, but then the column must be NULL for all rows. For
// column "a", the FD looks like this:
//
//   ()-->(a)
//
func (f *FuncDepSet) AddConstants(cols opt.ColSet) {
	if cols.Empty() {
		return
	}

	// Determine complete set of constants by computing closure.
	cols = f.ComputeClosure(cols)

	// Ensure that first FD in the set is a strict constant FD and make sure the
	// constants are part of it.
	if len(f.deps) == 0 || !f.deps[0].hasStrictConstants() {
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

	// Try to reduce the key based on the new constants.
	if f.hasKey {
		f.key = f.ReduceCols(f.key)
	}
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
	if from.Contains(int(col)) {
		panic("synthesized column cannot depend upon itself")
	}

	var colSet opt.ColSet
	colSet.Add(int(col))
	f.addDependency(from, colSet, true /* strict */, false /* equiv */)
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
	if f.hasKey && !f.key.SubsetOf(cols) {
		// Derive new candidate key (or key is no longer possible).
		if f.ColsAreStrictKey(cols) {
			f.setKey(f.ReduceCols(cols))
		} else {
			f.ClearKey()
		}
	}

	// Special case of <= 1 row.
	if f.hasKey && f.key.Empty() {
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

		// Remember strict constant columns.
		if fd.strict && fd.from.Empty() {
			constCols = fd.to
		}

		// Add closures to dependants containing un-projected columns.
		if !fd.to.SubsetOf(cols) {
			// Equivalence dependencies already maintain closure, so skip them.
			if !fd.equiv {
				fd.to = f.ComputeClosure(fd.to)
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
			if !fd.from.Empty() {
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
				if id, foundAll = equivMap[opt.ColumnID(c)]; !foundAll {
					break
				}
				afterCols.Add(int(id))
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

// MakeProduct modifies the FD set to reflect the impact of a cartesian product
// operation between this set and the given set. The result is a union of the
// FDs from each set, as well as a union of their keys. The two FD sets are
// expected to operate on disjoint columns, so the FDs from each are simply
// concatenated, rather than simplified via calls to addDependency (except for
// case of constant columns).
func (f *FuncDepSet) MakeProduct(fdset *FuncDepSet) {
	for i := range fdset.deps {
		fd := &fdset.deps[i]
		if fd.from.Empty() {
			f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
		} else {
			f.deps = append(f.deps, *fd)
		}
	}

	if f.hasKey && fdset.hasKey {
		f.setKey(f.key.Union(fdset.key))
	} else {
		f.ClearKey()
	}
}

// MakeApply modifies the FD set to reflect the impact of an apply join. This
// FD set reflects the properties of the outer query, and the given FD set
// reflects the properties of the inner query. The FD set from the inner query
// is mostly ignored, since most FDs no longer hold. For example:
//
//   SELECT *
//   FROM a
//   INNER JOIN LATERAL (SELECT * FROM b WHERE b.y=a.y)
//   ON True
//
// 1. The constant dependency created from the outer column reference b.y=a.y
//    does not hold for the Apply operator.
// 2. If a strict dependency (b.x,b.y)-->(b.z) held, it would have been reduced
//    to (b.x)-->(b.z) because (b.y) is constant in the inner query. However,
//    (b.x)-->(b.z) does not hold for the Apply operator, since (b.y) is not
//    constant in that case.
// 3. Lax dependencies can be invalidated for similar reasons to #2.
// 4. Equivalence dependencies in the inner query do still hold for the Apply
//    operator.
// 5. Any key for the inner query cannot be inherited by the Apply operator,
//    since it may have been reduced by an outer column constant, such as if the
//    strict dependency in #2 had been a key. The exception is if the inner
//    query returns at most one row, in which case a key can be derived for the
//    Apply operator based on the outer query's key.
//
// Given that nearly all dependencies are invalidated by the Apply, and since
// the hope is that the Apply can be eliminated anyway (via decorrelation),
// MakeApply ignores most of the inner query's FD information.
func (f *FuncDepSet) MakeApply(inner *FuncDepSet) {
	if inner.HasMax1Row() {
		if f.hasKey {
			// Ensure that outer FD set's key references all columns from the inner
			// FD set.
			f.addDependency(f.key, inner.ColSet(), true /* strict */, false /* equiv */)
		}
	} else {
		f.ClearKey()
	}
}

// MakeOuter modifies the FD set to reflect the impact of adding NULL-extended
// rows to the results of an inner join. An inner join can be modeled as a
// cartesian product + ON filtering, and an outer join is modeled as an inner
// join + union of NULL-extended rows. MakeOuter performs the final step, given
// the set of columns that will be null-extended (i.e. columns from the
// null-providing side(s) of the join), as well as the set of input columns from
// both sides of the join that are not null.
//
// See the "Left outer join" section on page 84 of the Master's Thesis for
// the impact of outer joins on FDs.
func (f *FuncDepSet) MakeOuter(nullExtendedCols, notNullCols opt.ColSet) {
	var newFDs []funcDep

	n := 0
	for i := range f.deps {
		fd := &f.deps[i]

		if fd.from.Empty() {
			// Null-extended constant dependency becomes lax (i.e. it may be either
			// its previous constant value or NULL after extension).
			if fd.strict && fd.to.Intersects(nullExtendedCols) {
				laxConstCols := fd.to.Intersection(nullExtendedCols)
				newFDs = append(newFDs, funcDep{from: fd.from, to: laxConstCols})
				if !fd.removeToCols(laxConstCols) {
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
			if fd.from.Intersects(nullExtendedCols) {
				if !fd.from.SubsetOf(nullExtendedCols) {
					if !f.ColsAreStrictKey(fd.from) {
						panic(fmt.Sprintf("determinant cannot contain columns from both sides of join: %s", f))
					}
				} else {
					// Rule #1, described above (determinant is on null-supplying side).
					if !fd.to.SubsetOf(nullExtendedCols) {
						// Split the dependants by which side of the join they're on.
						laxCols := fd.to.Difference(nullExtendedCols)
						newFDs = append(newFDs, funcDep{from: fd.from, to: laxCols})
						if !fd.removeToCols(laxCols) {
							continue
						}
					}

					// Rule #2, described above. Note that this rule does not apply to
					// equivalence FDs, which remain valid.
					if fd.strict && !fd.equiv && !fd.from.Intersects(notNullCols) {
						newFDs = append(newFDs, funcDep{from: fd.from, to: fd.to})
						continue
					}
				}
			} else {
				// Rule #1, described above (determinant is on row-supplying side).
				if !fd.removeToCols(nullExtendedCols) {
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

	// Note that there is no impact on any key that may be present for the
	// relation. If there is a key, then that means the row-providing side of
	// the join had its own key, and any added rows will therefore be unique.
	// However, the key's closure may no longer include all columns in the
	// relation, due to removing FDs and/or making them lax, so if necessary,
	// add a new strict FD that ensures the key's closure is maintained.
	f.ensureKeyClosure(f.ColSet())
}

// ensureKeyClosure checks whether the closure for this FD set's key (if there
// is one) includes the given columns. If not, then it adds a strict dependency
// so that the key determines the columns.
func (f *FuncDepSet) ensureKeyClosure(cols opt.ColSet) {
	if f.hasKey {
		closure := f.ComputeClosure(f.key)
		if !cols.SubsetOf(closure) {
			cols.DifferenceWith(closure)
			f.addDependency(f.key, cols, true /* strict */, false /* equiv */)
		}
	}
}

// Verify runs consistency checks against the FD set, in order to ensure that it
// conforms to several invariants:
//
//   1. An FD determinant should not intersect its dependants.
//   2. If a strict constant FD is present, it's the first FD in the set.
//   3. Lax equivalencies should be reduced to lax dependencies.
//   4. Equivalence determinant should be exactly one column.
//   5. The dependants of an equivalence is always its closure.
//   6. If FD set has a key, it should be a candidate key (already reduced).
//   7. Closure of key should include all known columns in the FD set.
//   8. If FD set has no key then key columns should be empty.
//
func (f *FuncDepSet) Verify() {
	for i := range f.deps {
		fd := &f.deps[i]

		if fd.from.Intersects(fd.to) {
			panic(fmt.Sprintf("expected FD determinant and dependants to be disjoint: %s (%d)", f, i))
		}

		if fd.strict && fd.from.Empty() {
			if i != 0 {
				panic(fmt.Sprintf("expected strict constant FD to be first FD in set: %s (%d)", f, i))
			}
		}

		if fd.equiv {
			if !fd.strict {
				panic(fmt.Sprintf("unexpected lax equivalency: %s (%d)", f, i))
			}

			if fd.from.Len() != 1 {
				panic(fmt.Sprintf("expected equivalence determinant to be single col: %s (%d)", f, i))
			}

			if !f.ComputeEquivClosure(fd.from).Equals(fd.from.Union(fd.to)) {
				panic(fmt.Sprintf("expected equivalence dependants to be its closure: %s (%d)", f, i))
			}
		}
	}

	if f.hasKey {
		if reduced := f.ReduceCols(f.key); !reduced.Equals(f.key) {
			panic(fmt.Sprintf("expected FD to have candidate key: %s", f))
		}

		allCols := f.ColSet()
		allCols.UnionWith(f.key)
		if !f.ComputeClosure(f.key).Equals(allCols) {
			panic(fmt.Sprintf("expected closure of FD key to include all known cols: %s", f))
		}
	} else {
		if !f.key.Empty() {
			panic(fmt.Sprintf("expected empty key columns since no key: %s", f))
		}
	}
}

func (f FuncDepSet) String() string {
	var buf bytes.Buffer
	if f.hasKey {
		fmt.Fprintf(&buf, "%s: ", f.key)
	}
	for i := range f.deps {
		fd := &f.deps[i]
		if i != 0 {
			buf.WriteString(", ")
		}
		if fd.equiv {
			if !fd.strict {
				panic("lax equivalent columns are not supported")
			}
			fmt.Fprintf(&buf, "%s==%s", fd.from, fd.to)
		} else {
			if fd.strict {
				fmt.Fprintf(&buf, "%s-->%s", fd.from, fd.to)
			} else {
				fmt.Fprintf(&buf, "%s~~>%s", fd.from, fd.to)
			}
		}
	}
	return buf.String()
}

// colsAreKey returns true if the given columns contain a strict or lax key for
// the relation.
func (f *FuncDepSet) colsAreKey(cols opt.ColSet, strict bool) bool {
	if !f.hasKey {
		// No key exists for the relation.
		return false
	}

	// Determine whether the key is in the closure of the given columns. The
	// closure is necessary in the general case since it's possible that the
	// columns form a different key. For example:
	//
	//   f.key = (a)
	//   cols  = (b,c)
	//
	// and yet both column sets form keys for the relation.
	return f.InClosureOf(f.key, cols, strict)
}

// InClosureOf computes the strict or lax closure of the "in" column set, and
// returns true if the "cols" columns are all contained in the resulting
// closure.
func (f *FuncDepSet) InClosureOf(cols, in opt.ColSet, strict bool) bool {
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

				// Equivalencies and constants are always transitive.
				if fd.equiv || fd.from.Empty() {
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

	// Delegate strict constant dependency.
	if strict && from.Empty() {
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

		if fd.from.Empty() {
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

	// Try to reduce the key based on the new equivalency.
	if f.hasKey {
		f.key = f.ReduceCols(f.key)
	}
}

// setKey updates the key that the set is currently maintaining.
func (f *FuncDepSet) setKey(key opt.ColSet) {
	f.hasKey = true
	f.key = key
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
			equivMap[opt.ColumnID(i)] = opt.ColumnID(id)
		}
	}
	return equivMap
}

func (f *funcDep) hasStrictConstants() bool {
	return f.strict && f.from.Empty()
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
	return !f.from.Empty()

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
