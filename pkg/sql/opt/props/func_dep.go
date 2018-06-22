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
// Removing Columns
//
// Some SQL operators can remove columns from their input (e.g. GroupBy and
// Project). The library discards FDs that include these removed columns when
// possible. However, in the general case this is a difficult task, as
// illustrated in this example:
//
//   (a)~~>(b,d)
//   (b,c)-->(d)
//   (b)==(e)
//   (e)==(b)
//
// Removing "b" from these FDs would require reducing, augmenting, splitting,
// discarding, and creating FDs. In addition, the impact of strict vs. lax
// dependencies and equivalent vs. non-equivalent dependencies would need to be
// carefully considered. It can be expensive to try and maintain the minimal set
// of FDs.
//
// Instead of attempting to do that, this library maintains a "removed" set that
// tracks which columns are no longer part of the relation. The removed set is
// not necessary for correctness, but is used for aesthetics when formatting the
// FD set for output. All operations still behave correctly whether or not
// columns have been removed, retained, or marked as removed.
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
//      expensive reductions. This library does perform some simplifications
//      that are possible with a limited number of passes over the FD set.
//
//   4. The paper "colors" columns black when they are no longer part of a
//      derived relation. This library maintains a "removed" set which serves
//      the same purpose. However, as with #2, sometimes it will physically
//      remove removed columns from the FDs when that can be done with a limited
//      number of passes over the FD set.
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

	// removed is the set of columns that are no longer part of the relation,
	// but are retained in lieu of more expensive FD set simplifications. See
	// the "Removing Columns" section above for more details.
	//
	// This set is  immutable; to update it, replace it with a different set
	// containing the desired columns.
	removed opt.ColSet

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
	return len(f.deps) == 0 && f.removed.Empty() && !f.hasKey && f.key.Empty()
}

// ClearKey marks the FD set as having no key.
func (f *FuncDepSet) ClearKey() {
	f.hasKey = false
	f.key = opt.ColSet{}
}

// CopyFrom copies the given FD into this FD, replacing any existing data.
func (f *FuncDepSet) CopyFrom(fd *FuncDepSet) {
	// Make certain to copy FDs to the slice owned by this set.
	f.deps = f.deps[:0]
	f.deps = append(f.deps, fd.deps...)
	f.removed = fd.removed
	f.key = fd.key
	f.hasKey = fd.hasKey
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
		if !f.inClosureOf(removed, cols, true /* strict */) {
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
	cols = cols.Copy()
	for i := 0; i < len(f.deps); i++ {
		fd := &f.deps[i]

		if fd.equiv && fd.from.SubsetOf(cols) && !fd.to.SubsetOf(cols) {
			cols.UnionWith(fd.to)

			// Restart iteration to get transitive closure.
			i = -1
		}
	}
	return cols
}

// AddStrictKey adds a FD for a new key. The given key columns are reduced to a
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
	f.removed = opt.ColSet{}
	f.setKey(opt.ColSet{})
}

// MakeNotNull modifies the FD set based on which columns cannot contain NULL
// values. This often allows upgrading lax dependencies to strict dependencies.
func (f *FuncDepSet) MakeNotNull(notNullCols opt.ColSet) {
	var newFDs []funcDep
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
			if newFD, created := fd.splitOnStrict(notNullCols); created {
				newFDs = append(newFDs, newFD)
			}
		} else {
			// Non-constant FD can be made strict if all determinant columns are
			// not null.
			if fd.from.SubsetOf(notNullCols) {
				fd.strict = true
			}
		}
	}

	for i := range newFDs {
		fd := &newFDs[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
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

	var aSet, bSet opt.ColSet
	aSet.Add(int(a))
	bSet.Add(int(b))
	f.addDependency(aSet, bSet, true /* strict */, true /* equiv */)
	f.addDependency(bSet, aSet, true /* strict */, true /* equiv */)

	// Try to reduce the key based on the new equivalency.
	if f.hasKey {
		f.key = f.ReduceCols(f.key)
	}
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

	constDep := -1
	n := 0
	for i := range f.deps {
		fd := &f.deps[i]
		if fd.strict && !fd.equiv {
			if constDep == -1 && fd.from.Empty() {
				// Found existing constant dependency, so add to that later.
				constDep = n
			} else if fd.from.Intersects(cols) {
				// Constants in the from side of a strict dependency are no-ops,
				// so remove them. This may allow the entire FD to be removed.
				fd.from = fd.from.Difference(cols)
				if fd.from.Empty() {
					// Determinant is constant, so dependants must be as well. Remove
					// the dependency from the set (using continue will skip it).
					cols = cols.Union(fd.to)
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

	if constDep == -1 {
		// Prepend constants for small performance boost when computing closure.
		deps := make([]funcDep, len(f.deps)+1)
		deps[0] = funcDep{to: cols, strict: true}
		copy(deps[1:], f.deps)
		f.deps = deps
	} else {
		// Add to existing dependency.
		f.deps[constDep].to = f.deps[constDep].to.Union(cols)
	}

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
	if !from.Empty() {
		f.addDependency(from, colSet, true /* strict */, false /* equiv */)
	} else {
		f.AddConstants(colSet)
	}
}

// ProjectCols removes all columns that are not in the given set. It makes a
// best effort to discard any FDs that include these columns, but as explained
// in the Removing Columns section of the FuncDepSet comment, this is not always
// easy to do. If a column is not fully removed from all FDs in the set, then
// it's instead added to the removed column list in order to mark it as removed.
//
// ProjectCols does the most extensive FD simplifications, so many operators
// call this at the end of constructing their FD as a way to trigger any
// simplifications enabled by calls to other FD set methods.
func (f *FuncDepSet) ProjectCols(cols opt.ColSet) {
	if f.hasKey && !f.key.SubsetOf(cols) {
		// Need to construct new candidate key based only on projected columns,
		// if that's possible.
		if f.ColsAreStrictKey(cols) {
			f.setKey(f.ReduceCols(cols))
		} else {
			f.ClearKey()
		}
	}

	// Special case of no columns.
	if cols.Empty() {
		f.deps = f.deps[:0]
		f.removed = opt.ColSet{}
		return
	}

	// Special case of <= 1 row.
	if f.hasKey && f.key.Empty() {
		f.MakeMax1Row(cols)
		return
	}

	// Check for special cases, in which columns to remove only appear on one
	// side of FD's, or if every determinant contains a removed column.
	var onlyFrom, onlyTo opt.ColSet
	removeAll := true
	for i := range f.deps {
		fd := &f.deps[i]

		// Skip lax dependencies with removed columns in the determinant, as
		// they will always be removed below.
		if !fd.strict && !fd.from.SubsetOf(cols) {
			continue
		}

		onlyFrom.UnionWith(fd.from)
		onlyTo.UnionWith(fd.to)
		if fd.from.SubsetOf(cols) {
			removeAll = false
		}
	}

	if removeAll {
		// Every determinant contains a removed column, so no matches are possible.
		f.deps = f.deps[:0]
		f.removed = opt.ColSet{}
		return
	}

	// onlyFrom now contains all determinants, and onlyTo contains all dependants.
	// Subtract onlyFrom and onlyTo from each other in order to find columns to
	// remove that appear on only one side.
	tempFrom := onlyFrom.Copy()
	onlyFrom.DifferenceWith(onlyTo)
	onlyFrom.DifferenceWith(cols)
	onlyTo.DifferenceWith(tempFrom)
	onlyTo.DifferenceWith(cols)

	// Accumulate all columns used by the FD set.
	f.removed = f.key.Copy()
	n := 0
	for i := range f.deps {
		fd := &f.deps[i]

		// Remove onlyTo columns from dependants, since they never determine
		// other columns, and therefore aren't needed for transitivity.
		if fd.to.Intersects(onlyTo) {
			fd.to = fd.to.Difference(onlyTo)
			if fd.to.SubsetOf(fd.from) {
				// Discard FD entirely, as it's now a no-op.
				continue
			}
		}

		// Remove all dependencies containing onlyFrom columns, as they can
		// never be satisfied again.
		if fd.from.Intersects(onlyFrom) {
			continue
		}

		// Remove all lax dependencies with removed columns in the determinant,
		// as they can never be satisfied, and can't be used for transitivity.
		if !fd.strict && !fd.from.SubsetOf(cols) {
			continue
		}

		f.removed.UnionWith(fd.from)
		f.removed.UnionWith(fd.to)

		if n != i {
			f.deps[n] = f.deps[i]
		}
		n++
	}
	f.deps = f.deps[:n]

	// Removed columns are all columns in use minus projected columns.
	f.removed.DifferenceWith(cols)
}

// AddFrom merges two FD sets by adding each FD from the given set to this set.
// While this requires O(N**2) time, it's useful when the two FD sets may
// overlap one another and substantial simplifications are possible.
func (f *FuncDepSet) AddFrom(fd *FuncDepSet) {
	for i := range fd.deps {
		fd := &fd.deps[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
	}
}

// MakeProduct modifies the FD set to reflect the impact of a cartesian product
// operation between this set and the given set. The result is a union of the
// FDs from each set, as well as a union of their keys. The two FD sets are
// expected to operate on disjoint columns, so the FDs from each are simply
// concatenated, rather than simplified via calls to addDependency.
func (f *FuncDepSet) MakeProduct(fd *FuncDepSet) {
	f.deps = append(f.deps, fd.deps...)
	f.removed = f.removed.Union(fd.removed)
	if f.hasKey && fd.hasKey {
		f.setKey(f.key.Union(fd.key))
	} else {
		f.ClearKey()
	}
}

// MakeOuter modifies the FD set to reflect the impact of adding NULL-extended
// rows to the results of an inner join. An inner join can be modeled as a
// cartesian product + ON filtering, and an outer join is modeled as an inner
// join + union of NULL-extended rows. MakeOuter performs the final step, given
// the set of columns that will be null-extended (i.e. columns from the
// null-providing side(s) of the join), as well as the set of all not null
// columns in the relation (from both sides of join).
//
// See the "Left outer join" section on page 84 of the Master's Thesis for
// the impact of outer joins on FDs.
func (f *FuncDepSet) MakeOuter(nullExtendedCols, notNullCols opt.ColSet) {
	var newFDs []funcDep
	var laxCols opt.ColSet
	for i := range f.deps {
		fd := &f.deps[i]

		// Null-extended constant dependency becomes lax (i.e. it may be either
		// its previous constant value or NULL after extension).
		if fd.from.Empty() {
			if fd.to.Intersects(nullExtendedCols) {
				laxCols.UnionWith(fd.to.Intersection(nullExtendedCols))
				if newFD, created := fd.splitOnStrict(fd.to.Difference(nullExtendedCols)); created {
					newFDs = append(newFDs, newFD)
				}
			}
			continue
		}

		if fd.from.Intersects(nullExtendedCols) {
			if !fd.from.SubsetOf(nullExtendedCols) || !fd.to.SubsetOf(nullExtendedCols) {
				allCols := fd.from.Union(fd.to).Difference(nullExtendedCols).Difference(f.removed)
				if !allCols.Empty() {
					panic(fmt.Sprintf("outer join dependencies cannot cross join boundary: %s", f))
				}
			}

			// Strict dependency on null-supplying side of join becomes lax if all
			// determinant columns are nullable. For example, given this join and
			// set of result rows:
			//
			//   SELECT * FROM ab LEFT OUTER JOIN cde ON a=c AND b=4
			//
			//   a  b  c     d     e
			//   ----------------------
			//   1  4  1     NULL  2
			//   2  5  NULL  NULL  NULL
			//   3  6  NULL  NULL  NULL
			//
			// Null-extending the (c,d,e) columns violates a strict (d)-->(e)
			// dependency, because the NULL "d" value now maps to both 2 and NULL. So
			// it must be weakened to a lax dependency. But if at least one non-NULL
			// column is part of the determinant, such as (c,d)-->(e), then the
			// (NULL,NULL) determinant will be unique, thus preserving a strict FD.
			if fd.strict && !fd.equiv && !fd.from.Intersects(notNullCols) {
				laxCols.UnionWith(fd.to)
				fd.makeLax()
			}
		} else {
			if fd.to.Intersects(nullExtendedCols) && !fd.from.SubsetOf(f.removed) {
				panic(fmt.Sprintf("outer join dependencies cannot cross join boundary: %s", f))
			}
		}
	}

	for i := range newFDs {
		fd := &newFDs[i]
		f.addDependency(fd.from, fd.to, fd.strict, fd.equiv)
	}

	// Note that there is no impact on any key that may be present for the
	// relation. If there is a key, then that means the row-providing side of
	// the join had its own key, and any added rows will therefore be unique.
	// However, the key's closure may no longer include all columns in the
	// relation, due to making FDs lax, so add a new strict FD now.
	if f.hasKey && !laxCols.Empty() {
		f.addDependency(f.key, laxCols, true /* strict */, false /* equiv */)
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
	if !f.removed.Empty() {
		fmt.Fprintf(&buf, " [removed: %s]", f.removed)
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
	return f.inClosureOf(f.key, cols, strict)
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

// setKey updates the key that the set is currently maintaining.
func (f *FuncDepSet) setKey(key opt.ColSet) {
	f.hasKey = true
	f.key = key
}

// makeLax sets the strict flag to false, indicating a lax dependency. In
// addition, this library does not currently support lax equivalencies, so
// that's just mapped into a regular lax dependency.
func (f *funcDep) makeLax() {
	f.strict = false
	f.equiv = false
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

// splitOnStrict splits this FD into two: one FD contains all the columns that
// are in the given strictCols set, and one contains all the columns that are
// not in that set. If one of the two sets is empty, then this FD is simply
// updated to include the non-empty set, and created is false. Otherwise, this
// FD is updated to include the lax set, a new FD containing the strict columns
// is returned, and created is true.
func (f *funcDep) splitOnStrict(strictCols opt.ColSet) (strictFD funcDep, created bool) {
	// If all dependant columns are strict, then just set the strict flag.
	if f.to.SubsetOf(strictCols) {
		f.strict = true
		return funcDep{}, false
	}

	// If none of the dependant columns are strict, then modify existing FD.
	if !f.to.Intersects(strictCols) {
		f.makeLax()
		return funcDep{}, false
	}

	// Some dependant columns are strict and some are not, so need new FD.
	strictCols = f.to.Intersection(strictCols)
	f.to = f.to.Difference(strictCols)
	f.strict = false
	return funcDep{from: f.from, to: strictCols, strict: true, equiv: false}, true
}
