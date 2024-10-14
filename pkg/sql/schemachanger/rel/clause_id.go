// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

// clauseIDBuilder is a helper struct to keep track of IDs for clauses. The IDs
// are meant to be used to identify the clause that created a slot for the query.
// They refer back to an entry in the Query.Clause(). For this reason, the IDs
// frozen when generating clause IDs for a subquery.
type clauseIDBuilder struct {
	nextClauseID int
	isSubquery   bool
}

// nextID will return the ID of the next clause. If we are in a subquery, the
// ID remains fixed so that it can refer back to the parent clause.
func (c *clauseIDBuilder) nextID() int {
	n := c.nextClauseID
	if !c.isSubquery {
		c.nextClauseID++
	}
	return n
}

// newBuilderForSubquery will return a new clauseIDBuilder that can be used to
// assign IDs to clauses found in a subquery.
func (c *clauseIDBuilder) newBuilderForSubquery() *clauseIDBuilder {
	cib := &clauseIDBuilder{
		nextClauseID: c.nextClauseID, isSubquery: true,
	}
	c.nextID() // Advance the clauseID for the parent query.
	return cib
}
