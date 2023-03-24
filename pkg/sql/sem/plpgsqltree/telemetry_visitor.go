package plpgsqltree

// PLpgSQLStmtVisitor defines methods that are called plpgsql statements during
// a statement walk.
type PLpgSQLStmtVisitor interface {
	// Visit is called during a statement walk.
	Visit(stmt PLpgSQLStatement)
}
