package workload_generator

const (

	// SQL keywords for constraints
	sqlIndex      = "INDEX"
	null          = "NULL"        // null is a constant string used to represent NULL values in column definitions
	notNull       = "NOT NULL"    // notNull is a constant string used to represent NOT NULL constraints in column definitions
	sqlPrimaryKey = "PRIMARY KEY" // sqlPrimaryKey is a constant string used to represent primary key constraints in column definitions
	sqlUnique     = "UNIQUE"      // unique is a constant string used to represent unique constraints in column definitions
	sqlDefault    = "DEFAULT"     // default is a constant string used to represent default value expressions in column definitions
	sqlCheck      = "CHECK"       // check is a constant string used to represent CHECK constraints in column definitions
	sqlForeignKey = "FOREIGN KEY" // foreignKey is a constant string used to represent foreign key constraints in column definitions
)
