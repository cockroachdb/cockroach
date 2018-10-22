package testcat

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/vtable"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var informationSchemaMap = map[string]*tree.CreateTable{}

var informationSchemaTables = []string{
	vtable.InformationSchemaColumns,
	vtable.InformationSchemaAdministrableRoleAuthorizations,
	vtable.InformationSchemaApplicableRoles,
	vtable.InformationSchemaColumnPrivileges,
	vtable.InformationSchemaSchemata,
	vtable.InformationSchemaTables,
}

func init() {
	// Build a map that maps the names of the various information_schema tables
	// to their CREATE TABLE AST.
	for _, table := range informationSchemaTables {
		parsed, err := parser.ParseOne(table)
		if err != nil {
			panic(fmt.Sprintf("error initializing virtual table map: %s", err))
		}

		name, err := parsed.(*tree.CreateTable).Table.Normalize()
		if err != nil {
			panic(fmt.Sprintf("error initializing virtual table map: %s", err))
		}

		ct, ok := parsed.(*tree.CreateTable)
		if !ok {
			panic("virtual table schemas must be CREATE TABLE statements")
		}

		informationSchemaMap[name.TableName.String()] = ct
	}
}

// Resolve returns true and the AST node describing the virtual table referenced.
// TODO(justin): make this complete for all virtual tables.
func resolveVTable(name *tree.TableName) (*tree.CreateTable, bool) {
	switch name.SchemaName {
	case "information_schema":
		schema, ok := informationSchemaMap[name.TableName.String()]
		return schema, ok
	}

	return nil, false
}
