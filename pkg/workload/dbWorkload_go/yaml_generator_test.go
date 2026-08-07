package dbworkloadgo

import (
	"os"
	"testing"
)

func TestDdlToYamlGeneration(t *testing.T) {
	//tweak as needed
	zipDir := "/Users/pradyumagarwal/workloads/git-dbworkload/dbworkload_intProj/debug-zips/debug_2" // make sure this path exists
	dbName := "tpcc"                                                                                 // use your actual test DB name
	outputFile := "test_output.yaml"                                                                 // generated YAML file

	// ---- STEP 1: Generate schema from DDL ----
	schemas, err := GenerateDDLs(
		zipDir,
		dbName,
		"",               // no DDL output directory 		// no cluster URL
		"ddl_output.sql", // will still dump DDLs here
		false,
	)
	if err != nil {
		t.Fatalf("GenerateDDLs failed: %v", err)
	}

	t.Logf("✅ Parsed %d tables", len(schemas))
	for name, schema := range schemas {
		t.Logf("Table: %s\n%s", name, schema.String())
	}

	// ---- STEP 2: Convert schema to YAML ----
	yamlOut, err := ddlToYamlCA(schemas, dbName)
	if err != nil {
		t.Fatalf("YAML generation failed: %v", err)
	}

	t.Log("✅ YAML generation succeeded")
	t.Logf("YAML Output:\n%s", yamlOut)

	// ---- STEP 3: Write YAML to file ----
	if err := os.WriteFile(outputFile, []byte(yamlOut), 0644); err != nil {
		t.Fatalf("Error writing YAML output to file: %v", err)
	}

	t.Logf("✅ YAML written to %s", outputFile)
}
