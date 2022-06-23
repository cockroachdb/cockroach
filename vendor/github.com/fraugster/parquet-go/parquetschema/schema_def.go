package parquetschema

import (
	"bytes"
	"fmt"
	"io"

	"github.com/fraugster/parquet-go/parquet"
)

// SchemaDefinition represents a valid textual schema definition.
type SchemaDefinition struct {
	RootColumn *ColumnDefinition
}

// ColumnDefinition represents the schema definition of a column and optionally its children.
type ColumnDefinition struct {
	Children      []*ColumnDefinition
	SchemaElement *parquet.SchemaElement
}

// SchemaDefinitionFromColumnDefinition creates a new schema definition from the provided root column definition.
func SchemaDefinitionFromColumnDefinition(c *ColumnDefinition) *SchemaDefinition {
	if c == nil {
		return nil
	}

	return &SchemaDefinition{RootColumn: c}
}

// ParseSchemaDefinition parses a textual schema definition and returns
// a SchemaDefinition object, or an error if parsing has failed. The textual schema definition
// needs to adhere to the following grammar:
//
//	message ::= 'message' <identifier> '{' <message-body> '}'
//	message-body ::= <column-definition>*
//	column-definition ::= <repetition-type> <column-type-definition>
//	repetition-type ::= 'required' | 'repeated' | 'optional'
//	column-type-definition ::= <group-definition> | <field-definition>
//	group-definition ::= 'group' <identifier> <converted-type-annotation>? '{' <message-body> '}'
//	field-definition ::= <type> <identifier> <logical-type-annotation>? <field-id-definition>? ';'
//	type ::= 'binary'
//		| 'float'
//		| 'double'
//		| 'boolean'
//		| 'int32'
//		| 'int64'
//		| 'int96'
//		| 'fixed_len_byte_array' '(' <number> ')'
//	converted-type-annotation ::= '(' <converted-type> ')'
//	converted-type ::= 'UTF8'
//		| 'MAP'
//		| 'MAP_KEY_VALUE'
//		| 'LIST'
//		| 'ENUM'
//		| 'DECIMAL'
//		| 'DATE'
//		| 'TIME_MILLIS'
//		| 'TIME_MICROS'
//		| 'TIMESTAMP_MILLIS'
//		| 'TIMESTAMP_MICROS'
//		| 'UINT_8'
//		| 'UINT_16'
//		| 'UINT_32'
//		| 'UINT_64'
//		| 'INT_8'
//		| 'INT_16'
//		| 'INT_32'
//		| 'INT_64'
//		| 'JSON'
//		| 'BSON'
//		| 'INTERVAL'
//	logical-type-annotation ::= '(' <logical-type> ')'
//	logical-type ::= 'STRING'
//		| 'DATE'
//		| 'TIMESTAMP' '(' <time-unit> ',' <boolean> ')'
//		| 'UUID'
//		| 'ENUM'
//		| 'JSON'
//		| 'BSON'
//		| 'INT' '(' <bit-width> ',' <boolean> ')'
//		| 'DECIMAL' '(' <precision> ',' <scale> ')'
//	field-id-definition ::= '=' <number>
//	number ::= <digit>+
//	digit ::= '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9'
//	time-unit ::= 'MILLIS' | 'MICROS' | 'NANOS'
//	boolean ::= 'false' | 'true'
//	identifier ::= <all-characters> - ' ' - ';' - '{' - '}' - '(' - ')' - '=' - ','
//	bit-width ::= '8' | '16' | '32' | '64'
//	precision ::= <number>
//	scale ::= <number>
//	all-characters ::= ? all visible characters ?
// For examples of textual schema definitions, please take a look at schema-files/*.schema.
func ParseSchemaDefinition(schemaText string) (*SchemaDefinition, error) {
	p := newSchemaParser(schemaText)
	if err := p.parse(); err != nil {
		return nil, err
	}

	return &SchemaDefinition{
		RootColumn: p.root,
	}, nil
}

// Clone returns a deep copy of the schema definition.
func (sd *SchemaDefinition) Clone() *SchemaDefinition {
	def, err := ParseSchemaDefinition(sd.String())
	if err != nil {
		panic(err) // this should never ever happen and indicates a serious bug.
	}
	return def
}

// String returns a textual representation of the schema definition. This textual representation
// adheres to the format accepted by the ParseSchemaDefinition function. A textual schema definition
// parsed by ParseSchemaDefinition and turned back into a string by this method repeatedly will
// always remain the same, save for differences in the emitted whitespaces.
func (sd *SchemaDefinition) String() string {
	if sd == nil || sd.RootColumn == nil {
		return "message empty {\n}\n"
	}

	buf := new(bytes.Buffer)

	fmt.Fprintf(buf, "message %s {\n", sd.RootColumn.SchemaElement.Name)

	printCols(buf, sd.RootColumn.Children, 2)

	fmt.Fprintf(buf, "}\n")

	return buf.String()
}

// SubSchema returns the direct child of the current schema definition
// that matches the provided name. If no such child exists, nil is
// returned.
func (sd *SchemaDefinition) SubSchema(name string) *SchemaDefinition {
	if sd == nil {
		return nil
	}

	for _, c := range sd.RootColumn.Children {
		if c.SchemaElement.Name == name {
			return &SchemaDefinition{
				RootColumn: c,
			}
		}
	}
	return nil
}

// SchemaElement returns the schema element associated with the current
// schema definition. If no schema element is present, then nil is returned.
func (sd *SchemaDefinition) SchemaElement() *parquet.SchemaElement {
	if sd == nil || sd.RootColumn == nil {
		return nil
	}

	return sd.RootColumn.SchemaElement
}

func printCols(w io.Writer, cols []*ColumnDefinition, indent int) {
	for _, col := range cols {
		printIndent(w, indent)

		elem := col.SchemaElement

		switch elem.GetRepetitionType() {
		case parquet.FieldRepetitionType_REPEATED:
			fmt.Fprintf(w, "repeated")
		case parquet.FieldRepetitionType_OPTIONAL:
			fmt.Fprintf(w, "optional")
		case parquet.FieldRepetitionType_REQUIRED:
			fmt.Fprintf(w, "required")
		}
		fmt.Fprintf(w, " ")

		if elem.Type == nil {
			fmt.Fprintf(w, "group %s", elem.GetName())
			if elem.ConvertedType != nil {
				fmt.Fprintf(w, " (%s)", elem.GetConvertedType().String())
			}
			fmt.Fprintf(w, " {\n")
			printCols(w, col.Children, indent+2)

			printIndent(w, indent)
			fmt.Fprintf(w, "}\n")
		} else {
			typ := getSchemaType(elem)
			fmt.Fprintf(w, "%s %s", typ, elem.GetName())
			if elem.LogicalType != nil {
				fmt.Fprintf(w, " (%s)", getSchemaLogicalType(elem.GetLogicalType()))
			} else if elem.ConvertedType != nil {
				fmt.Fprintf(w, " (%s)", elem.GetConvertedType().String())
			}
			if elem.FieldID != nil {
				fmt.Fprintf(w, " = %d", elem.GetFieldID())
			}
			fmt.Fprintf(w, ";\n")
		}
	}
}

func printIndent(w io.Writer, indent int) {
	for i := 0; i < indent; i++ {
		fmt.Fprintf(w, " ")
	}
}

func getSchemaType(elem *parquet.SchemaElement) string {
	switch elem.GetType() {
	case parquet.Type_BYTE_ARRAY:
		return "binary"
	case parquet.Type_FLOAT:
		return "float"
	case parquet.Type_DOUBLE:
		return "double"
	case parquet.Type_BOOLEAN:
		return "boolean"
	case parquet.Type_INT32:
		return "int32"
	case parquet.Type_INT64:
		return "int64"
	case parquet.Type_INT96:
		return "int96"
	case parquet.Type_FIXED_LEN_BYTE_ARRAY:
		return fmt.Sprintf("fixed_len_byte_array(%d)", elem.GetTypeLength())
	}
	return fmt.Sprintf("UT:%s", elem.GetType())
}

func getTimestampLogicalType(t *parquet.LogicalType) string {
	unit := ""
	switch {
	case t.TIMESTAMP.Unit.IsSetNANOS():
		unit = "NANOS"
	case t.TIMESTAMP.Unit.IsSetMICROS():
		unit = "MICROS"
	case t.TIMESTAMP.Unit.IsSetMILLIS():
		unit = "MILLIS"
	default:
		unit = "BUG_UNKNOWN_TIMESTAMP_UNIT"
	}
	return fmt.Sprintf("TIMESTAMP(%s, %t)", unit, t.TIMESTAMP.IsAdjustedToUTC)
}

func getTimeLogicalType(t *parquet.LogicalType) string {
	unit := ""
	switch {
	case t.TIME.Unit.IsSetNANOS():
		unit = "NANOS"
	case t.TIME.Unit.IsSetMICROS():
		unit = "MICROS"
	case t.TIME.Unit.IsSetMILLIS():
		unit = "MILLIS"
	default:
		unit = "BUG_UNKNOWN_TIMESTAMP_UNIT"
	}
	return fmt.Sprintf("TIME(%s, %t)", unit, t.TIME.IsAdjustedToUTC)
}

func getSchemaLogicalType(t *parquet.LogicalType) string {
	switch {
	case t.IsSetSTRING():
		return "STRING"
	case t.IsSetDATE():
		return "DATE"
	case t.IsSetTIMESTAMP():
		return getTimestampLogicalType(t)
	case t.IsSetTIME():
		return getTimeLogicalType(t)
	case t.IsSetUUID():
		return "UUID"
	case t.IsSetENUM():
		return "ENUM"
	case t.IsSetJSON():
		return "JSON"
	case t.IsSetBSON():
		return "BSON"
	case t.IsSetDECIMAL():
		return fmt.Sprintf("DECIMAL(%d, %d)", t.DECIMAL.Precision, t.DECIMAL.Scale)
	case t.IsSetINTEGER():
		return fmt.Sprintf("INT(%d, %t)", t.INTEGER.BitWidth, t.INTEGER.IsSigned)
	default:
		return "BUG(UNKNOWN)"
	}
}
