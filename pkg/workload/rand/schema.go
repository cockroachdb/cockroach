// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rand

import (
	gosql "database/sql"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

type Table struct {
	Name       string
	Cols       []Col
	PrimaryKey []int
}

type Col struct {
	Name          string
	DataType      *types.T
	DataPrecision int
	DataScale     int
	CDefault      gosql.NullString
	IsNullable    bool
	IsComputed    bool
}

func (t *Table) RandomRow(rng *rand.Rand, nullPct int) ([]any, error) {
	row := make([]any, len(t.Cols))
	for i, col := range t.Cols {
		nullChance := 0
		if col.IsNullable && nullPct > 0 {
			nullChance = 100 / nullPct
		}
		datum := randgen.RandDatumWithNullChance(rng, col.DataType, nullChance, false, false)

		var err error
		row[i], err = DatumToGoSQL(datum)
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

func (t *Table) MutateRow(rng *rand.Rand, nullPct int, row []any) ([]any, error) {
	mutatedRow, err := t.RandomRow(rng, nullPct)
	if err != nil {
		return nil, err
	}
	for _, primaryKeyColumn := range t.PrimaryKey {
		// TODO(jeffswenson): this doesn't quite work if there is a computed column
		// in the primary key. In that case we probably need to avoid mutating any
		// columns that contribute to the computed column value.
		mutatedRow[primaryKeyColumn] = row[primaryKeyColumn]
	}
	return mutatedRow, nil
}

// typeForOid returns the *types.T struct associated with the given
// OID. Note that for columns of type `BIT` and `CHAR`, the width is
// not recorded on the T struct itself; instead, we query the
// `information_schema.columns` view to get that information. When the
// `character_maximum_length` column is NULL, it means the column has
// variable width and we set the width of the type to 0, which will
// cause the random data generator to generate data with random width.
func typeForOid(db *gosql.DB, typeOid oid.Oid, tableName, columnName string) (*types.T, error) {
	datumType := *types.OidToType[typeOid]
	if typeOid == oid.T_bit || typeOid == oid.T_char {
		var width int32
		if err := db.QueryRow(
			`SELECT IFNULL(character_maximum_length, 0)
			FROM information_schema.columns
			WHERE table_name = $1 AND column_name = $2`,
			tableName, columnName).Scan(&width); err != nil {
			return nil, err
		}

		datumType.InternalType.Width = width
	}

	return &datumType, nil
}

// LoadTable loads a table's schema from the database.
func LoadTable(conn *gosql.DB, tableName string) (_ Table, retErr error) {
	var relid int
	sqlName := tree.Name(tableName)
	if err := conn.QueryRow("SELECT $1::REGCLASS::OID", sqlName.String()).Scan(&relid); err != nil {
		return Table{}, err
	}

	rows, err := conn.Query(`
SELECT attname, atttypid, adsrc, NOT attnotnull, attgenerated != ''
FROM pg_catalog.pg_attribute
LEFT JOIN pg_catalog.pg_attrdef
	ON attrelid=adrelid AND attnum=adnum
WHERE attrelid=$1 AND NOT attisdropped`, relid)
	if err != nil {
		return Table{}, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	var cols []Col
	var numCols = 0

	for rows.Next() {
		var c Col
		c.DataPrecision = 0
		c.DataScale = 0

		var typOid int
		if err := rows.Scan(&c.Name, &typOid, &c.CDefault, &c.IsNullable, &c.IsComputed); err != nil {
			return Table{}, err
		}
		c.DataType, err = typeForOid(conn, oid.Oid(typOid), tableName, c.Name)
		if err != nil {
			return Table{}, err
		}
		if c.CDefault.String == "unique_rowid()" { // skip
			continue
		}
		if strings.HasPrefix(c.CDefault.String, "uuid_v4()") { // skip
			continue
		}
		cols = append(cols, c)
		numCols++
	}

	if numCols == 0 {
		return Table{}, errors.New("no columns detected")
	}

	if err = rows.Err(); err != nil {
		return Table{}, err
	}

	t := Table{
		Name: tableName,
		Cols: cols,
	}

	rows, err = conn.Query(
		`
SELECT a.attname
FROM   pg_index i
JOIN   pg_attribute a ON a.attrelid = i.indrelid
					AND a.attnum = ANY(i.indkey)
WHERE  i.indrelid = $1
AND    i.indisprimary`, relid)
	if err != nil {
		return Table{}, err
	}
	defer func() { retErr = errors.CombineErrors(retErr, rows.Close()) }()
	for rows.Next() {
		var colname string
		if err := rows.Scan(&colname); err != nil {
			return Table{}, err
		}
		for i, c := range cols {
			if c.Name == colname {
				t.PrimaryKey = append(t.PrimaryKey, i)
			}
		}
	}
	if err = rows.Err(); err != nil {
		return Table{}, err
	}

	return t, nil
}

// DatumToGoSQL converts a datum to a Go type.
func DatumToGoSQL(d tree.Datum) (interface{}, error) {
	d = tree.UnwrapDOidWrapper(d)
	if d == tree.DNull {
		return nil, nil
	}
	switch d := d.(type) {
	case *tree.DBool:
		return bool(*d), nil
	case *tree.DString:
		return string(*d), nil
	case *tree.DCollatedString:
		return d.String(), nil
	case *tree.DBytes:
		return fmt.Sprintf(`x'%s'`, hex.EncodeToString([]byte(*d))), nil
	case *tree.DDate, *tree.DTime:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	case *tree.DTimestamp:
		return d.Time, nil
	case *tree.DTimestampTZ:
		return d.Time, nil
	case *tree.DInterval:
		return d.Duration.String(), nil
	case *tree.DBitArray:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	case *tree.DInt:
		return int64(*d), nil
	case *tree.DOid:
		return uint32(d.Oid), nil
	case *tree.DFloat:
		return float64(*d), nil
	case *tree.DDecimal:
		// use string representation here since randgen might generate
		// decimals that don't fit into a float64
		return d.String(), nil
	case *tree.DArray:
		arr := make([]interface{}, len(d.Array))
		for i := range d.Array {
			elt, err := DatumToGoSQL(d.Array[i])
			if err != nil {
				return nil, err
			}
			if elt == nil {
				elt = nullVal{}
			}
			arr[i] = elt
		}
		return sqlArray{arr, d.ParamTyp}, nil
	case *tree.DUuid:
		return d.UUID, nil
	case *tree.DIPAddr:
		return d.IPAddr.String(), nil
	case *tree.DJSON:
		return d.JSON.String(), nil
	case *tree.DJsonpath:
		return d.String(), nil
	case *tree.DTimeTZ:
		return d.TimeTZ.String(), nil
	case *tree.DBox2D:
		return d.CartesianBoundingBox.Repr(), nil
	case *tree.DGeography:
		return geo.SpatialObjectToEWKT(d.Geography.SpatialObject(), 2)
	case *tree.DGeometry:
		return geo.SpatialObjectToEWKT(d.Geometry.SpatialObject(), 2)
	case *tree.DPGLSN:
		return d.LSN.String(), nil
	case *tree.DTSQuery:
		return d.String(), nil
	case *tree.DTSVector:
		return d.String(), nil
	case *tree.DPGVector:
		return d.String(), nil
	}
	return nil, errors.Errorf("unhandled datum type: %s", reflect.TypeOf(d))
}
