// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eavtest

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavquery"
	"github.com/cockroachdb/datadriven"
	"gopkg.in/yaml.v2"
)

// DataDriven is a toolkit for running data driven tests using eav and
// eavquery.
type DataDriven struct {
	schema         *schema
	entities       map[*entity]string
	entitiesByName map[string]eav.Entity
	dbs            map[string]eav.DatabaseWriter
	queries        map[string]eavquery.Query
}

// NewDataDriven construct a new DataDriven.
func NewDataDriven() *DataDriven {
	return &DataDriven{
		entities:       make(map[*entity]string),
		entitiesByName: make(map[string]eav.Entity),
		dbs:            make(map[string]eav.DatabaseWriter),
		queries:        make(map[string]eavquery.Query),
	}
}

// Cmd executes the datadriven command. The commands accepted are as follows:
//
//  schema name=<string>
//    Initializes the schema, takes a yaml map of attribute name to type.
//
//  entities
//    Defines entities. Takes a yaml map of entity name to a yaml map
//    of attributes to values. The entity name must be unique. The map
//    of values must conform to the proper type for the attribute.
//
//  new-db name=<string>
//    Defines a new database which can be used in subsequent operations.
//    The name must be unique.
//
//  insert db=<string>
//    Upserts entities into the specified database. Take a yaml list of
//    entity names.
//
//  iterate db=<string>
//    Iterates a database. Optionally take a where clause defined as a
//    yaml map of attribute to value. Only entities which have been previously
//    inserted into the db will be printed. Prints a lexicographically sorted
//    list of entity names.
//
//  query name=<string>
//    Defines a new query. The DSL for the query is embedded in a yaml list
//    which may contain various types of statements. The entries can either
//    be a mapping of an entity-attribute to a constant (1) or a referenced
//    entity-attribute (2) or a string expression indicating either an inequality between
//    entity-attribute pairs (3) or between entities (4). Imagine we have entities
//    e1 and e2 and attributes a1 and a2 where a1 is a string and a2 is a number.
//    The following would be a valid query:
//     - e1[a1]: e2[a2]
//     - e2[a1]: foo
//     - e2[a2]: 1
//     - e1 != e2
//     - e1[a2] != e2[a2]
//
func (dd *DataDriven) Cmd(t *testing.T, d *datadriven.TestData) string {
	switch d.Cmd {
	case "schema":
		return dd.schemaCmd(t, d)
	case "entities":
		return dd.entitiesCmd(t, d)
	case "new-db":
		return dd.newDBCmd(t, d)
	case "insert":
		return dd.insertCmd(t, d)
	case "iterate":
		return dd.iterateCmd(t, d)
	case "query":
		return dd.queryCmd(t, d)
	case "eval":
		return dd.evalCmd(t, d)
	default:
		d.Fatalf(t, "unknown command %s", d.Cmd)
		return ""
	}
}

func (dd *DataDriven) schemaCmd(t *testing.T, d *datadriven.TestData) string {
	if dd.schema != nil {
		d.Fatalf(t, "schema already initialized")
	}
	var m yaml.MapSlice
	if err := yaml.UnmarshalStrict([]byte(d.Input), &m); err != nil {
		d.Fatalf(t, "failed to unmarshal schema: %v", err)
	}
	md := make([]attributeMetadata, len(m))
	for i, row := range m {
		k, ok := row.Key.(string)
		if !ok {
			d.Fatalf(t, "failed to parse key of type %T as string", k)
		}
		s, _ := row.Value.(string)
		md[i] = attributeMetadata{
			Name: k,
			Type: parseType(t, d, s),
		}
	}
	dd.schema = newSchema(md)
	return ""
}

func (dd *DataDriven) entitiesCmd(t *testing.T, d *datadriven.TestData) string {
	dd.ensureSchema(t, d)
	var entityMaps map[string]map[string]interface{}
	if err := yaml.UnmarshalStrict([]byte(d.Input), &entityMaps); err != nil {
		d.Fatalf(t, "failed to decode entities: %v", err)
	}
	for k, m := range entityMaps {
		if _, exists := dd.entitiesByName[k]; exists {
			d.Fatalf(t, "entity with name %s already exists", k)
		}
		vals, err := dd.schema.parseValues(m)
		if err != nil {
			d.Fatalf(t, "failed to parse entity %s: %v", k, err)
		}
		e := &entity{
			onGet:  dd.schema,
			Values: vals,
		}
		dd.entitiesByName[k] = e
		dd.entities[e] = k
	}
	return ""
}

func (dd *DataDriven) newDBCmd(t *testing.T, d *datadriven.TestData) string {
	dd.ensureSchema(t, d)
	dbName, _ := dd.getDB(t, d, "name", false /* shouldExist */)
	var attrStrs [][]string
	if d.Input != "" {
		if err := yaml.UnmarshalStrict([]byte(d.Input), &attrStrs); err != nil {
			d.Fatalf(t, "failed to parse attributes: %v", err)
		}
	}
	attrs := make([][]eav.Attribute, len(attrStrs))
	for i, strs := range attrStrs {
		var err error
		if attrs[i], err = dd.schema.parseAttributes(strs); err != nil {
			d.Fatalf(t, "failed to parse attributes: %v", err)
		}
	}
	dd.dbs[dbName] = eav.NewTree(dd.schema, attrs)
	return ""
}

func (dd *DataDriven) insertCmd(t *testing.T, d *datadriven.TestData) string {
	const shouldExist = true
	_, db := dd.getDB(t, d, "db", shouldExist)
	var entityNames []string
	if err := yaml.UnmarshalStrict([]byte(d.Input), &entityNames); err != nil {
		d.Fatalf(t, "failed to unmarshal entities: %v", err)
	}
	entities := dd.getEntitiesFromNames(t, d, entityNames)
	for _, e := range entities {
		db.Insert(e)
	}
	return ""
}

func (dd *DataDriven) iterateCmd(t *testing.T, d *datadriven.TestData) string {
	const shouldExist = true
	_, db := dd.getDB(t, d, "db", shouldExist)
	var where eav.Values
	if d.Input != "" {
		var valuesMap map[string]interface{}
		if err := yaml.UnmarshalStrict([]byte(d.Input), &valuesMap); err != nil {
			d.Fatalf(t, "failed to decode where: %v", err)
		}
		var err error
		where, err = dd.schema.parseValues(valuesMap)
		if err != nil {
			d.Fatalf(t, "failed to parse where: %v", err)
		}
	}
	var numReads int
	defer dd.schema.setOnGet(func(eav.Attribute) { numReads++ })()
	var got []string
	_ = db.Iterate(where, func(e eav.Entity) error {
		got = append(got, dd.entities[e.(*entity)])
		return nil
	})
	sort.Strings(got)
	return fmt.Sprintf("%s\nreads: %d", strings.Join(got, ","), numReads)
}

func (dd *DataDriven) queryCmd(t *testing.T, d *datadriven.TestData) string {
	dd.ensureSchema(t, d)

	var name string
	d.ScanArgs(t, "name", &name)
	facts := dd.parseFacts(t, d)

	q := eavquery.MustBuild(func(b eavquery.Builder) {
		for _, f := range facts {
			f(b)
		}
	})
	dd.queries[name] = q
	return ""
}

func (dd *DataDriven) evalCmd(t *testing.T, d *datadriven.TestData) string {
	dd.ensureSchema(t, d)
	_, db := dd.getDB(t, d, "db", true /* shouldExist */)
	var queryName string
	d.ScanArgs(t, "query", &queryName)
	q, ok := dd.queries[queryName]
	if !ok {
		d.Fatalf(t, "query %s does not exist", queryName)
	}
	var entities []string
	if err := yaml.UnmarshalStrict([]byte(d.Input), &entities); err != nil {
		d.Fatalf(t, "failed to parse entities: %v", err)
	}
	var got []string
	q.Evaluate(db, func(r eavquery.Result) error {
		var row []string
		for _, en := range entities {
			row = append(row, dd.entities[r.Entity(en).(*entity)])
		}
		got = append(got, strings.Join(row, ","))
		return nil
	})
	sort.Strings(got)
	return strings.Join(got, "\n")
}

func (dd *DataDriven) ensureSchema(t *testing.T, d *datadriven.TestData) {
	if dd.schema != nil {
		return
	}
	d.Fatalf(t, "schema must be initialized before running %s", d.Cmd)
}

func (dd *DataDriven) getDB(
	t *testing.T, d *datadriven.TestData, key string, shouldExist bool,
) (string, eav.DatabaseWriter) {
	dd.ensureSchema(t, d)
	var dbName string
	d.ScanArgs(t, key, &dbName)
	db, ok := dd.dbs[dbName]
	if !ok && shouldExist {
		d.Fatalf(t, "db with name %s does not exist", dbName)
	} else if ok && !shouldExist {
		d.Fatalf(t, "db with name %s already exists", dbName)
	}
	return dbName, db
}

func (dd *DataDriven) getEntitiesFromNames(
	t *testing.T, d *datadriven.TestData, entityNames []string,
) []eav.Entity {
	entities := make([]eav.Entity, len(entityNames))
	for i, entityName := range entityNames {
		entities[i] = dd.getEntityFromName(t, d, entityName)
	}
	return entities
}

func (dd *DataDriven) getEntityFromName(
	t *testing.T, d *datadriven.TestData, entityName string,
) eav.Entity {
	entity, ok := dd.entitiesByName[entityName]
	if !ok {
		d.Fatalf(t, "failed to find entity %s", entityName)
	}
	return entity
}

const (
	entityAttrPat     = `(?P<entity>\S+)\[(?P<attr>\S+)\]`
	entityStmtPat     = `(?P<lhs>\S+)\s+` + opPat + `\s+(?P<rhs>\S+)`
	entityAttrStmtPat = `(?P<lhs>` + entityAttrPat + `)\s+` + opPat +
		`\s+(?P<rhs>(?P<rhsEntityAttr>` + entityAttrPat + `)|(?P<rhsValue>.*))`
	opPat = `(?P<op>=|!=)`
)

var (
	entityAttrRE = regexp.MustCompile(entityAttrPat)
	entityIdx    = entityAttrRE.SubexpIndex("entity")
	attrIdx      = entityAttrRE.SubexpIndex("attr")

	entityStmtRE     = regexp.MustCompile(entityStmtPat)
	entityStmtRHSIdx = entityStmtRE.SubexpIndex("rhs")
	entityStmtOpIdx  = entityStmtRE.SubexpIndex("op")
	entityStmtLHSIdx = entityStmtRE.SubexpIndex("lhs")

	entityAttrStmtRE     = regexp.MustCompile(entityAttrStmtPat)
	entityAttrStmtRHSIdx = entityAttrStmtRE.SubexpIndex("rhs")
	entityAttrStmtOpIdx  = entityAttrStmtRE.SubexpIndex("op")
	entityAttrStmtLHSIdx = entityAttrStmtRE.SubexpIndex("lhs")
)

type fact func(b eavquery.Builder)

func (dd *DataDriven) parseReference(
	t *testing.T, d *datadriven.TestData, k string,
) (string, *attribute) {
	matches := entityAttrRE.FindStringSubmatch(k)
	if matches == nil {
		d.Fatalf(t, "invalid reference specification %s", k)
	}
	attr, err := dd.schema.lookupAttribute(matches[attrIdx])
	if err != nil {
		d.Fatalf(t, "failed to look up attribute %s", matches[attrIdx])
	}
	return matches[entityIdx], attr
}

func (dd *DataDriven) parseFacts(t *testing.T, d *datadriven.TestData) []fact {
	var factStrs []string
	if err := yaml.UnmarshalStrict([]byte(d.Input), &factStrs); err != nil {
		d.Fatalf(t, "failed parse facts as string slice: %v", err)
	}
	var facts []fact
	for _, s := range factStrs {
		facts = append(facts, dd.parseFact(t, d, s))
	}
	return facts
}

func (dd *DataDriven) parseFact(t *testing.T, d *datadriven.TestData, s string) fact {
	switch {
	case entityAttrStmtRE.MatchString(s):
		matches := entityAttrStmtRE.FindStringSubmatch(s)
		entity, attr := dd.parseReference(t, d, matches[entityAttrStmtLHSIdx])
		var otherEntity string
		var otherAttr *attribute
		var value eav.Value
		if entityAttrRE.MatchString(matches[entityAttrStmtRHSIdx]) {
			otherEntity, otherAttr = dd.parseReference(t, d, matches[entityAttrStmtRHSIdx])
		} else {
			var v interface{}
			if err := yaml.UnmarshalStrict([]byte(matches[entityAttrStmtRHSIdx]), &v); err != nil {
				d.Fatalf(t, "parsing value: %v", err)
			}
			var err error
			value, err = attr.parseValue(v)
			if err != nil {
				d.Fatalf(t, "parsing value: %v", err)
			}
		}
		switch op := matches[entityAttrStmtOpIdx]; op {
		case "=":
			if value == nil {
				return func(b eavquery.Builder) {
					b.Entity(entity).Constrain(
						attr, b.Entity(otherEntity).Reference(otherAttr),
					)
				}
			}
			return func(b eavquery.Builder) {
				b.Entity(entity).Constrain(attr, value)
			}
		case "!=":
			if value == nil {
				return func(b eavquery.Builder) {
					b.Entity(entity)
					b.Entity(otherEntity)
					b.Filter(func(result eavquery.Result) bool {
						_, _, eq := result.Entity(entity).Get(attr).
							Compare(result.Entity(otherEntity).Get(otherAttr))
						return !eq
					})
				}
			}
			return func(b eavquery.Builder) {
				b.Entity(entity)
				b.Filter(func(result eavquery.Result) bool {
					_, _, eq := result.Entity(entity).Get(attr).
						Compare(value)
					return !eq
				})
			}
		default:
			d.Fatalf(t, "unknown operator %s", op)
			panic("unreachable")
		}
	case entityStmtRE.MatchString(s):
		matches := entityStmtRE.FindStringSubmatch(s)
		eq := func(result eavquery.Result) bool {
			return eav.Equal(
				dd.schema,
				result.Entity(matches[entityStmtLHSIdx]),
				result.Entity(matches[entityStmtRHSIdx]),
			)
		}
		switch op := matches[entityStmtOpIdx]; op {
		case "=":
			return func(b eavquery.Builder) {
				b.Entity(matches[entityStmtLHSIdx])
				b.Entity(matches[entityStmtRHSIdx])
				b.Filter(eq)
			}
		case "!=":
			return func(b eavquery.Builder) {
				b.Entity(matches[entityStmtLHSIdx])
				b.Entity(matches[entityStmtRHSIdx])
				b.Filter(func(result eavquery.Result) bool { return !eq(result) })
			}
		default:
			d.Fatalf(t, "unknown operator %s", op)
			panic("unreachable")
		}

	default:
		d.Fatalf(t, "invalid fact: %q %v %v", s, entityStmtRE, entityAttrStmtRE)
		panic("unreachable")
	}
}

func parseType(t *testing.T, d *datadriven.TestData, s string) eav.Type {
	typ, ok := typeByName[s]
	if !ok {
		d.Fatalf(t, "unknown type %s", s)
	}
	return typ
}

var typeByName = make(map[string]eav.Type, eav.NumTypes)

func init() {
	for typ := 1; typ < eav.NumTypes; typ++ {
		typeByName[eav.Type(typ).String()] = eav.Type(typ)
	}
}
