// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// This file implements the structural compatibility check between the Avro
// schema this changefeed generates locally and a schema fetched from a schema
// registry, plus the adoption path that re-encodes with it.
//
// In read-only registry mode the changefeed may not register its own schema,
// so the only schema ID it can stamp on a message is the registered one. Per
// the Confluent wire contract a consumer decodes each message with the schema
// named by that ID, so the payload must be produced by the registered schema
// itself. The changefeed therefore encodes with a codec built from the
// registered schema verbatim, feeding it the native values produced from the
// local schema. This check verifies that this re-encode is always safe: that
// the registered schema can represent every value the changefeed produces.
//
// The walk models the Avro specification's Schema Resolution rules between a
// writer schema (the local schema, describing the values the changefeed
// produces) and a reader schema (the registered schema, which does the
// encoding). Per those rules the registered schema, in any record at any
// depth, may add fields the changefeed does not produce, provided they carry
// an explicit default value, which goavro then encodes on every message. A
// nullable type alone is not enough: a registered field the changefeed does
// not supply needs a default, or the encoder has no value to write.
//
// The main algorithm is drawn from:
// https://avro.apache.org/docs/1.11.1/specification/#schema-resolution
//
// Deliberate deviations from the spec's resolution rules, all because the
// changefeed hands its native values straight to the registered codec rather
// than relying on a consumer to resolve two schemas at decode time:
//   - A registered record may not omit a field the changefeed produces, where
//     the spec drops it: the column would silently vanish from the wire, so
//     the mismatch is reported rather than the data lost.
//   - Envelope-level fields must match exactly; a missing or extra
//     before/after/updated/... is an error naming the field, because those
//     correspond to changefeed options rather than table columns.
//   - Type promotions (int→long, string↔bytes, ...) are not accepted: the
//     bytes are encoded with the registered schema and goavro selects union
//     branches by type name, so a promoted value would need a re-mapping
//     layer at encode time, and the types the changefeed emits only promote
//     lossily.
//   - Logical type annotations (logicalType, precision, scale) must match
//     exactly wherever two nodes are compared: the binary layout does not
//     capture them, but consumers use them to interpret decoded values.
//   - Named types (records, enums, fixed) match by full name and aliases are
//     not consulted, where the spec matches unqualified names: goavro
//     identifies union branches by full name at encode time, so a full-name
//     mismatch is unencodable there, and one name rule everywhere is simpler.
//   - A writer-only enum symbol is rejected even when the registered enum has
//     a default: the spec substitutes the default at decode time, but this
//     changefeed encodes with the registered schema, and its codec cannot
//     represent a symbol the schema does not define.
//
// A schema that passes the check is used verbatim
// (NewEnvelopeRecordWithRegisteredSchema): the changefeed encodes with a codec
// built from the registered schema, with no adaptation of the locally
// generated records. That is safe because
// goavro's record encoding pulls each field's value from the native map by
// name in the registered schema's own field order and encodes the field's
// schema default when the map holds no value for it; and because goavro
// selects union branches by type name rather than position (see Union /
// unionBinaryFromNative), while the check guarantees every union member key,
// logical type, and record name the changefeed emits exists identically on
// the registered side.

// CheckRegisteredEnvelopeCompat reports whether registeredSchemaJSON can
// represent every value of the envelope this changefeed produces, described by
// localSchemaJSON: whether it is safe to re-encode the changefeed's values with
// the registered schema (see the file comment). The check is pure: it does not
// mutate either schema, so a latest-compatible version walk can run it against
// many candidate versions before choosing one.
func CheckRegisteredEnvelopeCompat(localSchemaJSON, registeredSchemaJSON string) error {
	local, localNames, err := parseSchema(localSchemaJSON)
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(
			err, "parsing the schema generated by this changefeed")
	}
	if _, err := goavro.NewCodec(registeredSchemaJSON); err != nil {
		return errors.Wrap(err, "registered schema is not valid avro")
	}
	registered, registeredNames, err := parseSchema(registeredSchemaJSON)
	if err != nil {
		return errors.Wrap(err, "parsing the registered schema")
	}
	c := &compatChecker{writerNames: localNames, readerNames: registeredNames}
	return c.compareEnvelope(local, registered)
}

// NewEnvelopeRecordWithRegisteredSchema creates an envelope record that
// encodes with registeredSchemaJSON, provided that schema can represent every
// value local produces (see CheckRegisteredEnvelopeCompat). The returned
// record encodes with a codec built from the registered schema verbatim, so
// the bytes match the schema whose ID the sink stamps on each message.
func NewEnvelopeRecordWithRegisteredSchema(
	local *EnvelopeRecord, registeredSchemaJSON string,
) (*EnvelopeRecord, error) {
	if err := CheckRegisteredEnvelopeCompat(local.codec.Schema(), registeredSchemaJSON); err != nil {
		return nil, err
	}
	// The check validated the registered schema, so building its codec can
	// only fail on a code bug.
	registeredCodec, err := goavro.NewCodec(registeredSchemaJSON)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			err, "building codec for validated registered schema")
	}
	envelope := *local
	envelope.codec = registeredCodec
	return &envelope, nil
}

// parseSchema parses an envelope schema into its record node plus the
// named-type table its references resolve through.
func parseSchema(schemaJSON string) (map[string]interface{}, namedTypes, error) {
	var tree interface{}
	if err := json.Unmarshal([]byte(schemaJSON), &tree); err != nil {
		return nil, nil, err
	}
	rec, ok := tree.(map[string]interface{})
	if !ok || baseTypeName(rec) != `record` {
		return nil, nil, errors.New("not an avro record")
	}
	names := namedTypes{}
	if err := collectNamedTypes(rec, ``, names); err != nil {
		return nil, nil, err
	}
	return rec, names, nil
}

// compatChecker holds the state for one compatibility walk, oriented as the
// Avro spec's Schema Resolution: the writer schema (local) describes the values
// the changefeed produces, the reader schema (registered) does the encoding.
// writerNames and readerNames are the named-type tables the two schemas resolve
// references through.
type compatChecker struct {
	writerNames, readerNames namedTypes
}

// primitiveNames is the set of Avro primitive type names. A string schema that
// is not one of these is a reference to a named type.
var primitiveNames = map[string]struct{}{
	SchemaTypeBoolean: {},
	SchemaTypeBytes:   {},
	SchemaTypeDouble:  {},
	SchemaTypeFloat:   {},
	SchemaTypeInt:     {},
	SchemaTypeLong:    {},
	SchemaTypeNull:    {},
	SchemaTypeString:  {},
}

func (c *compatChecker) compareEnvelope(wRec, rRec map[string]interface{}) error {
	wFull := recordFullName(wRec, ``)
	rFull := recordFullName(rRec, ``)
	if wFull != rFull {
		return errors.Newf(
			"registered envelope record %q does not match envelope record %q produced by this "+
				"changefeed", rFull, wFull)
	}
	childNS := namespaceOf(wFull)

	wFields := fieldList(wRec)
	rFields := fieldList(rRec)
	wByName := indexByName(wFields)
	rByName := indexByName(rFields)

	// Envelope field-name sets must match exactly; envelope fields correspond
	// to changefeed options, unlike record fields below the envelope, which
	// follow the resolution rules for field-set differences.
	for _, wf := range wFields {
		if _, ok := rByName[fieldName(wf)]; !ok {
			return errors.Newf(
				"the registered schema is missing envelope field %q produced by this changefeed; "+
					"envelope fields are set by changefeed options, so adjust the options or "+
					"register a schema version with a matching envelope",
				fieldName(wf))
		}
	}
	for _, rf := range rFields {
		if _, ok := wByName[fieldName(rf)]; !ok {
			return errors.Newf(
				"the registered schema has envelope field %q that this changefeed does not "+
					"produce; envelope fields are set by changefeed options, so adjust the "+
					"options or register a schema version with a matching envelope",
				fieldName(rf))
		}
	}

	for _, wf := range wFields {
		name := fieldName(wf)
		if err := c.compareType(fieldType(wf), fieldType(rByName[name]), childNS, name); err != nil {
			return err
		}
	}
	return nil
}

// compareType compares two schema nodes, dispatching on which sides are
// unions.
func (c *compatChecker) compareType(writer, reader interface{}, ns, path string) error {
	wUnion, wIsUnion := writer.([]interface{})
	rUnion, rIsUnion := reader.([]interface{})
	switch {
	case wIsUnion && rIsUnion:
		// Every branch the writer may emit must match some reader branch;
		// reader-only branches are fine because they never occur in the data.
		for _, wb := range wUnion {
			if err := c.matchIntoUnion(wb, rUnion, ns, path); err != nil {
				return err
			}
		}
		return nil
	case rIsUnion:
		return c.matchIntoUnion(writer, rUnion, ns, path)
	case wIsUnion:
		// The reader accepts only one type, so every branch the writer may
		// emit must match it.
		for _, wb := range wUnion {
			if err := c.compareResolved(wb, reader, ns, path); err != nil {
				return err
			}
		}
		return nil
	default:
		return c.compareResolved(writer, reader, ns, path)
	}
}

// matchIntoUnion finds the reader union branch that accepts the writer node
// and compares against it. It prefers the exact union member key goavro uses
// at encode time; failing that it falls back to the base type so a
// logical-type mismatch is reported as such rather than as a missing branch.
func (c *compatChecker) matchIntoUnion(
	writer interface{}, rUnion []interface{}, ns, path string,
) error {
	wBase, wFull, err := c.unionMemberKey(writer, ns, c.writerNames)
	if err != nil {
		return err
	}
	for _, rb := range rUnion {
		_, rFull, err := c.unionMemberKey(rb, ns, c.readerNames)
		if err != nil {
			return err
		}
		if rFull == wFull {
			return c.compareResolved(writer, rb, ns, path)
		}
	}
	for _, rb := range rUnion {
		rBase, _, err := c.unionMemberKey(rb, ns, c.readerNames)
		if err != nil {
			return err
		}
		if rBase == wBase {
			return c.compareResolved(writer, rb, ns, path)
		}
	}
	return errors.Newf(
		"this changefeed produces type %s at %s that does not match any type accepted by the "+
			"registered schema", wFull, describePath(path))
}

// compareResolved compares two non-union nodes, dereferencing named
// references first.
func (c *compatChecker) compareResolved(writer, reader interface{}, ns, path string) error {
	writer, wns, err := c.deref(writer, ns, c.writerNames)
	if err != nil {
		return err
	}
	reader, rns, err := c.deref(reader, ns, c.readerNames)
	if err != nil {
		return err
	}

	// Logical annotations attach to primitives and are compared wherever they
	// appear, independent of the binary layout.
	if err := compareLogicalAttrs(writer, reader, path); err != nil {
		return err
	}

	wBase := baseTypeName(writer)
	rBase := baseTypeName(reader)
	if wBase != rBase {
		return errors.Newf(
			"this changefeed produces type %s at %s that does not match type %s in the "+
				"registered schema",
			describeType(writer), describePath(path), describeType(reader))
	}

	switch wBase {
	case `record`:
		return c.compareRecord(
			writer.(map[string]interface{}), reader.(map[string]interface{}), wns, rns, path)
	case `enum`:
		return c.compareEnum(
			writer.(map[string]interface{}), reader.(map[string]interface{}), wns, rns, path)
	case `fixed`:
		return c.compareFixed(
			writer.(map[string]interface{}), reader.(map[string]interface{}), wns, rns, path)
	case SchemaTypeArray:
		wItems, rItems := writer.(map[string]interface{})[`items`], reader.(map[string]interface{})[`items`]
		return c.compareType(wItems, rItems, wns, path+`.items`)
	case `map`:
		wVals, rVals := writer.(map[string]interface{})[`values`], reader.(map[string]interface{})[`values`]
		return c.compareType(wVals, rVals, wns, path+`.values`)
	default:
		if _, isPrimitive := primitiveNames[wBase]; isPrimitive {
			// Base type and logical attrs are already compared and a primitive
			// has nothing nested to descend into.
			return nil
		}
		return errors.Newf("unexpected avro type %q at %s", wBase, describePath(path))
	}
}

// compareRecord compares two records per the spec's record resolution rules:
// their names must match, fields are matched by name and shared ones compared
// recursively, a reader-only field must carry a default value (which a codec
// built from the reader schema encodes whenever no value is supplied for the
// field), and a writer-only field is rejected, deviating from the spec, which
// would drop its values from the wire.
func (c *compatChecker) compareRecord(
	wRec, rRec map[string]interface{}, wns, rns, path string,
) error {
	wFull := recordFullName(wRec, wns)
	rFull := recordFullName(rRec, rns)
	if wFull != rFull {
		return errors.Newf(
			"registered record %q at %s does not match record %q produced by this changefeed",
			rFull, describePath(path), wFull)
	}
	// The records share a name, so they share a namespace for their children.
	childNS := namespaceOf(wFull)
	wFields := fieldList(wRec)
	rFields := fieldList(rRec)
	wByName := indexByName(wFields)
	rByName := indexByName(rFields)
	for _, rf := range rFields {
		name := fieldName(rf)
		if wf, ok := wByName[name]; ok {
			if err := c.compareType(fieldType(wf), fieldType(rf), childNS, path+`.`+name); err != nil {
				return err
			}
			continue
		}
		if _, hasDefault := rf[`default`]; !hasDefault {
			return errors.Newf(
				"field %q of record %q at %s exists only in the registered schema and has no "+
					"default value, so this changefeed cannot supply a value for it; add a default "+
					"or register a schema version without the field",
				name, rFull, describePath(path))
		}
	}
	for _, wf := range wFields {
		name := fieldName(wf)
		if _, ok := rByName[name]; !ok {
			return errors.Newf(
				"field %q of record %q at %s exists only in this changefeed's schema, so "+
					"encoding with the registered schema would drop its values; register a "+
					"schema version that includes the field",
				name, wFull, describePath(path))
		}
	}
	return nil
}

// compareEnum requires the reader to accept every symbol the writer may
// emit; reader-only symbols are fine.
func (c *compatChecker) compareEnum(
	wEnum, rEnum map[string]interface{}, wns, rns, path string,
) error {
	wFull := recordFullName(wEnum, wns)
	rFull := recordFullName(rEnum, rns)
	if wFull != rFull {
		return errors.Newf(
			"registered enum %q at %s does not match enum %q produced by this changefeed",
			rFull, describePath(path), wFull)
	}
	rSyms := stringSet(rEnum[`symbols`])
	for sym := range stringSet(wEnum[`symbols`]) {
		if _, ok := rSyms[sym]; !ok {
			return errors.Newf(
				"registered enum %q at %s is missing symbol %q that this changefeed may emit",
				wFull, describePath(path), sym)
		}
	}
	return nil
}

func (c *compatChecker) compareFixed(
	wFixed, rFixed map[string]interface{}, wns, rns, path string,
) error {
	wFull := recordFullName(wFixed, wns)
	rFull := recordFullName(rFixed, rns)
	if wFull != rFull {
		return errors.Newf(
			"registered fixed %q at %s does not match fixed %q produced by this changefeed",
			rFull, describePath(path), wFull)
	}
	if wSize, rSize := fixedSize(wFixed), fixedSize(rFixed); wSize != rSize {
		return errors.Newf(
			"fixed %q at %s has size %d in this changefeed's schema but size %d in the "+
				"registered schema", wFull, describePath(path), wSize, rSize)
	}
	return nil
}

// deref resolves a named-type reference to its definition, returning the
// definition and the namespace it establishes for its children. Primitive names
// and non-string nodes are returned unchanged.
func (c *compatChecker) deref(
	t interface{}, ns string, names namedTypes,
) (interface{}, string, error) {
	s, ok := t.(string)
	if !ok {
		return t, ns, nil
	}
	if _, isPrimitive := primitiveNames[s]; isPrimitive {
		return t, ns, nil
	}
	def, defNS, ok := names.resolve(s, ns)
	if !ok {
		return nil, ns, errors.Newf("avro schema references unknown named type %q", s)
	}
	return def, defNS, nil
}

// unionMemberKey returns the key goavro uses to identify t as a union member.
// full includes any logicalType suffix ("long.timestamp-micros", "bytes.decimal")
// and is the key goavro looks up at encode time; base omits it and is used to
// diagnose logical-type mismatches. Named references are resolved through names.
func (c *compatChecker) unionMemberKey(
	t interface{}, ns string, names namedTypes,
) (base, full string, err error) {
	switch v := t.(type) {
	case string:
		if _, isPrimitive := primitiveNames[v]; isPrimitive {
			return v, v, nil
		}
		def, defNS, ok := names.resolve(v, ns)
		if !ok {
			return ``, ``, errors.Newf("avro schema references unknown named type %q", v)
		}
		return c.unionMemberKey(def, defNS, names)
	case map[string]interface{}:
		typ, _ := v[`type`].(string)
		switch typ {
		case `record`, `enum`, `fixed`:
			name := recordFullName(v, ns)
			return name, name, nil
		case SchemaTypeArray, `map`:
			return typ, typ, nil
		case ``:
			// The "type" is itself an object or array (a nested definition).
			return c.unionMemberKey(v[`type`], ns, names)
		default:
			key := typ
			if lt, ok := v[`logicalType`].(string); ok && lt != `` {
				key = typ + `.` + lt
			}
			return typ, key, nil
		}
	default:
		return ``, ``, errors.AssertionFailedf("unexpected avro union member %T", t)
	}
}

// namedTypes maps an Avro full name to the definition of the record, enum, or
// fixed type declared under it, so references can be resolved.
type namedTypes map[string]interface{}

// resolve looks up a reference relative to the enclosing namespace, per the Avro
// name rules, returning the definition and the namespace it establishes.
func (nt namedTypes) resolve(ref, enclosingNS string) (interface{}, string, bool) {
	full := ref
	if !strings.ContainsRune(ref, '.') && enclosingNS != `` {
		full = enclosingNS + `.` + ref
	}
	if def, ok := nt[full]; ok {
		return def, namespaceOf(full), true
	}
	if def, ok := nt[ref]; ok {
		return def, namespaceOf(ref), true
	}
	return nil, ``, false
}

// collectNamedTypes records every record, enum, and fixed definition reachable
// from node under its full name, so later references resolve to it.
func collectNamedTypes(node interface{}, ns string, out namedTypes) error {
	switch v := node.(type) {
	case []interface{}:
		for _, b := range v {
			if err := collectNamedTypes(b, ns, out); err != nil {
				return err
			}
		}
	case map[string]interface{}:
		typ, _ := v[`type`].(string)
		switch typ {
		case `record`:
			full := recordFullName(v, ns)
			out[full] = v
			childNS := namespaceOf(full)
			for _, f := range fieldList(v) {
				if err := collectNamedTypes(fieldType(f), childNS, out); err != nil {
					return err
				}
			}
		case `enum`, `fixed`:
			out[recordFullName(v, ns)] = v
		case SchemaTypeArray:
			return collectNamedTypes(v[`items`], ns, out)
		case `map`:
			return collectNamedTypes(v[`values`], ns, out)
		default:
		}
	case string:
		// A bare primitive name or reference, as above.
	default:
		return errors.AssertionFailedf("unexpected avro schema node %T", node)
	}
	return nil
}

// recordFullName computes the full name of a named-type node given the enclosing
// namespace, following the Avro rule that a dotted name is absolute, an explicit
// namespace attribute wins next, and the enclosing namespace applies otherwise.
func recordFullName(obj map[string]interface{}, enclosingNS string) string {
	name, _ := obj[`name`].(string)
	if strings.ContainsRune(name, '.') {
		return name
	}
	ns, ok := obj[`namespace`].(string)
	if !ok || ns == `` {
		ns = enclosingNS
	}
	if ns == `` {
		return name
	}
	return ns + `.` + name
}

func namespaceOf(fullName string) string {
	if i := strings.LastIndexByte(fullName, '.'); i >= 0 {
		return fullName[:i]
	}
	return ``
}

// baseTypeName returns the underlying Avro type name of a resolved (non-
// reference) node: the primitive name, or record/enum/fixed/array/map. Logical
// annotations are ignored here; they are compared separately.
func baseTypeName(t interface{}) string {
	switch v := t.(type) {
	case string:
		return v
	case map[string]interface{}:
		switch tv := v[`type`].(type) {
		case string:
			return tv
		default:
			return baseTypeName(tv)
		}
	case []interface{}:
		return `union`
	default:
		return ``
	}
}

func fieldList(rec map[string]interface{}) []map[string]interface{} {
	raw, _ := rec[`fields`].([]interface{})
	out := make([]map[string]interface{}, 0, len(raw))
	for _, f := range raw {
		if m, ok := f.(map[string]interface{}); ok {
			out = append(out, m)
		}
	}
	return out
}

func fieldName(f map[string]interface{}) string {
	s, _ := f[`name`].(string)
	return s
}

func fieldType(f map[string]interface{}) interface{} {
	return f[`type`]
}

func indexByName(fields []map[string]interface{}) map[string]map[string]interface{} {
	out := make(map[string]map[string]interface{}, len(fields))
	for _, f := range fields {
		out[fieldName(f)] = f
	}
	return out
}

func stringSet(v interface{}) map[string]struct{} {
	raw, _ := v.([]interface{})
	out := make(map[string]struct{}, len(raw))
	for _, s := range raw {
		if str, ok := s.(string); ok {
			out[str] = struct{}{}
		}
	}
	return out
}

func fixedSize(obj map[string]interface{}) int {
	f, _ := obj[`size`].(float64)
	return int(f)
}

// describeType renders a schema node for an error message.
func describeType(t interface{}) string {
	raw, err := json.Marshal(t)
	if err != nil {
		return fmt.Sprintf("%v", t)
	}
	return string(raw)
}
