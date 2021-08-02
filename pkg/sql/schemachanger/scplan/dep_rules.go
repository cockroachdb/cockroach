package scplan

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	q "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav/eavquery"
	. "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

type depRegistry struct {
	rules []depRule
}

type depRule struct {
	name     string
	from, to string
	q        *q.Query
}

func (r *depRegistry) Register(ruleName, from, to string, query *q.Query) {
	r.rules = append(r.rules, depRule{
		name: ruleName,
		from: from,
		to:   to,
		q:    query,
	})
}

var depRules depRegistry

func init() {
	depRules.Register(
		"database dependencies",
		"db", "other",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "db", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, DatabaseElement},
			})
			q.Constrain(b, "other", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, q.Any(
					TypeElement,
					TableElement,
					ViewElement,
					SequenceElement,
					SchemaElement,
				)},
			})
			b.Filter(func(result q.Result) bool {
				db := result.Entity("db").(Entity).GetElement().(*Database)
				other := result.Entity("other").(Entity)
				return idInIDs(db.DependentObjects, GetDescID(other))
			})
		}))

	depRules.Register(
		"schema dependencies",
		"schema", "other",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "schema", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SchemaElement},
			})
			q.Constrain(b, "other", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, q.Any(
					TypeElement,
					TableElement,
					ViewElement,
					SequenceElement,
				)},
			})
			b.Filter(func(result q.Result) bool {
				db := result.Entity("schema").(Entity).GetElement().(*Schema)
				other := result.Entity("other").(Entity)
				return idInIDs(db.DependentObjects, GetDescID(other))
			})
		}))

	depRules.Register(
		"sequence owned by being dropped relies on sequence entering delete only",
		"owned_by", "seq",
		q.MustBuild(func(b q.Builder) {
			ownedBy := q.Constrain(b, "owned_by", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceOwnerElement},
			})
			q.Constrain(b, "seq", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceElement},
				{AttrDescID, ownedBy.Reference(AttrDescID)},
			})
		}))

	// TODO(ajwerner): What does this even mean?
	depRules.Register(
		"type reference something",
		"type", "type_ref",
		q.MustBuild(func(b q.Builder) {
			typ := q.Constrain(b, "type", []q.AttributeValue{
				{AttrStatus, PublicStatus},
				{AttrDirection, DropDirection},
			})
			q.Constrain(b, "type_ref", []q.AttributeValue{
				{AttrStatus, DeleteOnlyStatus},
				{AttrDirection, DropDirection},
				{AttrReferencedDescID, typ.Reference(AttrDescID)},
			})
		}))

	// TODO(ajwerner): What does this even mean? The sequence starts in
	// public.
	depRules.Register(
		"sequence default expr",
		"seq", "def_expr",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "seq", []q.AttributeValue{
				{AttrStatus, PublicStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, SequenceElement},
			})
			q.Constrain(b, "def_expr", []q.AttributeValue{
				{AttrStatus, AbsentStatus},
				{AttrDirection, DropDirection},
				{AttrElementType, DefaultExpressionElement},
			})
			b.Filter(func(result q.Result) bool {
				seq := result.Entity("seq").(Entity).GetElement().(*Sequence)
				defExpr := result.Entity("def_expr").(Entity).GetElement().(*DefaultExpression)
				return defaultExprReferencesColumn(seq, defExpr)
			})
		}))

	dropViewAbsent := []q.AttributeValue{
		{AttrElementType, ViewElement},
		{AttrDirection, DropDirection},
		{AttrStatus, AbsentStatus},
	}
	depRules.Register(
		"view depends on view",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			q.Constrain(b, "from", dropViewAbsent)
			q.Constrain(b, "to", dropViewAbsent)
			b.Filter(func(result q.Result) bool {
				from := result.Entity("from").(Entity).GetElement().(*View)
				to := result.Entity("to").(Entity)
				toID := GetDescID(to)
				return GetDescID(from) != toID && idInIDs(from.DependedOnBy, toID)
			})
		}),
	)
	depRules.Register(
		"view depends on type",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			from := q.Constrain(b, "from", dropViewAbsent)
			q.Constrain(b, "to", []q.AttributeValue{
				{AttrElementType, TypeRefElement},
				{AttrDirection, DropDirection},
				{AttrStatus, AbsentStatus},
				{AttrDescID, from.Reference(AttrDescID)},
			})
		}))

	depRules.Register(
		"column depends on indexes",
		"from", "to",
		q.MustBuild(func(b q.Builder) {
			from := q.Constrain(b, "from", []q.AttributeValue{
				{AttrElementType, ColumnElement},
				{AttrDirection, AddDirection},
				{AttrStatus, q.Any(DeleteAndWriteOnlyStatus, PublicStatus)},
			})
			q.Constrain(b, "to", []q.AttributeValue{
				{AttrDescID, from.Reference(AttrDescID)},
				{AttrDirection, AddDirection},
				{AttrStatus, from.Reference(AttrStatus)},
				{AttrElementType, q.Any(PrimaryIndexElement, SecondaryIndexElement)},
			})
			b.Filter(func(result q.Result) bool {
				from := result.Entity("from").(Entity).GetElement().(*Column)
				to := result.Entity("to").(Entity).GetElement()
				var idx *descpb.IndexDescriptor
				switch to := to.(type) {
				case *PrimaryIndex:
					idx = &to.Index
				case *SecondaryIndex:
					idx = &to.Index
				default:
					panic(errors.AssertionFailedf("unexpected type %T", to))
				}
				return indexContainsColumn(idx, from.Column.ID)
			})
		}))

	primaryIndexReferenceEachOther := q.MustBuild(func(b q.Builder) {
		add := q.Constrain(b, "add", []q.AttributeValue{
			{AttrElementType, PrimaryIndexElement},
			{AttrDirection, AddDirection},
			{AttrStatus, PublicStatus},
		})
		q.Constrain(b, "drop", []q.AttributeValue{
			{AttrElementType, PrimaryIndexElement},
			{AttrDirection, DropDirection},
			{AttrStatus, DeleteAndWriteOnlyStatus},
			{AttrDescID, add.Reference(AttrDescID)},
		})
		b.Filter(func(result q.Result) bool {
			add := result.Entity("add").(Entity).GetElement().(*PrimaryIndex)
			drop := result.Entity("drop").(Entity).GetElement().(*PrimaryIndex)
			return add.OtherPrimaryIndexID == drop.Index.ID
		})
	})
	depRules.Register(
		"primary index add depends on drop",
		"add", "drop",
		primaryIndexReferenceEachOther,
	)
	depRules.Register(
		"primary index drop depends on add",
		"drop", "add",
		primaryIndexReferenceEachOther,
	)

}
