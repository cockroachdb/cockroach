package scattr

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

// Get retrieves the value of the attribute from the given element.
func Get(attr Attr, e scpb.Element) Value {

	switch e := e.GetElement().(type) {

	case *scpb.Column:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case ColumnID:
			return makeColumnID(&e.Column.ID)
		case Name:
			return makeName(&e.Column.Name)
		default:
			return nil
		}

	case *scpb.PrimaryIndex:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case IndexID:
			return makeIndexID(&e.Index.ID)
		case Name:
			return makeName(&e.Index.Name)
		default:
			return nil
		}

	case *scpb.SecondaryIndex:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case IndexID:
			return makeIndexID(&e.Index.ID)
		case Name:
			return makeName(&e.Index.Name)
		default:
			return nil
		}

	case *scpb.SequenceDependency:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.SequenceID)
		case ReferencedDescID:
			return makeDescID(&e.TableID)
		case ColumnID:
			return makeColumnID(&e.ColumnID)
		default:
			return nil
		}

	case *scpb.UniqueConstraint:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case IndexID:
			return makeIndexID(&e.IndexID)
		default:
			return nil
		}

	case *scpb.CheckConstraint:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case Name:
			return makeName(&e.Name)
		default:
			return nil
		}

	case *scpb.Sequence:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.SequenceID)
		default:
			return nil
		}

	case *scpb.DefaultExpression:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case ColumnID:
			return makeColumnID(&e.ColumnID)
		default:
			return nil
		}

	case *scpb.View:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		default:
			return nil
		}

	case *scpb.TypeReference:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.DescID)
		case ReferencedDescID:
			return makeDescID(&e.TypeID)
		default:
			return nil
		}

	case *scpb.Table:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		default:
			return nil
		}

	case *scpb.InboundForeignKey:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.OriginID)
		case ReferencedDescID:
			return makeDescID(&e.ReferenceID)
		case Name:
			return makeName(&e.Name)
		default:
			return nil
		}

	case *scpb.OutboundForeignKey:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.OriginID)
		case ReferencedDescID:
			return makeDescID(&e.ReferenceID)
		case Name:
			return makeName(&e.Name)
		default:
			return nil
		}

	case *scpb.RelationDependedOnBy:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TableID)
		case ReferencedDescID:
			return makeDescID(&e.DependedOnBy)
		default:
			return nil
		}

	case *scpb.SequenceOwnedBy:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.SequenceID)
		case ReferencedDescID:
			return makeDescID(&e.OwnerTableID)
		default:
			return nil
		}

	case *scpb.Type:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.TypeID)
		default:
			return nil
		}

	case *scpb.Schema:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.SchemaID)
		default:
			return nil
		}

	case *scpb.Database:
		switch attr {
		case Type:
			return getElementType(e)
		case DescID:
			return makeDescID(&e.DatabaseID)
		default:
			return nil
		}

	default:
		panic(errors.AssertionFailedf("unknown element type %T", e))
	}
}
