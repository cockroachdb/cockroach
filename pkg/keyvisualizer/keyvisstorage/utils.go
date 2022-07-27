package keyvisstorage

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func datumToNative(datum tree.Datum) (interface{}, error) {
	datum = tree.UnwrapDOidWrapper(datum)
	if datum == tree.DNull {
		return nil, nil
	}
	switch d := datum.(type) {
	case *tree.DUuid:
		return d.UUID.String(), nil
	case *tree.DTimestamp:
		return d.Time, nil
	case *tree.DBytes:
		return []byte(*d), nil
	case *tree.DInt:
		return int64(*d), nil
	}
	return nil, errors.Newf("cannot handle type %T", datum)
}
