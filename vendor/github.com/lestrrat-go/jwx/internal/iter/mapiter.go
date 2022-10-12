package iter

import (
	"context"

	"github.com/lestrrat-go/iter/mapiter"
	"github.com/pkg/errors"
)

// MapVisitor is a specialized visitor for our purposes.
// Whereas mapiter.Visitor supports any type of key, this
// visitor assumes the key is a string
type MapVisitor interface {
	Visit(string, interface{}) error
}

type MapVisitorFunc func(string, interface{}) error

func (fn MapVisitorFunc) Visit(s string, v interface{}) error {
	return fn(s, v)
}

func WalkMap(ctx context.Context, src mapiter.Source, visitor MapVisitor) error {
	return mapiter.Walk(ctx, src, mapiter.VisitorFunc(func(k, v interface{}) error {
		//nolint:forcetypeassert
		return visitor.Visit(k.(string), v)
	}))
}

func AsMap(ctx context.Context, src mapiter.Source) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := mapiter.AsMap(ctx, src, &m); err != nil {
		return nil, errors.Wrap(err, `mapiter.AsMap failed`)
	}
	return m, nil
}
