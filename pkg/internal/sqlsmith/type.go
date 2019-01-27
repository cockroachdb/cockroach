package sqlsmith

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var typeNames = func() map[string]types.T {
	m := map[string]types.T{
		"int8":   types.Int,
		"int8[]": types.TArray{Typ: types.Int},
		"float8": types.Float,
	}
	for _, T := range types.OidToType {
		m[T.SQLName()] = T
		m[T.String()] = T
	}
	return m
}()

func typeFromName(name string) types.T {
	// Fill in any collated string type names we see.
	if sp := strings.Split(name, "STRING COLLATE "); len(sp) == 2 {
		typeNames[strings.ToLower(name)] = types.TCollatedString{Locale: sp[1]}
	}
	typ, ok := typeNames[strings.ToLower(name)]
	if !ok {
		panic(fmt.Errorf("unknown type name: %s", name))
	}
	return typ
}

func getRandType() types.T {
	arr := types.AnyNonArray
	return arr[rand.Intn(len(arr))]
}
