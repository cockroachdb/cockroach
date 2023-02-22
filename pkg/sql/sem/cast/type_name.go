package cast

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// CastTypeName returns the name of the type used for casting.
func CastTypeName(t *types.T) string {
	// SQLString is wrong for these types.
	switch t.Oid() {
	case oid.T_numeric:
		// SQLString returns `decimal`
		return "numeric"
	case oid.T_char:
		// SQLString returns `"char"`
		return "char"
	case oid.T_bpchar:
		// SQLString returns `char`.
		return "bpchar"
	case oid.T_text:
		// SQLString returns `string`
		return "text"
	}
	return strings.ToLower(t.SQLString())
}
