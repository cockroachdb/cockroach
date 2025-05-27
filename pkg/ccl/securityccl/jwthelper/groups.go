// ccl/securityccl/jwthelper/groups.go
package jwthelper

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwt"
)

// ExtractGroups returns a sorted, deduplicated, lowercased list of groups
// taken from tok[claimName]. The claim may be a JSON array or a
// comma/space-separated string.
func ExtractGroups(st *cluster.Settings, tok jwt.Token, claim string) ([]string, error) {
	raw, ok := tok.Get(claim)
	if !ok {
		return nil, errors.Newf("%q claim missing", claim)
	}
	groups, err := normalize(raw)
	if err != nil {
		return nil, err
	}
	sort.Strings(groups) // deterministic GRANT order
	return groups, nil
}

func normalize(v any) ([]string, error) {
	add := func(dst []string, s string) []string {
		s = strings.ToLower(strings.TrimSpace(s))
		if s != "" {
			return append(dst, s)
		}
		return dst
	}

	var out []string
	switch g := v.(type) {
	case []any:
		for _, x := range g {
			out = add(out, fmt.Sprint(x))
		}
	case string:
		sep := ","
		if !strings.Contains(g, ",") {
			sep = " "
		}
		for _, part := range strings.Split(g, sep) {
			out = add(out, part)
		}
	default:
		return nil, errors.New("groups claim must be array or string")
	}

	uniq := map[string]struct{}{}
	for _, g := range out {
		uniq[g] = struct{}{}
	}
	out = out[:0]
	for g := range uniq {
		out = append(out, g)
	}
	return out, nil
}
