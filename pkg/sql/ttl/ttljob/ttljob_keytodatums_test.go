package ttljob

import (
	"encoding/hex"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestKeyToDatums(t *testing.T) {
	tenantID := roachpb.MakeTenantID(5554)
	codec := keys.MakeSQLCodec(tenantID)
	bytes, err := hex.DecodeString("2f54656e616e742f353535342f5461626c652f3132332f312f225c783163205c7865666660294c5c7866665c786265505c7862615c78646379475c7864665c78313522")
	require.NoError(t, err)
	rKey := roachpb.RKey(bytes)
	var alloc tree.DatumAlloc
	datums, err := keyToDatums(rKey, codec, []*types.T{types.Uuid}, &alloc)
	require.NoError(t, err)
	require.Len(t, datums, 1)
}
