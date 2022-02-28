package scpb_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestIndexesRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	for _, e := range []scpb.Element{
		&scpb.PrimaryIndex{
			Index: scpb.Index{
				TableID: 1,
				IndexID: 2,
			},
		},
		&scpb.SecondaryIndex{
			Index: scpb.Index{
				TableID: 1,
				IndexID: 2,
			},
		},
	} {
		t.Run(screl.ElementString(e), func(t *testing.T) {
			encoded, err := protoreflect.MessageToJSON(e, protoreflect.FmtFlags{})
			require.NoError(t, err)
			m, err := protoreflect.NewMessage(proto.MessageName(e))
			require.NoError(t, err)
			_, err = protoreflect.JSONBMarshalToMessage(encoded, m)
			require.NoError(t, err)
			require.Equal(t, e, m)
		})
	}
}
