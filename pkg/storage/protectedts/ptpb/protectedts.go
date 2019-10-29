package ptpb

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// NewRecord creates a new record with a random UUID.
func NewRecord(
	ts hlc.Timestamp, mode ProtectionMode, metaType string, meta []byte, spans ...roachpb.Span,
) Record {
	return Record{
		ID:        uuid.MakeV4(),
		Timestamp: ts,
		Mode:      mode,
		MetaType:  metaType,
		Meta:      meta,
		Spans:     spans,
	}
}
