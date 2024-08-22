package valueside

import "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"

// encodeInt8Range encodes a DInt8Range into a byte array.
// It's encode the following in order:
// - StartBound's Type
// - EndBound's Type
// - StartBound's Datum (int)
// - EndBound
func encodeInt8Range(d *tree.DInt8Range, colID ColumnIDDelta) ([]byte, error) {
	var err error
	buf := make([]byte, 0)
	buf = append(buf, byte(d.StartBound.Typ))
	buf = append(buf, byte(d.EndBound.Typ))

	buf, err = Encode(buf, colID, d.StartBound.Val, nil /* scratch */)
	if err != nil {
		return nil, err
	}
	buf, err = Encode(buf, colID, d.EndBound.Val, nil /* scratch */)
	if err != nil {
		return nil, err
	}
	return buf, nil
}
