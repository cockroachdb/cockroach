package ts

import (
	"encoding/gob"
	"io"

	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
)

// DumpRawTo is a helper that gob-encodes all messages received from the
// source stream to the given WriteCloser.
func DumpRawTo(src tspb.TimeSeries_DumpRawClient, out io.WriteCloser) error {
	enc := gob.NewEncoder(out)
	for {
		data, err := src.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := enc.Encode(data); err != nil {
			return err
		}
	}
}
