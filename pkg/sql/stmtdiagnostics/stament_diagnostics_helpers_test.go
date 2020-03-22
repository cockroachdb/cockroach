package stmtdiagnostics

import "context"

// RequestID exposes the definition of requestID to tests in this package.
type RequestID = requestID

// InsertRequestInternal exposes the form of insert which returns the RequestID
// to tests in this package.
func (r *Registry) InsertRequestInternal(ctx context.Context, fprint string) (RequestID, error) {
	return r.insertRequestInternal(ctx, fprint)
}
