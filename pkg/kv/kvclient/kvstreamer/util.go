package kvstreamer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// GetScanRows returns the Rows field from a ScanResponse or
// ReverseScanResponse.
func GetScanRows(response kvpb.Response) (rows []roachpb.KeyValue) {
	switch scan := response.(type) {
	case *kvpb.ScanResponse:
		return scan.Rows
	case *kvpb.ReverseScanResponse:
		return scan.Rows
	}
	panic(errors.AssertionFailedf("unexpected response: %v", response))
}

// GetScanIntentRows returns the IntentRows field from a ScanResponse or
// ReverseScanResponse.
func GetScanIntentRows(response kvpb.Response) (intentRows []roachpb.KeyValue) {
	switch scan := response.(type) {
	case *kvpb.ScanResponse:
		return scan.IntentRows
	case *kvpb.ReverseScanResponse:
		return scan.IntentRows
	}
	panic(errors.AssertionFailedf("unexpected response: %v", response))
}

// GetScanBatchResponses returns the BatchResponses field from a ScanResponse or
// ReverseScanResponse.
func GetScanBatchResponses(response kvpb.Response) (batchResponses [][]byte) {
	switch scan := response.(type) {
	case *kvpb.ScanResponse:
		return scan.BatchResponses
	case *kvpb.ReverseScanResponse:
		return scan.BatchResponses
	}
	panic(errors.AssertionFailedf("unexpected response: %v", response))
}

// GetScanResumeSpan returns the ResumeSpan field from a ScanResponse or
// ReverseScanResponse.
func GetScanResumeSpan(response kvpb.Response) (resumeSpan *roachpb.Span) {
	switch scan := response.(type) {
	case *kvpb.ScanResponse:
		return scan.ResumeSpan
	case *kvpb.ReverseScanResponse:
		return scan.ResumeSpan
	}
	panic(errors.AssertionFailedf("unexpected response: %v", response))
}

// GetScanResumeNextBytes returns the ResumeNextBytes field from a ScanResponse
// or ReverseScanResponse.
func GetScanResumeNextBytes(response kvpb.Response) (resumeNextBytes int64) {
	switch scan := response.(type) {
	case *kvpb.ScanResponse:
		return scan.ResumeNextBytes
	case *kvpb.ReverseScanResponse:
		return scan.ResumeNextBytes
	}
	panic(errors.AssertionFailedf("unexpected response: %v", response))
}
