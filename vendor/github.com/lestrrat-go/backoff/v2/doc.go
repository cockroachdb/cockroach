// Package backoff implments backoff algorithms for retrying operations.
//
// Users first create an appropriate `Policy` object, and when the operation
// that needs retrying is about to start, they kick the actual backoff
// 
package backoff