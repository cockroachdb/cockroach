//go:build !windows
// +build !windows

package s3manager

func defaultUploadBufferProvider() ReadSeekerWriteToProvider {
	return nil
}
