package protokit

import (
	"github.com/golang/protobuf/protoc-gen-go/descriptor"

	"bytes"
	"strconv"
	"strings"
)

// A Comment describes the leading, trailing, and detached comments for a proto object. See `SourceCodeInfo_Location` in
// descriptor.proto for details on what those terms mean
type Comment struct {
	Leading  string
	Trailing string
	Detached []string
}

// String returns the leading and trailing comments joined by 2 line breaks (`\n\n`). If either are empty, the line
// breaks are removed.
func (c *Comment) String() string {
	b := new(bytes.Buffer)
	if c.GetLeading() != "" {
		b.WriteString(c.GetLeading())
		b.WriteString("\n\n")
	}

	b.WriteString(c.GetTrailing())

	return strings.TrimSpace(b.String())
}

func newComment(loc *descriptor.SourceCodeInfo_Location) *Comment {
	detached := make([]string, len(loc.GetLeadingDetachedComments()))
	for i, c := range loc.GetLeadingDetachedComments() {
		detached[i] = scrub(c)
	}

	return &Comment{
		Leading:  scrub(loc.GetLeadingComments()),
		Trailing: scrub(loc.GetTrailingComments()),
		Detached: detached,
	}
}

// GetLeading returns the leading comments
func (c *Comment) GetLeading() string { return c.Leading }

// GetTrailing returns the leading comments
func (c *Comment) GetTrailing() string { return c.Trailing }

// GetDetached returns the detached leading comments
func (c *Comment) GetDetached() []string { return c.Detached }

// Comments is a map of source location paths to values.
type Comments map[string]*Comment

// ParseComments parses all comments within a proto file. The locations are encoded into the map by joining the paths
// with a "." character. E.g. `4.2.3.0`.
//
// Leading/trailing spaces are trimmed for each comment type (leading, trailing, detached)
func ParseComments(fd *descriptor.FileDescriptorProto) Comments {
	comments := make(Comments)

	for _, loc := range fd.GetSourceCodeInfo().GetLocation() {
		if loc.GetLeadingComments() == "" && loc.GetTrailingComments() == "" && len(loc.GetLeadingDetachedComments()) == 0 {
			continue
		}

		path := loc.GetPath()
		key := make([]string, len(path))
		for idx, p := range path {
			key[idx] = strconv.Itoa(int(p))
		}

		comments[strings.Join(key, ".")] = newComment(loc)
	}

	return comments
}

func (c Comments) Get(path string) *Comment {
	if val, ok := c[path]; ok {
		return val
	}

	// return an empty comment
	return &Comment{Detached: make([]string, 0)}
}

func scrub(str string) string {
	return strings.TrimSpace(strings.Replace(str, "\n ", "\n", -1))
}
