package ltree

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseLTree(t *testing.T) {
	tests := []struct {
		input   string
		want    string
		wantErr bool
	}{
		{
			input: "",
			want:  "",
		},
		{
			input: "a",
			want:  "a",
		},
		{
			input: "a.b.c",
			want:  "a.b.c",
		},
		{
			input: "a-0.B_9.z",
			want:  "a-0.B_9.z",
		},
		{
			input: "123.456",
			want:  "123.456",
		},
		{
			input: "Top.Middle.bottom",
			want:  "Top.Middle.bottom",
		},
		{
			input: "hello_world.test-case",
			want:  "hello_world.test-case",
		},
		{
			input:   "hello world",
			wantErr: true,
		},
		{
			input:   "hello..world",
			wantErr: true,
		},
		{
			input:   "hello@world",
			wantErr: true,
		},
		{
			input:   "test.৩.path",
			wantErr: true,
		},
		{
			input:   strings.Repeat("a", maxLabelLength+1),
			wantErr: true,
		},
		{
			input:   strings.Repeat("a.", maxNumOfLabels) + "a",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := ParseLTree(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.want, got.String())
		})
	}
}

func TestPrev(t *testing.T) {
	tests := []struct {
		input     string
		expected  string
		hasResult bool
	}{
		{
			input:     "",
			expected:  "",
			hasResult: false,
		},
		{
			input:     "b",
			expected:  "a",
			hasResult: true,
		},
		{
			input:     "a",
			expected:  "_",
			hasResult: true,
		},
		{
			input:     "_",
			expected:  "Z",
			hasResult: true,
		},
		{
			input:     "Z",
			expected:  "Y",
			hasResult: true,
		},
		{
			input:     "A",
			expected:  "9",
			hasResult: true,
		},
		{
			input:     "9",
			expected:  "8",
			hasResult: true,
		},
		{
			input:     "0",
			expected:  "-",
			hasResult: true,
		},
		{
			input:     "-",
			expected:  "",
			hasResult: true,
		},
		{
			input:     "ab",
			expected:  "aa",
			hasResult: true,
		},
		{
			input:     "a-",
			expected:  "a",
			hasResult: true,
		},
		{
			input:     "A.B",
			expected:  "A.A",
			hasResult: true,
		},
		{
			input:     "A.A",
			expected:  "A.9",
			hasResult: true,
		},
		{
			input:     "A.-",
			expected:  "A",
			hasResult: true,
		},
		{
			input:     "a.b.c",
			expected:  "a.b.b",
			hasResult: true,
		},
		{
			input:     "-.-.a",
			expected:  "-.-._",
			hasResult: true,
		},
		{
			input:     "-.-.-",
			expected:  "-.-",
			hasResult: true,
		},
		{
			input:     "abc.def",
			expected:  "abc.dee",
			hasResult: true,
		},
		{
			input:     "A",
			expected:  "9",
			hasResult: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			var input T
			var err error
			if tt.input == "" {
				input = Empty
			} else {
				input, err = ParseLTree(tt.input)
				require.NoError(t, err)
			}

			result, hasResult := input.Prev()

			require.Equal(t, tt.hasResult, hasResult)

			if tt.hasResult {
				require.Equal(t, tt.expected, result.String())
			}
		})
	}
}
