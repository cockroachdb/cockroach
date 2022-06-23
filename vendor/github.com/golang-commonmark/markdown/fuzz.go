//+build gofuzz

package markdown

func Fuzz(data []byte) int {
	md := New(HTML(true))
	md.Parse(data)
	return 1
}
