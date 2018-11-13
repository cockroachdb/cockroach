package ui

// Compare error objects by their Error() value.
type ErrorsByError []error

func (l ErrorsByError) Len() int {
	return len(l)
}
func (l ErrorsByError) Less(i, j int) bool {
	return l[i].Error() < l[j].Error()
}
func (l ErrorsByError) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
