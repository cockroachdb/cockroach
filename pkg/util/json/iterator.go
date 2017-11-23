package json

// ObjectKeyIterator is an iterator to access the keys of an object.
type ObjectKeyIterator struct {
	src jsonObject
	idx int
}

// Next returns true and the next key in the iterator if one exists,
// and false otherwise.
func (it *ObjectKeyIterator) Next() (bool, string) {
	it.idx++
	if it.idx >= len(it.src) {
		return false, ""
	}
	return true, *it.src[it.idx].k.AsText()
}
