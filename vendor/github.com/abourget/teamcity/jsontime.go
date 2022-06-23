package teamcity

import "time"

// JSONTime is just a string wrapper for Team City's date format.
type JSONTime string

// Time parses the string to extract a time.Time object.
func (t JSONTime) Time() time.Time {
	t0, err := time.Parse("20060102T150405-0700", string(t))
	if err != nil {
		return time.Time{}
	}
	return t0
}

// Empty checks if the JSONTime was specified at all
func (t JSONTime) Empty() bool {
	return t == ""
}

// IsZero returns whether the date is zero.
func (t JSONTime) IsZero() bool {
	return t.Time().IsZero()
}
