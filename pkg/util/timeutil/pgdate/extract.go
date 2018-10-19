package pgdate

import (
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
)

const (
	keywordAllBalls  = "allballs"
	keywordAM        = "am"
	keywordEpoch     = "epoch"
	keywordEraAD     = "ad"
	keywordEraBC     = "bc"
	keywordEraBCE    = "bce"
	keywordEraCE     = "ce"
	keywordInfinity  = "infinity"
	keywordNow       = "now"
	keywordPM        = "pm"
	keywordToday     = "today"
	keywordTomorrow  = "tomorrow"
	keywordYesterday = "yesterday"
)

// extract attempts to break the input string into a collection
// of date/time fields in order to populate a fieldExtract.  The
// fieldExtract returned from this function may not be the one
// passed in if a sentinel value is returned. An error will be
// returned if not all of the required fields have been populated.
func extract(fe fieldExtract, s string) (time.Time, error) {

	// Break the string into alphanumeric chunks.
	chunks, _ := filterSplitString(s, func(r rune) bool {
		return unicode.IsDigit(r) || unicode.IsLetter(r)
	})

	numbers := make([]numberChunk, 0, len(chunks))
	var sentinel *time.Time

	// First, we'll try to pluck out any keywords that exist in the input.
	// If a chunk is not a keyword or other special-case pattern, it
	// must be a numeric value, which we'll pluck out for a second
	// pass. If we see certain sentinel values, we'll pick them out,
	// but keep going to ensure that the user hasn't written something
	// like "epoch infinity".
	for _, chunk := range chunks {
		match := strings.ToLower(chunk.Match)
		matchedSentinel := func() error {
			if sentinel != nil {
				return errors.Errorf("unexpected input: %s", match)
			}
			return nil
		}

		switch match {
		case keywordEpoch:
			if err := matchedSentinel(); err != nil {
				return TimeEpoch, err
			}
			sentinel = &TimeEpoch

		case keywordInfinity:
			if err := matchedSentinel(); err != nil {
				return TimeEpoch, err
			}
			if strings.HasSuffix(chunk.NotMatch, "-") {
				sentinel = &TimeNegativeInfinity
			} else {
				sentinel = &TimeInfinity
			}

		case keywordNow:
			if err := matchedSentinel(); err != nil {
				return TimeEpoch, err
			}
			sentinel = &fe.now

		default:
			// Fan out to other keyword-based extracts.
			if m, ok := keywordSetters[match]; ok {
				if err := m(&fe, match); err != nil {
					return TimeEpoch, err
				}
				continue
			}

			// Try to parse Julian dates.  We can disregard the error here.
			if err := fieldSetterJulianDate(&fe, match); err == nil {
				continue
			}

			// At this point, the chunk must be numeric; we'll convert it
			// and add it to the slice for the second pass.
			v, err := strconv.Atoi(match)
			if err != nil {
				return TimeEpoch, errors.Errorf("unexpected input chunk: %s", match)
			}
			lastRune, _ := utf8.DecodeLastRuneInString(chunk.NotMatch)
			numbers = append(numbers, numberChunk{prefix: lastRune, v: v, magnitude: len(match)})
		}
	}

	if sentinel != nil {
		return *sentinel, nil
	}

	// In the second pass, we'll use pattern-matching and the knowledge
	// of which fields have already been set in order to keep picking
	// out field data.
	textMonth := !fe.Wants(fieldMonth)
	for _, n := range numbers {
		if err := fe.interpretNumber(n, textMonth); err != nil {
			return TimeEpoch, err
		}
	}

	if err := fe.Validate(); err != nil {
		return TimeEpoch, err
	}

	switch {
	case fe.has.HasAll(dateTimeRequiredFields):
	case fe.has.HasAll(dateRequiredFields):
		year, _ := fe.Get(fieldYear)
		month, _ := fe.Get(fieldMonth)
		day, _ := fe.Get(fieldDay)
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, fe.now.Location()), nil

	case fe.has.HasAll(timeRequiredFields):
	}

	return TimeEpoch, errors.New("unimplemented")
}

// splitChunks are returned from filterSplitString.
type splitChunk struct {
	// The contiguous span of characters that did not match the filter and
	// which appear immediately before Match.
	NotMatch string
	// The contiguous span of characters that matched the filter.
	Match string
}

// filterSplitString filters the runes in a string and returns
// contiguous spans of acceptable characters.
func filterSplitString(s string, accept func(rune) bool) ([]splitChunk, string) {
	ret := make([]splitChunk, 0, 8)

	matchStart := 0
	matchEnd := 0
	previousMatchEnd := 0

	flush := func() {
		if matchEnd > matchStart {
			ret = append(ret, splitChunk{
				NotMatch: s[previousMatchEnd:matchStart],
				Match:    s[matchStart:matchEnd]})
			previousMatchEnd = matchEnd
			matchStart = matchEnd
		}
	}

	for offset, r := range s {
		if accept(r) {
			if matchStart >= matchEnd {
				matchStart = offset
			}
			matchEnd = offset + utf8.RuneLen(r)
		} else {
			flush()
		}
	}
	flush()

	return ret, s[matchEnd:]
}
