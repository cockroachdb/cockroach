// strftime.go
//
// Copyright (c) 2017 Cockroach Labs
// Copyright (c) 2015 Kyoung-chan Lee (leekchan@gmail.com)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package strtime

// Code taken over from github.com/leekchan/timeutil and extended.

import (
	"fmt"
	"strings"
	"time"
)

var longDayNames = []string{
	"Sunday",
	"Monday",
	"Tuesday",
	"Wednesday",
	"Thursday",
	"Friday",
	"Saturday",
}

var shortDayNames = []string{
	"Sun",
	"Mon",
	"Tue",
	"Wed",
	"Thu",
	"Fri",
	"Sat",
}

var shortMonthNames = []string{
	"---",
	"Jan",
	"Feb",
	"Mar",
	"Apr",
	"May",
	"Jun",
	"Jul",
	"Aug",
	"Sep",
	"Oct",
	"Nov",
	"Dec",
}

var longMonthNames = []string{
	"---",
	"January",
	"February",
	"March",
	"April",
	"May",
	"June",
	"July",
	"August",
	"September",
	"October",
	"November",
	"December",
}

func weekNumber(t time.Time, char int) int {
	weekday := int(t.Weekday())

	if char == 'W' {
		// Monday as the first day of the week
		if weekday == 0 {
			weekday = 6
		} else {
			weekday -= 1
		}
	}

	return (t.YearDay() + 6 - weekday) / 7
}

// Strftime formats time.Date according to the directives in the given format string. The directives begins with a percent (%) character.
func Strftime(t time.Time, f string) (string, error) {
	var result []string
	format := []rune(f)

	add := func(str string) {
		result = append(result, str)
	}

	for i := 0; i < len(format); i++ {
		switch format[i] {
		case '%':
			if i < len(format)-1 {
				switch format[i+1] {
				case 'a':
					add(shortDayNames[t.Weekday()])
				case 'A':
					add(longDayNames[t.Weekday()])
				case 'b', 'h':
					add(shortMonthNames[t.Month()])
				case 'B':
					add(longMonthNames[t.Month()])
				case 'c':
					add(t.Format("Mon Jan 2 15:04:05 2006"))
				case 'C':
					add(fmt.Sprintf("%02d", t.Year()/100))
				case 'd':
					add(fmt.Sprintf("%02d", t.Day()))
				case 'D', 'x':
					add(fmt.Sprintf("%02d/%02d/%02d", t.Month(), t.Day(), t.Year()%100))
				case 'e':
					add(fmt.Sprintf("%2d", t.Day()))
				case 'f':
					add(fmt.Sprintf("%06d", t.Nanosecond()/1000))
				case 'F':
					add(fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day()))
				case 'H':
					add(fmt.Sprintf("%02d", t.Hour()))
				case 'I':
					if t.Hour() == 0 {
						add(fmt.Sprintf("%02d", 12))
					} else if t.Hour() > 12 {
						add(fmt.Sprintf("%02d", t.Hour()-12))
					} else {
						add(fmt.Sprintf("%02d", t.Hour()))
					}
				case 'j':
					add(fmt.Sprintf("%03d", t.YearDay()))
				case 'k':
					add(fmt.Sprintf("%2d", t.Hour()))
				case 'l':
					if t.Hour() == 0 {
						add(fmt.Sprintf("%2d", 12))
					} else if t.Hour() > 12 {
						add(fmt.Sprintf("%2d", t.Hour()-12))
					} else {
						add(fmt.Sprintf("%2d", t.Hour()))
					}
				case 'm':
					add(fmt.Sprintf("%02d", t.Month()))
				case 'M':
					add(fmt.Sprintf("%02d", t.Minute()))
				case 'n':
					add("\n")
				case 'p':
					if t.Hour() < 12 {
						add("AM")
					} else {
						add("PM")
					}
				case 'r':
					s, _ := Strftime(t, "%I:%M:%S %p")
					add(s)
				case 'R':
					add(fmt.Sprintf("%02d:%02d", t.Hour(), t.Minute()))
				case 's':
					add(fmt.Sprintf("%d", t.Unix()))
				case 'S':
					add(fmt.Sprintf("%02d", t.Second()))
				case 't':
					add("\t")
				case 'T', 'X':
					add(fmt.Sprintf("%02d:%02d:%02d", t.Hour(), t.Minute(), t.Second()))
				case 'u':
					w := t.Weekday()
					if w == 0 {
						w = 7
					}
					add(fmt.Sprintf("%d", w))
				case 'U':
					add(fmt.Sprintf("%02d", weekNumber(t, 'U')))
				case 'w':
					add(fmt.Sprintf("%d", t.Weekday()))
				case 'W':
					add(fmt.Sprintf("%02d", weekNumber(t, 'W')))
				case 'y':
					add(fmt.Sprintf("%02d", t.Year()%100))
				case 'Y':
					add(fmt.Sprintf("%02d", t.Year()))
				case 'z':
					add(t.Format("-0700"))
				case 'Z':
					add(t.Format("MST"))
				case '%':
					add("%")
				default:
					return "", fmt.Errorf("invalid format code: %c", format[i+1])
				}
				i += 1
			}
		default:
			add(string(format[i]))
		}
	}

	return strings.Join(result, ""), nil
}
