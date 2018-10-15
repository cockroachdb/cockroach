// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgdate

var daysInMonth = [2][13]int{
	{0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
	{0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
}

// dateToJulianDay is based on the date2j function in PostgreSQL 10.5.
func dateToJulianDay(year int, month int, day int) int {
	if month > 2 {
		month += 1
		year += 4800
	} else {
		month += 13
		year += 4799
	}

	century := year / 100
	jd := year*365 - 32167
	jd += year/4 - century + century/4
	jd += 7834*month/256 + day

	return jd
}

// isLeap returns true if the given year is a leap year.
func isLeap(year int) bool {
	return (year%4 == 0) && (year%100 != 0 || year%400 == 0)
}

// julianDayToDate is based on the j2date function in PostgreSQL 10.5.
func julianDayToDate(j int) (year int, month int, day int) {

	jd := uint(j)
	jd += 32044
	quad := jd / 146097
	extra := (jd-quad*146097)*4 + 3
	jd += 60 + quad*3 + extra/146097
	quad = jd / 1461
	jd -= quad * 1461
	y := jd * 4 / 1461
	if y != 0 {
		jd = (jd + 305) % 365
	} else {
		jd = (jd + 306) % 366
	}
	jd += 123
	y += quad * 4
	year = int(y - 4800)
	quad = jd * 2141 / 65536
	day = int(jd - 7834*quad/256)
	month = int((quad+10)%12 + 1)

	return
}
