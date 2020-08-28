// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package telemetry

import "time"

// BucketDuration quantizes a duration.
//
// Values under or equal to a second are quantized as an integer.
//    e.g. 300ms -> 100ms, 40ns -> 10ns
// Values under or equal to a minute are quantized to the nearest 10-second bucket that's smaller or equal.
//    e.g. 35s500ms -> 30s
// Values under or equal to an hour are quantized to the nearest 10-minute bucket that's smaller or equal.
//    e.g. 35m40s -> 30m
// Values under or equal to a day are quantized to the nearest half hour that's smaller or equal.
//    e.g. 5h50m -> 5h30m
// Values between 1 and 10 day inclusive are quantized to the nearest day smaller or equal:
//    - day exactly if the number of days is round, e.g. 10d -> 10d
//    - day+1s if the number is in-between days.    e.g. 4d4h -> 4d1s
// Values above 10 days are quantized as an integer number of days.
//    e.g. 20days -> 10days, 101days -> 100days
func BucketDuration(dur time.Duration) time.Duration {
	sign := time.Duration(1)
	if dur < 0 {
		sign = -1
	}
	if dur <= time.Second && dur >= -time.Second {
		return time.Duration(Bucket10(int64(dur)))
	}
	if dur <= time.Minute && dur >= -time.Minute {
		tenSecondBucket := dur / (10 * time.Second)
		quantized := tenSecondBucket * 10 * time.Second
		if quantized == 0 {
			quantized = time.Second * sign
		}
		return quantized
	}
	if dur <= time.Hour && dur >= -time.Hour {
		tenMinuteBucket := dur / (10 * time.Minute)
		quantized := tenMinuteBucket * 10 * time.Minute
		if quantized == 0 {
			quantized = time.Minute * sign
		}
		return quantized
	}
	const timeDay = 24 * time.Hour
	if dur <= timeDay && dur >= -timeDay {
		halfHourBucket := dur / (30 * time.Minute)
		quantized := halfHourBucket * 30 * time.Minute
		if quantized == 0 {
			quantized = time.Hour * sign
		}
		return quantized
	}
	if dur <= 10*timeDay && dur >= -10*timeDay {
		dayBucket := dur / timeDay
		quantized := dayBucket * timeDay
		if quantized == 0 {
			quantized = timeDay * sign
		}
		if dur != quantized {
			quantized += time.Second * sign
		}
		return quantized
	}
	days := dur / timeDay
	quantized := time.Duration(Bucket10(int64(days))) * timeDay
	return quantized
}
