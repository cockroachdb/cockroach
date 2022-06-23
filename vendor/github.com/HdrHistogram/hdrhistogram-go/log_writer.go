//The log format encodes into a single file, multiple histograms with optional shared meta data.
package hdrhistogram

import (
	"fmt"
	"io"
	"regexp"
	"time"
)

const HISTOGRAM_LOG_FORMAT_VERSION = "1.3"
const MsToNsRatio float64 = 1000000.0

type HistogramLogOptions struct {
	startTimeStampSec float64
	endTimeStampSec   float64
	maxValueUnitRatio float64
}

func DefaultHistogramLogOptions() *HistogramLogOptions {
	return &HistogramLogOptions{0, 0, MsToNsRatio}
}

type HistogramLogWriter struct {
	baseTime int64
	log      io.Writer
}

// Return the current base time offset
func (lw *HistogramLogWriter) BaseTime() int64 {
	return lw.baseTime
}

// Set a base time to subtract from supplied histogram start/end timestamps when
// logging based on histogram timestamps.
// baseTime is expected to be in msec since the epoch, as histogram start/end times
// are typically stamped with absolute times in msec since the epoch.
func (lw *HistogramLogWriter) SetBaseTime(baseTime int64) {
	lw.baseTime = baseTime
}

func NewHistogramLogWriter(log io.Writer) *HistogramLogWriter {
	return &HistogramLogWriter{baseTime: 0, log: log}
}

// Output an interval histogram, using the start/end timestamp indicated in the histogram, and the [optional] tag associated with the histogram.
// The histogram start and end timestamps are assumed to be in msec units
//
// By convention, histogram start/end time are generally stamped with absolute times in msec
// since the epoch. For logging with absolute time stamps, the base time would remain zero ( default ).
// For logging with relative time stamps (time since a start point), the base time should be set with SetBaseTime(baseTime int64)
//
// The max value in the histogram will be reported scaled down by a default maxValueUnitRatio of 1000000.0 (which is the msec : nsec ratio).
// If you need to specify a different start/end timestamp or a different maxValueUnitRatio you should use OutputIntervalHistogramWithLogOptions(histogram *Histogram, logOptions *HistogramLogOptions)
func (lw *HistogramLogWriter) OutputIntervalHistogram(histogram *Histogram) (err error) {
	return lw.OutputIntervalHistogramWithLogOptions(histogram, nil)
}

// Output an interval histogram, with the given timestamp information and the [optional] tag associated with the histogram
//
// If you specify non-nil logOptions, and non-zero start timestamp, the the specified timestamp information will be used, and the start timestamp information in the actual histogram will be ignored.
// If you specify non-nil logOptions, and non-zero start timestamp, the the specified timestamp information will be used, and the end timestamp information in the actual histogram will be ignored.
// If you specify non-nil logOptions, The max value reported with the interval line will be scaled by the given maxValueUnitRatio,
// otherwise  a default maxValueUnitRatio of 1,000,000 (which is the msec : nsec ratio) will be used.
//
// By convention, histogram start/end time are generally stamped with absolute times in msec
// since the epoch. For logging with absolute time stamps, the base time would remain zero ( default ).
// For logging with relative time stamps (time since a start point), the base time should be set with SetBaseTime(baseTime int64)
func (lw *HistogramLogWriter) OutputIntervalHistogramWithLogOptions(histogram *Histogram, logOptions *HistogramLogOptions) (err error) {
	tag := histogram.Tag()
	var match bool
	tagStr := ""
	if tag != "" {
		match, err = regexp.MatchString(".[, \\r\\n].", tag)
		if err != nil {
			return
		}
		if match {
			err = fmt.Errorf("Tag string cannot contain commas, spaces, or line breaks. Used tag: %s", tag)
			return
		}
		tagStr = fmt.Sprintf("Tag=%s,", tag)
	}
	var usedStartTime float64 = float64(histogram.StartTimeMs())
	var usedEndTime float64 = float64(histogram.EndTimeMs())
	var maxValueUnitRatio float64 = MsToNsRatio
	if logOptions != nil {
		if logOptions.startTimeStampSec != 0 {
			usedStartTime = logOptions.startTimeStampSec
		}
		if logOptions.endTimeStampSec != 0 {
			usedEndTime = logOptions.endTimeStampSec
		}
		maxValueUnitRatio = logOptions.maxValueUnitRatio
	}
	startTime := usedStartTime - float64(lw.baseTime)/1000.0
	endTime := usedEndTime - float64(lw.baseTime)/1000.0
	maxValueAsDouble := float64(histogram.Max()) / maxValueUnitRatio
	cpayload, err := histogram.Encode(V2CompressedEncodingCookieBase)
	if err != nil {
		return
	}
	_, err = lw.log.Write([]byte(fmt.Sprintf("%s%f,%f,%f,%s\n", tagStr, startTime, endTime, maxValueAsDouble, string(cpayload))))
	return
}

// Log a start time in the log.
// Start time is represented as seconds since epoch with up to 3 decimal places. Line starts with the leading text '#[StartTime:'
func (lw *HistogramLogWriter) OutputStartTime(start_time_msec int64) (err error) {
	secs := start_time_msec / 1000
	iso_str := time.Unix(secs, start_time_msec%int64(1000)*int64(1000000000)).Format(time.RFC3339)
	_, err = lw.log.Write([]byte(fmt.Sprintf("#[StartTime: %d (seconds since epoch), %s]\n", secs, iso_str)))
	return
}

// Log a base time in the log.
// Base time is represented as seconds since epoch with up to 3 decimal places. Line starts with the leading text '#[BaseTime:'
func (lw *HistogramLogWriter) OutputBaseTime(base_time_msec int64) (err error) {
	secs := base_time_msec / 1000
	_, err = lw.log.Write([]byte(fmt.Sprintf("#[Basetime: %d (seconds since epoch)]\n", secs)))
	return
}

// Log a comment to the log.
// A comment is any line that leads with '#' that is not matched by the BaseTime or StartTime formats. Comments are ignored when parsed.
func (lw *HistogramLogWriter) OutputComment(comment string) (err error) {
	_, err = lw.log.Write([]byte(fmt.Sprintf("#%s\n", comment)))
	return
}

// Output a legend line to the log.
// Human readable column headers. Ignored when parsed.
func (lw *HistogramLogWriter) OutputLegend() (err error) {
	_, err = lw.log.Write([]byte("\"StartTimestamp\",\"Interval_Length\",\"Interval_Max\",\"Interval_Compressed_Histogram\"\n"))
	return
}

// Output a log format version to the log.
func (lw *HistogramLogWriter) OutputLogFormatVersion() (err error) {
	return lw.OutputComment(fmt.Sprintf("[Histogram log format version %s]", HISTOGRAM_LOG_FORMAT_VERSION))
}
