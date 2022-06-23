package hdrhistogram

import (
	"bufio"
	"io"
	"math"
	"regexp"
	"strconv"
	"strings"
)

type HistogramLogReader struct {
	log               *bufio.Reader
	startTimeSec      float64
	observedStartTime bool
	baseTimeSec       float64
	observedBaseTime  bool

	// scanner handling state
	absolute            bool
	rangeStartTimeSec   float64
	rangeEndTimeSec     float64
	observedMax         bool
	rangeObservedMax    int64
	observedMin         bool
	rangeObservedMin    int64
	reStartTime         *regexp.Regexp
	reBaseTime          *regexp.Regexp
	reHistogramInterval *regexp.Regexp
}

func (hlr *HistogramLogReader) ObservedMin() bool {
	return hlr.observedMin
}

func (hlr *HistogramLogReader) ObservedMax() bool {
	return hlr.observedMax
}

// Returns the overall observed max limit ( up to the current point ) of the read histograms
func (hlr *HistogramLogReader) RangeObservedMax() int64 {
	return hlr.rangeObservedMax
}

// Returns the overall observed min limit ( up to the current point ) of the read histograms
func (hlr *HistogramLogReader) RangeObservedMin() int64 {
	return hlr.rangeObservedMin
}

func NewHistogramLogReader(log io.Reader) *HistogramLogReader {
	//# "#[StartTime: %f (seconds since epoch), %s]\n"
	reStartTime, _ := regexp.Compile(`#\[StartTime: ([\d\.]*)`)

	//# "#[BaseTime: %f (seconds since epoch)]\n"
	reBaseTime, _ := regexp.Compile(`#\[BaseTime: ([\d\.]*)`)

	//# 0.127,1.007,2.769,HISTFAAAAEV42pNpmSz...
	//# Tag=A,0.127,1.007,2.769,HISTFAAAAEV42pNpmSz
	//# "%f,%f,%f,%s\n"
	reHistogramInterval, _ := regexp.Compile(`([\d\.]*),([\d\.]*),([\d\.]*),(.*)`)
	//
	reader := bufio.NewReader(log)

	return &HistogramLogReader{log: reader,
		startTimeSec:        0.0,
		observedStartTime:   false,
		baseTimeSec:         0.0,
		observedBaseTime:    false,
		reStartTime:         reStartTime,
		reBaseTime:          reBaseTime,
		reHistogramInterval: reHistogramInterval,
		rangeObservedMin:    math.MaxInt64,
		observedMin:         false,
		rangeObservedMax:    math.MinInt64,
		observedMax:         false,
	}
}

func (hlr *HistogramLogReader) NextIntervalHistogram() (histogram *Histogram, err error) {
	return hlr.NextIntervalHistogramWithRange(0.0, math.MaxFloat64, true)
}

func (hlr *HistogramLogReader) NextIntervalHistogramWithRange(rangeStartTimeSec, rangeEndTimeSec float64, absolute bool) (histogram *Histogram, err error) {
	hlr.rangeStartTimeSec = rangeStartTimeSec
	hlr.rangeEndTimeSec = rangeEndTimeSec
	hlr.absolute = absolute
	return hlr.decodeNextIntervalHistogram()
}

func (hlr *HistogramLogReader) decodeNextIntervalHistogram() (histogram *Histogram, err error) {
	var line string
	var tag string = ""
	var logTimeStampInSec float64
	var intervalLengthSec float64
	for {
		line, err = hlr.log.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			break
		}
		if line[0] == '#' {
			matchRes := hlr.reStartTime.FindStringSubmatch(line)
			if len(matchRes) > 0 {
				hlr.startTimeSec, err = strconv.ParseFloat(matchRes[1], 64)
				if err != nil {
					return
				}
				hlr.observedStartTime = true
				continue
			}
			matchRes = hlr.reBaseTime.FindStringSubmatch(line)
			if len(matchRes) > 0 {
				hlr.baseTimeSec, err = strconv.ParseFloat(matchRes[1], 64)
				if err != nil {
					return
				}
				hlr.observedBaseTime = true
				continue
			}
			continue
		}

		if strings.HasPrefix(line, "Tag=") {
			commaPos := strings.Index(line, ",")
			tag = line[4:commaPos]
			line = line[commaPos+1:]
		}

		matchRes := hlr.reHistogramInterval.FindStringSubmatch(line)
		if len(matchRes) >= 1 {
			// Decode: startTimestamp, intervalLength, maxTime, histogramPayload
			// Timestamp is expected to be in seconds
			logTimeStampInSec, err = strconv.ParseFloat(matchRes[1], 64)
			if err != nil {
				return
			}
			intervalLengthSec, err = strconv.ParseFloat(matchRes[2], 64)
			if err != nil {
				return
			}
			cpayload := matchRes[4]

			// No explicit start time noted. Use 1st observed time:

			if !hlr.observedStartTime {
				hlr.startTimeSec = logTimeStampInSec
				hlr.observedStartTime = true
			}

			// No explicit base time noted.
			// Deduce from 1st observed time (compared to start time):
			if !hlr.observedBaseTime {
				// Criteria Note: if log timestamp is more than a year in
				// the past (compared to StartTime),
				// we assume that timestamps in the log are not absolute
				if logTimeStampInSec < (hlr.startTimeSec - (365 * 24 * 3600.0)) {
					hlr.baseTimeSec = hlr.startTimeSec
				} else {
					hlr.baseTimeSec = 0.0
				}
				hlr.observedBaseTime = true
			}

			absoluteStartTimeStampSec := logTimeStampInSec + hlr.baseTimeSec
			offsetStartTimeStampSec := absoluteStartTimeStampSec + hlr.startTimeSec

			// Timestamp length is expect to be in seconds
			absoluteEndTimeStampSec := absoluteStartTimeStampSec + intervalLengthSec

			var startTimeStampToCheckRangeOn float64
			if hlr.absolute {
				startTimeStampToCheckRangeOn = absoluteStartTimeStampSec
			} else {
				startTimeStampToCheckRangeOn = offsetStartTimeStampSec
			}

			if startTimeStampToCheckRangeOn < hlr.rangeStartTimeSec {
				continue
			}

			if startTimeStampToCheckRangeOn > hlr.rangeEndTimeSec {
				return
			}
			histogram, err = Decode([]byte(cpayload))
			if err != nil {
				return
			}

			if histogram.Max() > hlr.rangeObservedMax {
				hlr.rangeObservedMax = histogram.Max()
			}

			if histogram.Min() < hlr.rangeObservedMin {
				hlr.rangeObservedMin = histogram.Min()
			}

			histogram.SetStartTimeMs(int64(absoluteStartTimeStampSec * 1000.0))
			histogram.SetEndTimeMs(int64(absoluteEndTimeStampSec * 1000.0))
			if tag != "" {
				histogram.SetTag(tag)
			}
			return
		}
	}
	return
}
