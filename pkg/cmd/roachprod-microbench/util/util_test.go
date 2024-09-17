// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLabelMapToString(t *testing.T) {
	require.Equal(t, "_f__HPNJPI__Y_lkjf_ew=\"477777_sqsq_w_q\",_deifbie09=\"6\","+
		"_fclia7580=\"4\",a__2dlds=\"p\",b_2dwo=\"value1\",d__2344as=\"q\",pclia75_s80=\"4\"",
		LabelMapToString(map[string]string{
			"b$2dwo":     "value1",
			"d__2344as":  "q",
			"a+=2dlds":   "p",
			"=fclia7580": "4",
			`pclia75
s80`: "4",
			"+f(*HPNJPI&*Y'lkjf[ew": `477777
sqsq"w\q`,
			"9deifbie09": "6",
		}))
}

func TestSanitizeKey(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{"test", "test"},
		{"test/sla//sh", "test_sla__sh"},
		{"5words", "_words"},
		{"b$2dwo", "b_2dwo"},
		{`477777
nsqsq"w\q`, "_77777_nsqsq_w_q"},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.output, SanitizeKey(tc.input))
		})
	}
}

func TestSanitizeValue(t *testing.T) {
	testCases := []struct {
		input  string
		output string
	}{
		{"test", "test"},
		{"test/sla//sh", "test/sla//sh"},
		{"b$2dwo", "b$2dwo"},
		{`477777
nsqsq"w\q`, "477777_nsqsq_w_q"},
	}
	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.output, SanitizeValue(tc.input))
		})
	}
}
