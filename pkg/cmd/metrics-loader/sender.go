// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"bytes"
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
)

type Sender interface {
	// Send sends a slice of time series to a remote storage.
	Send(timeSeriesSlice []prompb.TimeSeries) error
}

type httpSender struct {
	url string
}

// NewHTTPSender creates a new HTTP sender with a given Prometheus remote-write
// endpoint URL to send time series data to.
// See: https://prometheus.io/docs/prometheus/latest/storage/#remote-storage-integrations
func NewHTTPSender(url string) Sender {
	return &httpSender{
		url: url,
	}
}

// Send implements the Sender interface.
func (c *httpSender) Send(timeSeriesSlice []prompb.TimeSeries) error {
	writeRequest := &prompb.WriteRequest{
		Timeseries: timeSeriesSlice,
	}

	reqData, err := protoutil.Marshal(writeRequest)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal WriteRequest")
	}
	encData := snappy.Encode(nil, reqData)
	req, err := http.NewRequest("POST", c.url, bytes.NewBuffer(encData))
	if err != nil {
		return errors.Wrapf(err, "failed to create HTTP request")
	}

	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Content-Encoding", "snappy")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "failed to send HTTP request")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.Newf("unexpected status code %d", resp.StatusCode)
	}
	return nil
}
