// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package transform

import (
	"github.com/cockroachdb/cockroach/pkg/obsservice/obspb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func GetEventInfo(event *obspb.Event, eventId string) *obspb.EventInfo {
	ts := timeutil.FromUnixNanos(int64(event.LogRecord.TimeUnixNano))

	eventInfo := obspb.EventInfo{
		Timestamp: &ts,
		EventID:   eventId,
		OrgID:     "org_id", // TODO(marylia): replace with real value
	}

	for _, attribute := range event.Resource.Attributes {
		if attribute.Key == obspb.ClusterID {
			eventInfo.ClusterID = attribute.Value.GetStringValue()
		}
		if attribute.Key == obspb.TenantID {
			eventInfo.TenantID = attribute.Value.GetStringValue()
		}
	}

	return &eventInfo
}
