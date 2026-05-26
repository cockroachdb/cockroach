// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload

import (
	"context"
	"os"
	"strings"

	"github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV1"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var eventsClient = datadogV1.NewEventsApi(datadog.NewAPIClient(datadog.NewConfiguration()))

// NewDatadogContext adds values to the passed in ctx to configure it to
// communicate with Datadog. If value of site or apiKey is not provided ctx
// is returned without any changes.
func NewDatadogContext(ctx context.Context, site, apiKey string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}

	if site == "" || apiKey == "" {
		return ctx
	}

	ctx = context.WithValue(ctx, datadog.ContextAPIKeys, map[string]datadog.APIKey{
		"apiKeyAuth": {
			Key: apiKey,
		},
	})

	ctx = context.WithValue(ctx, datadog.ContextServerVariables, map[string]string{
		"site": site,
	})
	return ctx
}

// EmitDatadogEvent sends an event to Datadog if the passed in ctx has the necessary values to
// communicate with Datadog.
func EmitDatadogEvent(
	ctx context.Context, title, text string, eventType datadogV1.EventAlertType, tags string,
) {
	_, hasAPIKey := ctx.Value(datadog.ContextAPIKeys).(map[string]datadog.APIKey)
	_, hasServerVariables := ctx.Value(datadog.ContextServerVariables).(map[string]string)
	if !hasAPIKey && !hasServerVariables {
		return
	}

	hostName, _ := os.Hostname()
	_, _, _ = eventsClient.CreateEvent(ctx, datadogV1.EventCreateRequest{
		AlertType:      &eventType,
		DateHappened:   datadog.PtrInt64(timeutil.Now().Unix()),
		Host:           &hostName,
		SourceTypeName: datadog.PtrString("workload"),
		Tags:           getDatadogTags(tags),
		Text:           text,
		Title:          title,
	})
}

func getDatadogTags(tags string) []string {
	return strings.Split(tags, ",")
}
