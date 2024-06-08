// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/cmd/drtprod/helpers"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

const (
	listMaxResults    = "10000"
	defaultDnsProject = "cockroach-shared"
)

// DNSRecord as read from DNS list
type DNSRecord struct {
	Name       string   `json:"name"`
	Kind       string   `json:"kind"`
	RecordType string   `json:"type"`
	TTL        int      `json:"ttl"`
	RRDatas    []string `json:"rrdatas"`
}

// hostIPRegExp extracts the ip from the host admin url
var hostIPRegExp = regexp.MustCompile(`//([\d.]+):\d+`)

func GetDnsCmd(ctx context.Context) *cobra.Command {
	return &cobra.Command{
		Use:   "dns <cluster>",
		Short: "manages dns for drt prod cluster",
		Long: `Manages dns for drt prod cluster.
roachprod only manages DNS in ephemeral, so we just do this ourselves.
These are very low-churn clusters so this is fine being manual and in a wrapper.
The command creates the DNS record sets if it does not exist, or it updates the same.
`,
		Args: cobra.ExactArgs(1),
		Run: helpers.Wrap(func(cmd *cobra.Command, args []string) (retErr error) {
			clusterName := args[0]
			if err := helpers.ValidateClusterName(clusterName); err != nil {
				return err
			}
			return ConfigureDns(ctx, clusterName)
		}),
	}
}

// ConfigureDns configures the DNS for the cluster based on the action
func ConfigureDns(ctx context.Context, clusterName string) error {
	hosts, err := roachprod.AdminURL(ctx, config.Logger, clusterName, "", 0, "", true, false, true)
	if err != nil {
		return err
	}
	for hostIndex, host := range hosts {
		matches := hostIPRegExp.FindStringSubmatch(host)
		if len(matches) < 2 {
			return errors.Newf("host %s could not be parsed for IP.", host)
		}
		ip := matches[1]
		name := fmt.Sprintf("%s-%04d.%s.", clusterName, hostIndex+1, helpers.DefaultDns)
		existingRecords, err := listRecordSets(ctx, name)
		if err != nil {
			return err
		}
		action := "create"
		if len(existingRecords) > 0 {
			// update as record is already present
			// checking for more than one record s not necessary as these records
			// are created by the same code
			action = "update"
		}
		if err = createOrUpdateRecord(ctx, name, action, ip); err != nil {
			return err
		}
	}
	return nil
}

// createOrUpdateRecord creates or updates the record based on the action
func createOrUpdateRecord(ctx context.Context, name, action, ip string) error {
	args := []string{"--project", defaultDnsProject, "dns", "record-sets", action, name,
		"--type", "A",
		"--ttl", "60",
		"--zone", helpers.DNSZone,
		"--rrdatas", ip,
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	fmt.Println(cmd.String())
	_, err := cmd.CombinedOutput()
	if err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "failed to list dns records")
	}
	return nil
}

// listRecordSets lists all records for the name
func listRecordSets(ctx context.Context, name string) ([]DNSRecord, error) {
	args := []string{"--project", defaultDnsProject, "dns", "record-sets", "list",
		"--limit", listMaxResults,
		"--page-size", listMaxResults,
		"--zone", helpers.DNSZone,
		"--filter", name,
		"--format", "json",
	}
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	res, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to list dns records")
	}
	var jsonList []DNSRecord
	err = json.Unmarshal(res, &jsonList)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err, "failed to unlarshall dns records")
	}
	return jsonList, nil
}
