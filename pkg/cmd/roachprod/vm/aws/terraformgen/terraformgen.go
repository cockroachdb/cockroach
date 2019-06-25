// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Command terraformgen generate the terraform file used to configure AWS for
// multiregion support.
package main

import (
	"fmt"
	"io"
	"os"
	"text/template"

	"github.com/spf13/cobra"
)

var templates = []struct {
	name, template string
}{
	{"header",
		`# ---------------------------------------------------------------------------------------------------------------------
# TERRAFORM SETTINGS
# ---------------------------------------------------------------------------------------------------------------------
terraform {
  required_version = ">= 0.11.8"
  backend "s3" {
    key            = "terraform/{{ .ResourcePrefix }}"
    bucket         = "{{ .ResourcePrefix }}-cloud-state"
    region         = "us-east-2"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# Variable names that should not change beyond initial config.
# ---------------------------------------------------------------------------------------------------------------------
locals {
  account_number = {{ printf "%q" .AccountNumber }}
  label          = {{ printf "%q" .ResourcePrefix }}
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS
# ---------------------------------------------------------------------------------------------------------------------`},

	{"regions",
		`{{ range .Regions }}
provider "aws" {
  alias   = "{{ $.Resource . }}"
  region  = "{{ . }}"

  # Fixed fields, DO NOT MODIFY.
  version = "~> 1.41"
}

module "aws_{{ $.Resource . }}" {
  providers {
    aws  = "aws.{{ $.Resource . }}"
  }
  region = {{ . | printf "%q" }}
  source = "aws-region"
  label  = {{ $.ResourcePrefix | printf "%q" }}
}
{{ end }}`},

	{"peerings",
		`{{ range  .Peerings }}
module "vpc_peer_{{index . 0 }}-{{ index . 1 }}" {
  providers {
    aws.owner    = "aws.{{ index . 0 }}"
    aws.peer     = "aws.{{ index . 1 }}"
  }
  owner_vpc_info = "${module.aws_{{index . 0}}.vpc_info}"
  peer_vpc_info  = "${module.aws_{{index . 1}}.vpc_info}"

  label          = {{ $.ResourcePrefix | printf "%q" }}
  source         = "aws-vpc-peer"
}
{{ end }}
`},

	{"output",
		`output "regions" {
  value = "${list({{- range $index, $el := .Regions }}{{ if $index }},{{end}}
    "${module.aws_{{ $.Resource . }}.region_info}"
    {{- end }}
  )}"
}
`},
	{"terraform",
		`{{ template "header" . }}
{{ template "regions" . }}
{{ template "peerings" . }}
{{ template "output" . }}
`},
}

var tmpl = func() *template.Template {
	cur := template.New("base")
	for _, t := range templates {
		cur = template.Must(cur.New(t.name).Parse(t.template))
	}
	return cur
}()

type data struct {
	AccountNumber  string
	ResourcePrefix string
	Regions        []string
}

func (d *data) Resource(s string) string {
	return d.ResourcePrefix + "-" + s
}

func (d *data) Peerings() (peerings [][2]string) {
	for i := 0; i < len(d.Regions); i++ {
		for j := i + 1; j < len(d.Regions); j++ {
			peerings = append(peerings, [2]string{
				d.Resource(d.Regions[i]),
				d.Resource(d.Regions[j]),
			})
		}
	}
	return peerings
}

// Defeat the unused linter because it's not smart enough to know about template
// calls.
var _ = (*data)(nil).Peerings
var _ = (*data)(nil).Resource

var defaultData = data{
	Regions: []string{
		"ap-northeast-1",
		"ap-northeast-2",
		"ap-south-1",
		"ap-southeast-1",
		"ap-southeast-2",
		"ca-central-1",
		"eu-central-1",
		"eu-west-1",
		"eu-west-2",
		"eu-west-3",
		"sa-east-1",
		"us-east-1",
		"us-east-2",
		"us-west-1",
		"us-west-2",
	},
	AccountNumber:  "541263489771",
	ResourcePrefix: "roachprod",
}

func main() {
	data := defaultData
	output := "-"
	rootCmd := &cobra.Command{
		Use:   "terraformgen",
		Short: "terraformgen generates a terraform file for use with roachprod on aws.",
		Long: `
terraformgen generates a terraform main file which, when combined with the 
modules defined in the sibling terraform directory to this programs source code
will set up VPCs and security groups for each of specified regions for
the provided account number. The json artifact created as a result of running
terraform apply is consumed by roachprod.
`,
		Run: func(_ *cobra.Command, _ []string) {
			out := io.Writer(os.Stderr)
			if output != "-" {
				f, err := os.Create(output)
				exitIfError(err)
				defer f.Close()
				out = f
			}
			exitIfError(tmpl.Execute(out, &data))
		},
	}
	rootCmd.Flags().StringSliceVar(&data.Regions, "regions", data.Regions,
		"list of regions to operate in")
	rootCmd.Flags().StringVar(&data.AccountNumber, "account-number", data.AccountNumber,
		"AWS account number to use")
	rootCmd.Flags().StringVarP(&output, "output", "o", output,
		"path to output the generated file, \"-\" for stderr")

	exitIfError(rootCmd.Execute())
}

func exitIfError(err error) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}
