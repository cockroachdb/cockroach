// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package server

import (
	"flag"
	"os"
)

// Environment variables that inform the locality of where the process runs.
const (
	EnvVarDatacenter = "DATACENTER"
	EnvVarPDU        = "PDU"
	EnvVarRack       = "RACK"
)

var (
	datacenter = flag.String("datacenter", "", "datacenter identifier; used to allocate according to zone configs")
	pdu        = flag.String("pdu", "", "power distribution unit (PDU); used to minimize correlated failures")
	rack       = flag.String("rack", "", "rack identifier; used to minimize correlated failures")
)

// getFlagOrEnvVar returns the value of the flag variable if not
// empty; otherwise, the environment variable named by envVar is
// returned via a call to getEnvVar().
func getFlagOrEnvVar(flag, envVar string) string {
	if flag != "" {
		return flag
	}
	return os.Getenv(envVar)
}

// getDatacenter determines the datacenter either through a command
// line flag (-datacenter) or from the DATACENTER environment
// variable. If neither is available, returns an empty string.
func getDatacenter() string {
	return getFlagOrEnvVar(*datacenter, EnvVarDatacenter)
}

// getPDU determines the power distribution unit either through a
// command line flag (-pdu) or from the PDU environment variable. If
// neither is available, returns an empty string.
func getPDU() string {
	return getFlagOrEnvVar(*pdu, EnvVarPDU)
}

// getRack determines the hardware rack identifier either through a
// command line flag (-rack) or from the RACK environment
// variable. If neither is available, returns an empty string.
func getRack() string {
	return getFlagOrEnvVar(*rack, EnvVarRack)
}
