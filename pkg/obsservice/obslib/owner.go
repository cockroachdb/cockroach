// Copyright 2023 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package obslib

// OwnerTeam denotes the specific team that owns a pipeline, or pipeline
// component, in the platform. OwnerTeam can be used to explicitly attribute
// problematic pipelines to a specific team, which is helpful during incidents
// and support escalations.
type OwnerTeam string

const (
	// ObsInfra is the Observability Infrastructure team.
	Observability OwnerTeam = "observability"
)
