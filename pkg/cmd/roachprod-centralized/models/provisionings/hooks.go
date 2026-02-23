// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

// SSHConfig defines the SSH connection parameters declared in a template's
// template.yaml. The PrivateKeyVar references an environment variable that
// holds the PEM-encoded private key. Machines is a jq expression evaluated
// against the provisioning outputs to extract target IPs. User is the SSH
// username.
type SSHConfig struct {
	PrivateKeyVar string `yaml:"private_key_var" json:"private_key_var"`
	Machines      string `yaml:"machines" json:"machines"`
	User          string `yaml:"user" json:"user"`
}

// HookTrigger identifies when a hook should execute.
type HookTrigger string

const (
	TriggerPostApply   HookTrigger = "post_apply"
	TriggerManual      HookTrigger = "manual"
	TriggerOnKeyChange HookTrigger = "on_key_change"
)

// RetryConfig specifies retry behavior for a hook. Interval and Timeout are
// duration strings (e.g. "10s", "5m") parsed at execution time.
type RetryConfig struct {
	Interval string `yaml:"interval" json:"interval"`
	Timeout  string `yaml:"timeout" json:"timeout"`
}

// HookEnvMapping maps hook-local variable names to environment variable keys.
// The key is the variable name exposed inside the hook script; the value is the
// key in the resolved environment.
type HookEnvMapping map[string]string

// HookDeclaration represents a single hook declared in a template's
// template.yaml. Hooks execute in the order they appear in the YAML list.
type HookDeclaration struct {
	Name     string         `yaml:"name" json:"name"`
	Type     string         `yaml:"type" json:"type"`
	SSH      bool           `yaml:"ssh" json:"ssh"`
	Machines string         `yaml:"machines,omitempty" json:"machines,omitempty"`
	User     string         `yaml:"user,omitempty" json:"user,omitempty"`
	Command  string         `yaml:"command,omitempty" json:"command,omitempty"`
	Env      HookEnvMapping `yaml:"env,omitempty" json:"env,omitempty"`
	Optional bool           `yaml:"optional,omitempty" json:"optional,omitempty"`
	Retry    *RetryConfig   `yaml:"retry,omitempty" json:"retry,omitempty"`
	Triggers []HookTrigger  `yaml:"triggers" json:"triggers"`
}

// HasTrigger returns true if the hook declares the given trigger.
func (h *HookDeclaration) HasTrigger(t HookTrigger) bool {
	for _, trigger := range h.Triggers {
		if trigger == t {
			return true
		}
	}
	return false
}
