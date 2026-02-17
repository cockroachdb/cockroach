// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisionings

import (
	"crypto/rand"
	"encoding/json"
	"log/slog"
	"math/big"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ProvisioningState represents the lifecycle state of a provisioning.
type ProvisioningState string

const (
	ProvisioningStateNew           ProvisioningState = "new"
	ProvisioningStateInitializing  ProvisioningState = "initializing"
	ProvisioningStatePlanning      ProvisioningState = "planning"
	ProvisioningStateProvisioning  ProvisioningState = "provisioning"
	ProvisioningStateProvisioned   ProvisioningState = "provisioned"
	ProvisioningStateFailed        ProvisioningState = "failed"
	ProvisioningStateDestroying    ProvisioningState = "destroying"
	ProvisioningStateDestroyed     ProvisioningState = "destroyed"
	ProvisioningStateDestroyFailed ProvisioningState = "destroy_failed"
	// Future work: cancellation support is not yet wired into the state
	// machine (expectedTransitions). These constants are defined for forward
	// compatibility but have no transitions or task handlers yet.
	ProvisioningStateCancelling ProvisioningState = "cancelling"
	ProvisioningStateCancelled  ProvisioningState = "cancelled"
)

// expectedTransitions defines the valid state machine transitions.
// Used for logging warnings; not enforced in PoC.
var expectedTransitions = map[ProvisioningState][]ProvisioningState{
	ProvisioningStateNew:           {ProvisioningStateInitializing, ProvisioningStateFailed, ProvisioningStateDestroyed},
	ProvisioningStateInitializing:  {ProvisioningStatePlanning, ProvisioningStateFailed},
	ProvisioningStatePlanning:      {ProvisioningStateProvisioning, ProvisioningStateFailed},
	ProvisioningStateProvisioning:  {ProvisioningStateProvisioned, ProvisioningStateFailed},
	ProvisioningStateProvisioned:   {ProvisioningStateDestroying},
	ProvisioningStateFailed:        {ProvisioningStateInitializing, ProvisioningStateDestroying},
	ProvisioningStateDestroying:    {ProvisioningStateDestroyed, ProvisioningStateDestroyFailed},
	ProvisioningStateDestroyFailed: {ProvisioningStateDestroying},
}

// Provisioning represents a terraform-managed infrastructure instance.
//
// Each provisioning is associated with an environment (FK to environments.name)
// that provides credentials and variables, and a template that defines the
// terraform configuration. The template is snapshotted at creation time and
// stored as a tar.gz archive in TemplateSnapshot so that destroy always uses
// the same template version as the original apply.
//
// The Identifier is an 8-character random string (letter + alphanumeric) that is
// unconditionally auto-injected as a terraform variable. Every template must
// declare variable "identifier" { type = string } and use it for resource
// naming to ensure uniqueness.
type Provisioning struct {
	ID               uuid.UUID              `json:"id"`
	Name             string                 `json:"name"`
	Environment      string                 `json:"environment"`
	TemplateType     string                 `json:"template_type"`
	TemplateChecksum string                 `json:"template_checksum"`
	TemplateSnapshot []byte                 `json:"-"`
	State            ProvisioningState      `json:"state"`
	Identifier       string                 `json:"identifier"`
	Variables        map[string]interface{} `json:"variables"`
	Outputs          map[string]interface{} `json:"outputs"`
	PlanOutput       json.RawMessage        `json:"plan_output,omitempty"`
	Error            string                 `json:"error,omitempty"`
	Owner            string                 `json:"owner"`
	ClusterName      string                 `json:"cluster_name,omitempty"`
	Lifetime         time.Duration          `json:"lifetime"`
	CreatedAt        time.Time              `json:"created_at"`
	UpdatedAt        time.Time              `json:"updated_at"`
	ExpiresAt        *time.Time             `json:"expires_at,omitempty"`
	LastStep         string                 `json:"last_step"`
	WorkingDir       string                 `json:"-"`
}

// SetState transitions the provisioning to a new state. If the transition is
// not in expectedTransitions, a warning is logged but the transition is still
// performed. This is warning-only in the PoC.
func (p *Provisioning) SetState(newState ProvisioningState, l *logger.Logger) {
	if valid, ok := expectedTransitions[p.State]; ok {
		found := false
		for _, s := range valid {
			if s == newState {
				found = true
				break
			}
		}
		if !found {
			l.Warn("unexpected state transition",
				slog.String("from", string(p.State)),
				slog.String("to", string(newState)),
				slog.String("provisioning_id", p.ID.String()),
			)
		}
	}
	p.State = newState
}

// identifierLetters is the set of characters used for the first character
// of the identifier. Cloud providers (GCP, AWS, Azure) require resource
// names/tags to start with a lowercase letter.
const identifierLetters = "abcdefghijklmnopqrstuvwxyz"

// identifierCharset is the full set of characters used for subsequent
// characters of the identifier.
const identifierCharset = "abcdefghijklmnopqrstuvwxyz0123456789"

// IdentifierLength is the length of the generated identifier.
const IdentifierLength = 8

// GenerateIdentifier creates a random 8-character lowercase alphanumeric
// string suitable for use as a provisioning identifier. The first character
// is always a letter to satisfy cloud provider naming constraints (e.g.
// GCP tags must match [a-z][-a-z0-9]*[a-z0-9]). Uses crypto/rand with
// big.Int to avoid modulo bias.
func GenerateIdentifier() (string, error) {
	b := make([]byte, IdentifierLength)
	for i := range b {
		charset := identifierCharset
		if i == 0 {
			charset = identifierLetters
		}
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		b[i] = charset[n.Int64()]
	}
	return string(b), nil
}
