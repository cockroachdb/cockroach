// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hooks

import (
	"context"
	"log/slog"

	provmodels "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/provisionings"
	envtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/environments/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/provisionings/templates"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/errors"
)

// IOrchestrator defines the interface for hook orchestration. This enables
// mocking in service tests.
type IOrchestrator interface {
	RunPostApply(
		ctx context.Context, l *logger.Logger,
		prov provmodels.Provisioning,
		workingDir string,
		resolvedEnv envtypes.ResolvedEnvironment,
	) error
	RunSingle(
		ctx context.Context, l *logger.Logger,
		prov provmodels.Provisioning,
		meta provmodels.TemplateMetadata,
		hookType string,
		resolvedEnv envtypes.ResolvedEnvironment,
	) error
}

// Orchestrator coordinates the execution of post-provisioning hooks. It
// resolves SSH configuration, evaluates jq expressions to find target
// machines, resolves environment variables, and dispatches to executors.
type Orchestrator struct {
	registry *Registry
}

// NewOrchestrator creates a new hook orchestrator with the given registry.
func NewOrchestrator(registry *Registry) *Orchestrator {
	return &Orchestrator{registry: registry}
}

// RunPostApply runs all hooks with the post_apply trigger in YAML list order.
// Reads the template metadata from the working directory. If no hooks with
// post_apply trigger exist, returns nil immediately. Optional hooks that fail
// produce a warning; non-optional hooks that fail return an error immediately.
func (o *Orchestrator) RunPostApply(
	ctx context.Context,
	l *logger.Logger,
	prov provmodels.Provisioning,
	workingDir string,
	resolvedEnv envtypes.ResolvedEnvironment,
) error {
	meta, err := templates.ParseMetadataFromDir(workingDir)
	if err != nil {
		l.Warn("skipping hooks: could not parse template metadata",
			slog.String("provisioning_id", prov.ID.String()),
			slog.Any("error", err),
		)
		return nil
	}

	return o.runHooksForTrigger(
		ctx, l, prov, meta, provmodels.TriggerPostApply, resolvedEnv,
	)
}

// RunSingle runs all hooks of the given type. Used by manual trigger flows
// (e.g., ssh-keys-setup in Commit 12).
func (o *Orchestrator) RunSingle(
	ctx context.Context,
	l *logger.Logger,
	prov provmodels.Provisioning,
	meta provmodels.TemplateMetadata,
	hookType string,
	resolvedEnv envtypes.ResolvedEnvironment,
) error {
	if meta.SSH == nil {
		return errors.Newf("template %q has no SSH configuration", meta.Name)
	}

	for i := range meta.Hooks {
		hook := &meta.Hooks[i]
		if hook.Type != hookType {
			continue
		}
		if err := o.executeHook(
			ctx, l, prov, meta, hook, resolvedEnv,
		); err != nil {
			return err
		}
	}
	return nil
}

// runHooksForTrigger iterates hooks in list order and executes those matching
// the given trigger.
func (o *Orchestrator) runHooksForTrigger(
	ctx context.Context,
	l *logger.Logger,
	prov provmodels.Provisioning,
	meta provmodels.TemplateMetadata,
	trigger provmodels.HookTrigger,
	resolvedEnv envtypes.ResolvedEnvironment,
) error {
	if meta.SSH == nil && hasSSHHooks(meta.Hooks, trigger) {
		return errors.Newf(
			"template %q has SSH hooks but no SSH configuration", meta.Name,
		)
	}

	hasAny := false
	for i := range meta.Hooks {
		if meta.Hooks[i].HasTrigger(trigger) {
			hasAny = true
			break
		}
	}
	if !hasAny {
		l.Debug("no hooks with trigger, skipping",
			slog.String("trigger", string(trigger)),
			slog.String("provisioning_id", prov.ID.String()),
		)
		return nil
	}

	l.Info("running post-apply hooks",
		slog.String("provisioning_id", prov.ID.String()),
		slog.Int("hook_count", countHooksForTrigger(meta.Hooks, trigger)),
	)

	for i := range meta.Hooks {
		hook := &meta.Hooks[i]
		if !hook.HasTrigger(trigger) {
			continue
		}

		if err := o.executeHook(
			ctx, l, prov, meta, hook, resolvedEnv,
		); err != nil {
			if hook.Optional {
				l.Warn("optional hook failed, continuing",
					slog.String("hook", hook.Name),
					slog.String("provisioning_id", prov.ID.String()),
					slog.Any("error", err),
				)
				continue
			}
			return errors.Wrapf(err, "hook %q failed", hook.Name)
		}
	}
	return nil
}

// executeHook resolves all context for a single hook and dispatches to the
// appropriate executor.
func (o *Orchestrator) executeHook(
	ctx context.Context,
	l *logger.Logger,
	prov provmodels.Provisioning,
	meta provmodels.TemplateMetadata,
	hook *provmodels.HookDeclaration,
	resolvedEnv envtypes.ResolvedEnvironment,
) error {
	l.Info("executing hook",
		slog.String("hook", hook.Name),
		slog.String("type", hook.Type),
		slog.String("provisioning_id", prov.ID.String()),
	)

	executor, err := o.registry.Get(hook.Type)
	if err != nil {
		return err
	}

	// Validate that SSH-dependent hook types declare ssh: true.
	if hook.Type == "run-command" && !hook.SSH {
		return errors.Newf(
			"hook %q: run-command type requires ssh: true", hook.Name,
		)
	}

	hctx := HookContext{
		Provisioning: prov,
		Declaration:  *hook,
	}

	// Resolve SSH configuration if needed.
	if hook.SSH && meta.SSH != nil {
		machines, user := resolveSSHForHook(*meta.SSH, *hook)

		// Resolve SSH private key from environment.
		privateKey, err := resolveEnvVar(
			resolvedEnv, meta.SSH.PrivateKeyVar,
		)
		if err != nil {
			return errors.Wrapf(
				err, "resolve SSH private key %q", meta.SSH.PrivateKeyVar,
			)
		}
		hctx.SSHPrivateKey = []byte(privateKey)
		hctx.SSHUser = user

		// Evaluate jq expression against provisioning outputs.
		ips, err := evaluateJQ(machines, prov.Outputs)
		if err != nil {
			return errors.Wrapf(
				err, "evaluate machines expression for hook %q", hook.Name,
			)
		}
		hctx.MachineIPs = ips

		l.Info("hook targets resolved",
			slog.String("hook", hook.Name),
			slog.Int("machines", len(ips)),
			slog.String("user", user),
		)
	}

	// Resolve hook-level env vars.
	if len(hook.Env) > 0 {
		hookEnv := make(map[string]string, len(hook.Env))
		for hookVar, envKey := range hook.Env {
			val, err := resolveEnvVar(resolvedEnv, envKey)
			if err != nil {
				return errors.Wrapf(
					err, "resolve env var %q for hook %q", envKey, hook.Name,
				)
			}
			hookEnv[hookVar] = val
		}
		hctx.HookEnv = hookEnv
	}

	if err := executor.Execute(ctx, l, hctx); err != nil {
		return err
	}

	l.Info("hook completed successfully",
		slog.String("hook", hook.Name),
		slog.String("provisioning_id", prov.ID.String()),
	)
	return nil
}

// resolveSSHForHook returns the machines expression and SSH user for a hook,
// applying hook-level overrides on top of the template-level SSH config.
func resolveSSHForHook(
	sshCfg provmodels.SSHConfig, hook provmodels.HookDeclaration,
) (machines string, user string) {
	machines = sshCfg.Machines
	if hook.Machines != "" {
		machines = hook.Machines
	}
	user = sshCfg.User
	if hook.User != "" {
		user = hook.User
	}
	return machines, user
}

// resolveEnvVar finds a variable by key in the resolved environment.
func resolveEnvVar(env envtypes.ResolvedEnvironment, key string) (string, error) {
	for _, v := range env.Variables {
		if v.Key == key {
			return v.Value, nil
		}
	}
	return "", errors.Newf(
		"environment variable %q not found in resolved environment %q",
		key, env.Name,
	)
}

// hasSSHHooks returns true if any hook matching the trigger requires SSH.
func hasSSHHooks(hooks []provmodels.HookDeclaration, trigger provmodels.HookTrigger) bool {
	for i := range hooks {
		if hooks[i].HasTrigger(trigger) && hooks[i].SSH {
			return true
		}
	}
	return false
}

// countHooksForTrigger returns the number of hooks matching the trigger.
func countHooksForTrigger(hooks []provmodels.HookDeclaration, trigger provmodels.HookTrigger) int {
	count := 0
	for i := range hooks {
		if hooks[i].HasTrigger(trigger) {
			count++
		}
	}
	return count
}
