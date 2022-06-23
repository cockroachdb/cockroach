// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buf

import (
	"context"
	"time"

	"github.com/bufbuild/buf/private/buf/bufcli"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/alpha/registry/token/tokencreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/alpha/registry/token/tokendelete"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/alpha/registry/token/tokenget"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/alpha/registry/token/tokenlist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/commit/commitget"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/commit/commitlist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/organization/organizationcreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/organization/organizationdelete"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/organization/organizationget"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/plugin/plugincreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/plugin/plugindelete"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/plugin/pluginlist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/plugin/pluginversion/pluginversionlist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/repository/repositorycreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/repository/repositorydelete"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/repository/repositoryget"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/repository/repositorylist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/tag/tagcreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/tag/taglist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/template/templatecreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/template/templatedelete"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/template/templatelist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/template/templateversion/templateversioncreate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/beta/registry/template/templateversion/templateversionlist"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/breaking"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/build"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/config/configlsbreakingrules"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/config/configlslintrules"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/config/configmigratev1beta1"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/convert"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/export"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/generate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/lint"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/login"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/logout"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/lsfiles"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/mod/modclearcache"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/mod/modinit"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/mod/modprune"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/mod/modupdate"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/protoc"
	"github.com/bufbuild/buf/private/buf/cmd/buf/command/push"
	"github.com/bufbuild/buf/private/pkg/app/appcmd"
	"github.com/bufbuild/buf/private/pkg/app/appflag"
)

const (
	checkDeprecationMessage = `"buf check" sub-commands are now all implemented with the top-level "buf lint" and "buf breaking" commands.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	checkBreakingDeprecationMessage = `"buf check breaking" has been moved to "buf breaking", use "buf breaking" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	checkLintDeprecationMessage = `"buf check lint" has been moved to "buf lint", use "buf lint" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	checkLSBreakingCheckersDeprecationMessage = `"buf check ls-breaking-checkers" has been moved to "buf config ls-breaking-rules", use "buf config ls-breaking-rules" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	checkLSLintCheckersDeprecationMessage = `"buf check ls-lint-checkers" has been moved to "buf config ls-lint-rules", use "buf config ls-lint-rules" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	imageDeprecationMessage = `"buf image" sub-commands are now all implemented under the top-level "buf build" command, use "buf build" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`
	betaConfigDeprecationMessage = `"buf beta config" has been moved to "buf beta mod".
We recommend migrating, however this command continues to work.`
	betaConfigInitDeprecationMessage = `"buf beta config init" has been moved to "buf mod init".
We recommend migrating, however this command continues to work.`
	betaPushDeprecationMessage = `"buf beta push" has been moved to "buf push".
We recommend migrating, however this command continues to work.`
	betaModDeprecationMessage = `"buf beta mod ..." has been moved to "buf mod ...".
We recommend migrating, however this command continues to work.`
	betaModExportDeprecationMessage = `"buf beta mod export" has been moved to "buf export".
We recommend migrating, however this command continues to work.`
)

func Main(name string) {
	appcmd.Main(context.Background(), NewRootCommand(name))
}

// NewRootCommand returns a new root command.
//
// This is public for use in testing.
func NewRootCommand(name string) *appcmd.Command {
	builder := appflag.NewBuilder(
		name,
		appflag.BuilderWithTimeout(120*time.Second),
		appflag.BuilderWithTracing(),
	)
	globalFlags := bufcli.NewGlobalFlags()
	return &appcmd.Command{
		Use: name,
		SubCommands: []*appcmd.Command{
			build.NewCommand("build", builder, "", false),
			export.NewCommand("export", builder, "", false),
			{
				Use:        "image",
				Short:      "Work with Images and FileDescriptorSets.",
				Deprecated: imageDeprecationMessage,
				Hidden:     true,
				SubCommands: []*appcmd.Command{
					build.NewCommand(
						"build",
						builder,
						imageDeprecationMessage,
						true,
					),
				},
			},
			{
				Use:        "check",
				Short:      "Run linting or breaking change detection.",
				Deprecated: checkDeprecationMessage,
				Hidden:     true,
				SubCommands: []*appcmd.Command{
					lint.NewCommand("lint", builder, checkLintDeprecationMessage, true),
					breaking.NewCommand("breaking", builder, checkBreakingDeprecationMessage, true),
					configlslintrules.NewCommand("ls-lint-checkers", builder, checkLSLintCheckersDeprecationMessage, true),
					configlsbreakingrules.NewCommand("ls-breaking-checkers", builder, checkLSBreakingCheckersDeprecationMessage, true),
				},
			},
			lint.NewCommand("lint", builder, "", false),
			breaking.NewCommand("breaking", builder, "", false),
			generate.NewCommand("generate", builder),
			protoc.NewCommand("protoc", builder),
			lsfiles.NewCommand("ls-files", builder),
			{
				Use:   "mod",
				Short: "Configure and update buf modules.",
				SubCommands: []*appcmd.Command{
					modinit.NewCommand("init", builder, "", false),
					modprune.NewCommand("prune", builder),
					modupdate.NewCommand("update", builder, "", false),
					modclearcache.NewCommand("clear-cache", builder, "", false, "cc"),
				},
			},
			{
				Use:   "config",
				Short: "Interact with the configuration of Buf.",
				SubCommands: []*appcmd.Command{
					configlslintrules.NewCommand("ls-lint-rules", builder, "", false),
					configlsbreakingrules.NewCommand("ls-breaking-rules", builder, "", false),
					configmigratev1beta1.NewCommand("migrate-v1beta1", builder),
				},
			},
			login.NewCommand("login", builder),
			logout.NewCommand("logout", builder),
			{
				Use:   "beta",
				Short: "Beta commands. Unstable and will likely change.",
				SubCommands: []*appcmd.Command{
					{
						Use:        "config",
						Short:      "Interact with the configuration of Buf.",
						Deprecated: betaConfigDeprecationMessage,
						Hidden:     true,
						SubCommands: []*appcmd.Command{
							modinit.NewCommand("init", builder, betaConfigInitDeprecationMessage, true),
						},
					},
					{
						Use:        "image",
						Short:      "Work with Images and FileDescriptorSets.",
						Deprecated: imageDeprecationMessage,
						Hidden:     true,
						SubCommands: []*appcmd.Command{
							convert.NewCommand(
								"convert",
								builder,
								imageDeprecationMessage,
								true,
							),
						},
					},
					push.NewCommand("push", builder, betaPushDeprecationMessage, true),
					{
						Use:        "mod",
						Short:      "Configure and update buf modules.",
						Deprecated: betaModDeprecationMessage,
						Hidden:     true,
						SubCommands: []*appcmd.Command{
							modinit.NewCommand("init", builder, betaModDeprecationMessage, true),
							modupdate.NewCommand("update", builder, betaModDeprecationMessage, true),
							export.NewCommand("export", builder, betaModExportDeprecationMessage, true),
							modclearcache.NewCommand("clear-cache", builder, betaModDeprecationMessage, true, "cc"),
						},
					},
					{
						Use:   "registry",
						Short: "Interact with the Buf Schema Registry.",
						SubCommands: []*appcmd.Command{
							{
								Use:   "organization",
								Short: "Organization commands.",
								SubCommands: []*appcmd.Command{
									organizationcreate.NewCommand("create", builder),
									organizationget.NewCommand("get", builder),
									organizationdelete.NewCommand("delete", builder),
								},
							},
							{
								Use:   "repository",
								Short: "Repository commands.",
								SubCommands: []*appcmd.Command{
									repositorycreate.NewCommand("create", builder),
									repositoryget.NewCommand("get", builder),
									repositorylist.NewCommand("list", builder),
									repositorydelete.NewCommand("delete", builder),
								},
							},
							//{
							//	Use:   "branch",
							//	Short: "Repository branch commands.",
							//	SubCommands: []*appcmd.Command{
							//		branchcreate.NewCommand("create", builder),
							//		branchlist.NewCommand("list", builder),
							//	},
							//},
							{
								Use:   "tag",
								Short: "Repository tag commands.",
								SubCommands: []*appcmd.Command{
									tagcreate.NewCommand("create", builder),
									taglist.NewCommand("list", builder),
								},
							},
							{
								Use:   "commit",
								Short: "Repository commit commands.",
								SubCommands: []*appcmd.Command{
									commitget.NewCommand("get", builder),
									commitlist.NewCommand("list", builder),
								},
							},
							{
								Use:   "plugin",
								Short: "Plugin commands.",
								SubCommands: []*appcmd.Command{
									plugincreate.NewCommand("create", builder),
									pluginlist.NewCommand("list", builder),
									plugindelete.NewCommand("delete", builder),
									{
										Use:   "version",
										Short: "Plugin version commands.",
										SubCommands: []*appcmd.Command{
											pluginversionlist.NewCommand("list", builder),
										},
									},
								},
							},
							{
								Use:   "template",
								Short: "Template commands.",
								SubCommands: []*appcmd.Command{
									templatecreate.NewCommand("create", builder),
									templatelist.NewCommand("list", builder),
									templatedelete.NewCommand("delete", builder),
									{
										Use:   "version",
										Short: "Template version commands.",
										SubCommands: []*appcmd.Command{
											templateversioncreate.NewCommand("create", builder),
											templateversionlist.NewCommand("list", builder),
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Use:   "experimental",
				Short: "Experimental commands. Unstable and will likely change.",
				Deprecated: `use "beta" instead.
We recommend migrating, however this command continues to work.
See https://docs.buf.build/faq for more details.`,
				Hidden: true,
				SubCommands: []*appcmd.Command{
					{
						Use:        "image",
						Short:      "Work with Images and FileDescriptorSets.",
						Deprecated: imageDeprecationMessage,
						Hidden:     true,
						SubCommands: []*appcmd.Command{
							convert.NewCommand(
								"convert",
								builder,
								imageDeprecationMessage,
								true,
							),
						},
					},
				},
			},
			{
				Use:    "alpha",
				Short:  "Alpha commands. These are so early in development that they should not be used except in development.",
				Hidden: true,
				SubCommands: []*appcmd.Command{
					{
						Use:   "registry",
						Short: "Interact with the Buf Schema Registry.",
						SubCommands: []*appcmd.Command{
							{
								Use:   "token",
								Short: "Token commands.",
								SubCommands: []*appcmd.Command{
									tokencreate.NewCommand("create", builder),
									tokenget.NewCommand("get", builder),
									tokenlist.NewCommand("list", builder),
									tokendelete.NewCommand("delete", builder),
								},
							},
						},
					},
				},
			},
			push.NewCommand("push", builder, "", false),
		},
		BindPersistentFlags: appcmd.BindMultiple(builder.BindRoot, globalFlags.BindRoot),
		Version:             bufcli.Version,
	}
}
