// Copyright 2016 The go-github AUTHORS. All rights reserved.
//
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// These event types are shared between the Events API and used as Webhook payloads.

package github

import "encoding/json"

// RequestedAction is included in a CheckRunEvent when a user has invoked an action,
// i.e. when the CheckRunEvent's Action field is "requested_action".
type RequestedAction struct {
	Identifier string `json:"identifier"` // The integrator reference of the action requested by the user.
}

// BranchProtectionRuleEvent triggered when a check suite is "created", "edited", or "deleted".
// The Webhook event name is "branch_protection_rule".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#branch_protection_rule
type BranchProtectionRuleEvent struct {
	Action       *string               `json:"action,omitempty"`
	Rule         *BranchProtectionRule `json:"rule,omitempty"`
	Changes      *ProtectionChanges    `json:"changes,omitempty"`
	Repo         *Repository           `json:"repository,omitempty"`
	Org          *Organization         `json:"organization,omitempty"`
	Sender       *User                 `json:"sender,omitempty"`
	Installation *Installation         `json:"installation,omitempty"`
}

// CheckRunEvent is triggered when a check run is "created", "completed", or "rerequested".
// The Webhook event name is "check_run".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#check_run
type CheckRunEvent struct {
	CheckRun *CheckRun `json:"check_run,omitempty"`
	// The action performed. Possible values are: "created", "completed", "rerequested" or "requested_action".
	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`

	// The action requested by the user. Populated when the Action is "requested_action".
	RequestedAction *RequestedAction `json:"requested_action,omitempty"` //
}

// CheckSuiteEvent is triggered when a check suite is "completed", "requested", or "rerequested".
// The Webhook event name is "check_suite".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#check_suite
type CheckSuiteEvent struct {
	CheckSuite *CheckSuite `json:"check_suite,omitempty"`
	// The action performed. Possible values are: "completed", "requested" or "rerequested".
	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// CommitCommentEvent is triggered when a commit comment is created.
// The Webhook event name is "commit_comment".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#commit_comment
type CommitCommentEvent struct {
	Comment *RepositoryComment `json:"comment,omitempty"`

	// The following fields are only populated by Webhook events.
	Action       *string       `json:"action,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// ContentReferenceEvent is triggered when the body or comment of an issue or
// pull request includes a URL that matches a configured content reference
// domain.
// The Webhook event name is "content_reference".
//
// GitHub API docs: https://developer.github.com/webhooks/event-payloads/#content_reference
type ContentReferenceEvent struct {
	Action           *string           `json:"action,omitempty"`
	ContentReference *ContentReference `json:"content_reference,omitempty"`
	Repo             *Repository       `json:"repository,omitempty"`
	Sender           *User             `json:"sender,omitempty"`
	Installation     *Installation     `json:"installation,omitempty"`
}

// CreateEvent represents a created repository, branch, or tag.
// The Webhook event name is "create".
//
// Note: webhooks will not receive this event for created repositories.
// Additionally, webhooks will not receive this event for tags if more
// than three tags are pushed at once.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/github-event-types#createevent
type CreateEvent struct {
	Ref *string `json:"ref,omitempty"`
	// RefType is the object that was created. Possible values are: "repository", "branch", "tag".
	RefType      *string `json:"ref_type,omitempty"`
	MasterBranch *string `json:"master_branch,omitempty"`
	Description  *string `json:"description,omitempty"`

	// The following fields are only populated by Webhook events.
	PusherType   *string       `json:"pusher_type,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// DeleteEvent represents a deleted branch or tag.
// The Webhook event name is "delete".
//
// Note: webhooks will not receive this event for tags if more than three tags
// are deleted at once.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/github-event-types#deleteevent
type DeleteEvent struct {
	Ref *string `json:"ref,omitempty"`
	// RefType is the object that was deleted. Possible values are: "branch", "tag".
	RefType *string `json:"ref_type,omitempty"`

	// The following fields are only populated by Webhook events.
	PusherType   *string       `json:"pusher_type,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// DeployKeyEvent is triggered when a deploy key is added or removed from a repository.
// The Webhook event name is "deploy_key".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#deploy_key
type DeployKeyEvent struct {
	// Action is the action that was performed. Possible values are:
	// "created" or "deleted".
	Action *string `json:"action,omitempty"`

	// The deploy key resource.
	Key *Key `json:"key,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// DeploymentEvent represents a deployment.
// The Webhook event name is "deployment".
//
// Events of this type are not visible in timelines, they are only used to trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#deployment
type DeploymentEvent struct {
	Deployment *Deployment `json:"deployment,omitempty"`
	Repo       *Repository `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// DeploymentStatusEvent represents a deployment status.
// The Webhook event name is "deployment_status".
//
// Events of this type are not visible in timelines, they are only used to trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#deployment_status
type DeploymentStatusEvent struct {
	Deployment       *Deployment       `json:"deployment,omitempty"`
	DeploymentStatus *DeploymentStatus `json:"deployment_status,omitempty"`
	Repo             *Repository       `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// DiscussionEvent represents a webhook event for a discussion.
// The Webhook event name is "discussion".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#discussion
type DiscussionEvent struct {
	// Action is the action that was performed. Possible values are:
	// created, edited, deleted, pinned, unpinned, locked, unlocked,
	// transferred, category_changed, answered, or unanswered.
	Action       *string       `json:"action,omitempty"`
	Discussion   *Discussion   `json:"discussion,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// Discussion represents a discussion in a GitHub DiscussionEvent.
type Discussion struct {
	RepositoryURL      *string             `json:"repository_url,omitempty"`
	DiscussionCategory *DiscussionCategory `json:"category,omitempty"`
	AnswerHTMLURL      *string             `json:"answer_html_url,omitempty"`
	AnswerChosenAt     *Timestamp          `json:"answer_chosen_at,omitempty"`
	AnswerChosenBy     *string             `json:"answer_chosen_by,omitempty"`
	HTMLURL            *string             `json:"html_url,omitempty"`
	ID                 *int64              `json:"id,omitempty"`
	NodeID             *string             `json:"node_id,omitempty"`
	Number             *int                `json:"number,omitempty"`
	Title              *string             `json:"title,omitempty"`
	User               *User               `json:"user,omitempty"`
	State              *string             `json:"state,omitempty"`
	Locked             *bool               `json:"locked,omitempty"`
	Comments           *int                `json:"comments,omitempty"`
	CreatedAt          *Timestamp          `json:"created_at,omitempty"`
	UpdatedAt          *Timestamp          `json:"updated_at,omitempty"`
	AuthorAssociation  *string             `json:"author_association,omitempty"`
	ActiveLockReason   *string             `json:"active_lock_reason,omitempty"`
	Body               *string             `json:"body,omitempty"`
}

// DiscussionCategory represents a discussion category in a GitHub DiscussionEvent.
type DiscussionCategory struct {
	ID           *int64     `json:"id,omitempty"`
	NodeID       *string    `json:"node_id,omitempty"`
	RepositoryID *int64     `json:"repository_id,omitempty"`
	Emoji        *string    `json:"emoji,omitempty"`
	Name         *string    `json:"name,omitempty"`
	Description  *string    `json:"description,omitempty"`
	CreatedAt    *Timestamp `json:"created_at,omitempty"`
	UpdatedAt    *Timestamp `json:"updated_at,omitempty"`
	Slug         *string    `json:"slug,omitempty"`
	IsAnswerable *bool      `json:"is_answerable,omitempty"`
}

// ForkEvent is triggered when a user forks a repository.
// The Webhook event name is "fork".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#fork
type ForkEvent struct {
	// Forkee is the created repository.
	Forkee *Repository `json:"forkee,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// GitHubAppAuthorizationEvent is triggered when a user's authorization for a
// GitHub Application is revoked.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#github_app_authorization
type GitHubAppAuthorizationEvent struct {
	// The action performed. Possible value is: "revoked".
	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// Page represents a single Wiki page.
type Page struct {
	PageName *string `json:"page_name,omitempty"`
	Title    *string `json:"title,omitempty"`
	Summary  *string `json:"summary,omitempty"`
	Action   *string `json:"action,omitempty"`
	SHA      *string `json:"sha,omitempty"`
	HTMLURL  *string `json:"html_url,omitempty"`
}

// GollumEvent is triggered when a Wiki page is created or updated.
// The Webhook event name is "gollum".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#gollum
type GollumEvent struct {
	Pages []*Page `json:"pages,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// EditChange represents the changes when an issue, pull request, comment,
// or repository has been edited.
type EditChange struct {
	Title *EditTitle `json:"title,omitempty"`
	Body  *EditBody  `json:"body,omitempty"`
	Base  *EditBase  `json:"base,omitempty"`
	Repo  *EditRepo  `json:"repository,omitempty"`
}

// EditTitle represents a pull-request title change.
type EditTitle struct {
	From *string `json:"from,omitempty"`
}

// EditBody represents a change of pull-request body.
type EditBody struct {
	From *string `json:"from,omitempty"`
}

// EditBase represents the change of a pull-request base branch.
type EditBase struct {
	Ref *EditRef `json:"ref,omitempty"`
	SHA *EditSHA `json:"sha,omitempty"`
}

// EditRef represents a ref change of a pull-request.
type EditRef struct {
	From *string `json:"from,omitempty"`
}

// EditRepo represents a change of repository name.
type EditRepo struct {
	Name *RepoName `json:"name,omitempty"`
}

// RepoName represents a change of repository name.
type RepoName struct {
	From *string `json:"from,omitempty"`
}

// EditSHA represents a sha change of a pull-request.
type EditSHA struct {
	From *string `json:"from,omitempty"`
}

// ProjectChange represents the changes when a project has been edited.
type ProjectChange struct {
	Name *ProjectName `json:"name,omitempty"`
	Body *ProjectBody `json:"body,omitempty"`
}

// ProjectName represents a project name change.
type ProjectName struct {
	From *string `json:"from,omitempty"`
}

// ProjectBody represents a project body change.
type ProjectBody struct {
	From *string `json:"from,omitempty"`
}

// ProjectCardChange represents the changes when a project card has been edited.
type ProjectCardChange struct {
	Note *ProjectCardNote `json:"note,omitempty"`
}

// ProjectCardNote represents a change of a note of a project card.
type ProjectCardNote struct {
	From *string `json:"from,omitempty"`
}

// ProjectColumnChange represents the changes when a project column has been edited.
type ProjectColumnChange struct {
	Name *ProjectColumnName `json:"name,omitempty"`
}

// ProjectColumnName represents a project column name change.
type ProjectColumnName struct {
	From *string `json:"from,omitempty"`
}

// TeamChange represents the changes when a team has been edited.
type TeamChange struct {
	Description *TeamDescription `json:"description,omitempty"`
	Name        *TeamName        `json:"name,omitempty"`
	Privacy     *TeamPrivacy     `json:"privacy,omitempty"`
	Repository  *TeamRepository  `json:"repository,omitempty"`
}

// TeamDescription represents a team description change.
type TeamDescription struct {
	From *string `json:"from,omitempty"`
}

// TeamName represents a team name change.
type TeamName struct {
	From *string `json:"from,omitempty"`
}

// TeamPrivacy represents a team privacy change.
type TeamPrivacy struct {
	From *string `json:"from,omitempty"`
}

// TeamRepository represents a team repository permission change.
type TeamRepository struct {
	Permissions *TeamPermissions `json:"permissions,omitempty"`
}

// TeamPermissions represents a team permission change.
type TeamPermissions struct {
	From *TeamPermissionsFrom `json:"from,omitempty"`
}

// TeamPermissionsFrom represents a team permission change.
type TeamPermissionsFrom struct {
	Admin *bool `json:"admin,omitempty"`
	Pull  *bool `json:"pull,omitempty"`
	Push  *bool `json:"push,omitempty"`
}

// InstallationEvent is triggered when a GitHub App has been installed, uninstalled, suspend, unsuspended
// or new permissions have been accepted.
// The Webhook event name is "installation".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#installation
type InstallationEvent struct {
	// The action that was performed. Can be either "created", "deleted", "suspend", "unsuspend" or "new_permissions_accepted".
	Action       *string       `json:"action,omitempty"`
	Repositories []*Repository `json:"repositories,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
	// TODO key "requester" is not covered
}

// InstallationRepositoriesEvent is triggered when a repository is added or
// removed from an installation. The Webhook event name is "installation_repositories".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#installation_repositories
type InstallationRepositoriesEvent struct {
	// The action that was performed. Can be either "added" or "removed".
	Action              *string       `json:"action,omitempty"`
	RepositoriesAdded   []*Repository `json:"repositories_added,omitempty"`
	RepositoriesRemoved []*Repository `json:"repositories_removed,omitempty"`
	RepositorySelection *string       `json:"repository_selection,omitempty"`
	Sender              *User         `json:"sender,omitempty"`
	Installation        *Installation `json:"installation,omitempty"`
}

// IssueCommentEvent is triggered when an issue comment is created on an issue
// or pull request.
// The Webhook event name is "issue_comment".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#issue_comment
type IssueCommentEvent struct {
	// Action is the action that was performed on the comment.
	// Possible values are: "created", "edited", "deleted".
	Action  *string       `json:"action,omitempty"`
	Issue   *Issue        `json:"issue,omitempty"`
	Comment *IssueComment `json:"comment,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`

	// The following field is only present when the webhook is triggered on
	// a repository belonging to an organization.
	Organization *Organization `json:"organization,omitempty"`
}

// IssuesEvent is triggered when an issue is opened, edited, deleted, transferred,
// pinned, unpinned, closed, reopened, assigned, unassigned, labeled, unlabeled,
// locked, unlocked, milestoned, or demilestoned.
// The Webhook event name is "issues".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#issues
type IssuesEvent struct {
	// Action is the action that was performed. Possible values are: "opened",
	// "edited", "deleted", "transferred", "pinned", "unpinned", "closed", "reopened",
	// "assigned", "unassigned", "labeled", "unlabeled", "locked", "unlocked",
	// "milestoned", or "demilestoned".
	Action   *string `json:"action,omitempty"`
	Issue    *Issue  `json:"issue,omitempty"`
	Assignee *User   `json:"assignee,omitempty"`
	Label    *Label  `json:"label,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// LabelEvent is triggered when a repository's label is created, edited, or deleted.
// The Webhook event name is "label"
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#label
type LabelEvent struct {
	// Action is the action that was performed. Possible values are:
	// "created", "edited", "deleted"
	Action *string `json:"action,omitempty"`
	Label  *Label  `json:"label,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// MarketplacePurchaseEvent is triggered when a user purchases, cancels, or changes
// their GitHub Marketplace plan.
// Webhook event name "marketplace_purchase".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#marketplace_purchase
type MarketplacePurchaseEvent struct {
	// Action is the action that was performed. Possible values are:
	// "purchased", "cancelled", "pending_change", "pending_change_cancelled", "changed".
	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.
	EffectiveDate               *Timestamp           `json:"effective_date,omitempty"`
	MarketplacePurchase         *MarketplacePurchase `json:"marketplace_purchase,omitempty"`
	PreviousMarketplacePurchase *MarketplacePurchase `json:"previous_marketplace_purchase,omitempty"`
	Sender                      *User                `json:"sender,omitempty"`
	Installation                *Installation        `json:"installation,omitempty"`
}

// MemberEvent is triggered when a user is added as a collaborator to a repository.
// The Webhook event name is "member".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#member
type MemberEvent struct {
	// Action is the action that was performed. Possible value is: "added".
	Action *string `json:"action,omitempty"`
	Member *User   `json:"member,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// MembershipEvent is triggered when a user is added or removed from a team.
// The Webhook event name is "membership".
//
// Events of this type are not visible in timelines, they are only used to
// trigger organization webhooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#membership
type MembershipEvent struct {
	// Action is the action that was performed. Possible values are: "added", "removed".
	Action *string `json:"action,omitempty"`
	// Scope is the scope of the membership. Possible value is: "team".
	Scope  *string `json:"scope,omitempty"`
	Member *User   `json:"member,omitempty"`
	Team   *Team   `json:"team,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// MetaEvent is triggered when the webhook that this event is configured on is deleted.
// This event will only listen for changes to the particular hook the event is installed on.
// Therefore, it must be selected for each hook that you'd like to receive meta events for.
// The Webhook event name is "meta".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#meta
type MetaEvent struct {
	// Action is the action that was performed. Possible value is: "deleted".
	Action *string `json:"action,omitempty"`
	// The ID of the modified webhook.
	HookID *int64 `json:"hook_id,omitempty"`
	// The modified webhook.
	// This will contain different keys based on the type of webhook it is: repository,
	// organization, business, app, or GitHub Marketplace.
	Hook *Hook `json:"hook,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// MilestoneEvent is triggered when a milestone is created, closed, opened, edited, or deleted.
// The Webhook event name is "milestone".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#milestone
type MilestoneEvent struct {
	// Action is the action that was performed. Possible values are:
	// "created", "closed", "opened", "edited", "deleted"
	Action    *string    `json:"action,omitempty"`
	Milestone *Milestone `json:"milestone,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// OrganizationEvent is triggered when an organization is deleted and renamed, and when a user is added,
// removed, or invited to an organization.
// Events of this type are not visible in timelines. These events are only used to trigger organization hooks.
// Webhook event name is "organization".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#organization
type OrganizationEvent struct {
	// Action is the action that was performed.
	// Possible values are: "deleted", "renamed", "member_added", "member_removed", or "member_invited".
	Action *string `json:"action,omitempty"`

	// Invitation is the invitation for the user or email if the action is "member_invited".
	Invitation *Invitation `json:"invitation,omitempty"`

	// Membership is the membership between the user and the organization.
	// Not present when the action is "member_invited".
	Membership *Membership `json:"membership,omitempty"`

	Organization *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// OrgBlockEvent is triggered when an organization blocks or unblocks a user.
// The Webhook event name is "org_block".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#org_block
type OrgBlockEvent struct {
	// Action is the action that was performed.
	// Can be "blocked" or "unblocked".
	Action       *string       `json:"action,omitempty"`
	BlockedUser  *User         `json:"blocked_user,omitempty"`
	Organization *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// PackageEvent represents activity related to GitHub Packages.
// The Webhook event name is "package".
//
// This event is triggered when a GitHub Package is published or updated.
//
// GitHub API docs: https://developer.github.com/webhooks/event-payloads/#package
type PackageEvent struct {
	// Action is the action that was performed.
	// Can be "published" or "updated".
	Action  *string       `json:"action,omitempty"`
	Package *Package      `json:"package,omitempty"`
	Repo    *Repository   `json:"repository,omitempty"`
	Org     *Organization `json:"organization,omitempty"`
	Sender  *User         `json:"sender,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// PageBuildEvent represents an attempted build of a GitHub Pages site, whether
// successful or not.
// The Webhook event name is "page_build".
//
// This event is triggered on push to a GitHub Pages enabled branch (gh-pages
// for project pages, master for user and organization pages).
//
// Events of this type are not visible in timelines, they are only used to trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#page_build
type PageBuildEvent struct {
	Build *PagesBuild `json:"build,omitempty"`

	// The following fields are only populated by Webhook events.
	ID           *int64        `json:"id,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// PingEvent is triggered when a Webhook is added to GitHub.
//
// GitHub API docs: https://developer.github.com/webhooks/#ping-event
type PingEvent struct {
	// Random string of GitHub zen.
	Zen *string `json:"zen,omitempty"`
	// The ID of the webhook that triggered the ping.
	HookID *int64 `json:"hook_id,omitempty"`
	// The webhook configuration.
	Hook         *Hook         `json:"hook,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// ProjectEvent is triggered when project is created, modified or deleted.
// The webhook event name is "project".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#project
type ProjectEvent struct {
	Action  *string        `json:"action,omitempty"`
	Changes *ProjectChange `json:"changes,omitempty"`
	Project *Project       `json:"project,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// ProjectCardEvent is triggered when a project card is created, updated, moved, converted to an issue, or deleted.
// The webhook event name is "project_card".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#project_card
type ProjectCardEvent struct {
	Action      *string            `json:"action,omitempty"`
	Changes     *ProjectCardChange `json:"changes,omitempty"`
	AfterID     *int64             `json:"after_id,omitempty"`
	ProjectCard *ProjectCard       `json:"project_card,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// ProjectColumnEvent is triggered when a project column is created, updated, moved, or deleted.
// The webhook event name is "project_column".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#project_column
type ProjectColumnEvent struct {
	Action        *string              `json:"action,omitempty"`
	Changes       *ProjectColumnChange `json:"changes,omitempty"`
	AfterID       *int64               `json:"after_id,omitempty"`
	ProjectColumn *ProjectColumn       `json:"project_column,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// PublicEvent is triggered when a private repository is open sourced.
// According to GitHub: "Without a doubt: the best GitHub event."
// The Webhook event name is "public".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#public
type PublicEvent struct {
	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// PullRequestEvent is triggered when a pull request is assigned, unassigned, labeled,
// unlabeled, opened, edited, closed, reopened, synchronize, ready_for_review,
// locked, unlocked, a pull request review is requested, or a review request is removed.
// The Webhook event name is "pull_request".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/github-event-types#pullrequestevent
type PullRequestEvent struct {
	// Action is the action that was performed. Possible values are:
	// "assigned", "unassigned", "review_requested", "review_request_removed", "labeled", "unlabeled",
	// "opened", "edited", "closed", "ready_for_review", "locked", "unlocked", or "reopened".
	// If the action is "closed" and the "merged" key is "false", the pull request was closed with unmerged commits.
	// If the action is "closed" and the "merged" key is "true", the pull request was merged.
	// While webhooks are also triggered when a pull request is synchronized, Events API timelines
	// don't include pull request events with the "synchronize" action.
	Action      *string      `json:"action,omitempty"`
	Assignee    *User        `json:"assignee,omitempty"`
	Number      *int         `json:"number,omitempty"`
	PullRequest *PullRequest `json:"pull_request,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes *EditChange `json:"changes,omitempty"`
	// RequestedReviewer is populated in "review_requested", "review_request_removed" event deliveries.
	// A request affecting multiple reviewers at once is split into multiple
	// such event deliveries, each with a single, different RequestedReviewer.
	RequestedReviewer *User `json:"requested_reviewer,omitempty"`
	// In the event that a team is requested instead of a user, "requested_team" gets sent in place of
	// "requested_user" with the same delivery behavior.
	RequestedTeam *Team         `json:"requested_team,omitempty"`
	Repo          *Repository   `json:"repository,omitempty"`
	Sender        *User         `json:"sender,omitempty"`
	Installation  *Installation `json:"installation,omitempty"`
	Label         *Label        `json:"label,omitempty"` // Populated in "labeled" event deliveries.

	// The following field is only present when the webhook is triggered on
	// a repository belonging to an organization.
	Organization *Organization `json:"organization,omitempty"`

	// The following fields are only populated when the Action is "synchronize".
	Before *string `json:"before,omitempty"`
	After  *string `json:"after,omitempty"`
}

// PullRequestReviewEvent is triggered when a review is submitted on a pull
// request.
// The Webhook event name is "pull_request_review".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#pull_request_review
type PullRequestReviewEvent struct {
	// Action is always "submitted".
	Action      *string            `json:"action,omitempty"`
	Review      *PullRequestReview `json:"review,omitempty"`
	PullRequest *PullRequest       `json:"pull_request,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`

	// The following field is only present when the webhook is triggered on
	// a repository belonging to an organization.
	Organization *Organization `json:"organization,omitempty"`
}

// PullRequestReviewCommentEvent is triggered when a comment is created on a
// portion of the unified diff of a pull request.
// The Webhook event name is "pull_request_review_comment".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#pull_request_review_comment
type PullRequestReviewCommentEvent struct {
	// Action is the action that was performed on the comment.
	// Possible values are: "created", "edited", "deleted".
	Action      *string             `json:"action,omitempty"`
	PullRequest *PullRequest        `json:"pull_request,omitempty"`
	Comment     *PullRequestComment `json:"comment,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// PullRequestTargetEvent is triggered when a pull request is assigned, unassigned, labeled,
// unlabeled, opened, edited, closed, reopened, synchronize, ready_for_review,
// locked, unlocked, a pull request review is requested, or a review request is removed.
// The Webhook event name is "pull_request_target".
//
// GitHub API docs: https://docs.github.com/en/actions/reference/events-that-trigger-workflows#pull_request_target
type PullRequestTargetEvent struct {
	// Action is the action that was performed. Possible values are:
	// "assigned", "unassigned", "labeled", "unlabeled", "opened", "edited", "closed", "reopened",
	// "ready_for_review", "locked", "unlocked", "review_requested" or "review_request_removed".
	// If the action is "closed" and the "merged" key is "false", the pull request was closed with unmerged commits.
	// If the action is "closed" and the "merged" key is "true", the pull request was merged.
	// While webhooks are also triggered when a pull request is synchronized, Events API timelines
	// don't include pull request events with the "synchronize" action.
	Action      *string      `json:"action,omitempty"`
	Assignee    *User        `json:"assignee,omitempty"`
	Number      *int         `json:"number,omitempty"`
	PullRequest *PullRequest `json:"pull_request,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes *EditChange `json:"changes,omitempty"`
	// RequestedReviewer is populated in "review_requested", "review_request_removed" event deliveries.
	// A request affecting multiple reviewers at once is split into multiple
	// such event deliveries, each with a single, different RequestedReviewer.
	RequestedReviewer *User `json:"requested_reviewer,omitempty"`
	// In the event that a team is requested instead of a user, "requested_team" gets sent in place of
	// "requested_user" with the same delivery behavior.
	RequestedTeam *Team         `json:"requested_team,omitempty"`
	Repo          *Repository   `json:"repository,omitempty"`
	Sender        *User         `json:"sender,omitempty"`
	Installation  *Installation `json:"installation,omitempty"`
	Label         *Label        `json:"label,omitempty"` // Populated in "labeled" event deliveries.

	// The following field is only present when the webhook is triggered on
	// a repository belonging to an organization.
	Organization *Organization `json:"organization,omitempty"`

	// The following fields are only populated when the Action is "synchronize".
	Before *string `json:"before,omitempty"`
	After  *string `json:"after,omitempty"`
}

// PushEvent represents a git push to a GitHub repository.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#push
type PushEvent struct {
	PushID       *int64        `json:"push_id,omitempty"`
	Head         *string       `json:"head,omitempty"`
	Ref          *string       `json:"ref,omitempty"`
	Size         *int          `json:"size,omitempty"`
	Commits      []*HeadCommit `json:"commits,omitempty"`
	Before       *string       `json:"before,omitempty"`
	DistinctSize *int          `json:"distinct_size,omitempty"`

	// The following fields are only populated by Webhook events.
	After        *string              `json:"after,omitempty"`
	Created      *bool                `json:"created,omitempty"`
	Deleted      *bool                `json:"deleted,omitempty"`
	Forced       *bool                `json:"forced,omitempty"`
	BaseRef      *string              `json:"base_ref,omitempty"`
	Compare      *string              `json:"compare,omitempty"`
	Repo         *PushEventRepository `json:"repository,omitempty"`
	HeadCommit   *HeadCommit          `json:"head_commit,omitempty"`
	Pusher       *User                `json:"pusher,omitempty"`
	Sender       *User                `json:"sender,omitempty"`
	Installation *Installation        `json:"installation,omitempty"`

	// The following field is only present when the webhook is triggered on
	// a repository belonging to an organization.
	Organization *Organization `json:"organization,omitempty"`
}

func (p PushEvent) String() string {
	return Stringify(p)
}

// HeadCommit represents a git commit in a GitHub PushEvent.
type HeadCommit struct {
	Message  *string       `json:"message,omitempty"`
	Author   *CommitAuthor `json:"author,omitempty"`
	URL      *string       `json:"url,omitempty"`
	Distinct *bool         `json:"distinct,omitempty"`

	// The following fields are only populated by Events API.
	SHA *string `json:"sha,omitempty"`

	// The following fields are only populated by Webhook events.
	ID        *string       `json:"id,omitempty"`
	TreeID    *string       `json:"tree_id,omitempty"`
	Timestamp *Timestamp    `json:"timestamp,omitempty"`
	Committer *CommitAuthor `json:"committer,omitempty"`
	Added     []string      `json:"added,omitempty"`
	Removed   []string      `json:"removed,omitempty"`
	Modified  []string      `json:"modified,omitempty"`
}

func (p HeadCommit) String() string {
	return Stringify(p)
}

// PushEventRepository represents the repo object in a PushEvent payload.
type PushEventRepository struct {
	ID              *int64     `json:"id,omitempty"`
	NodeID          *string    `json:"node_id,omitempty"`
	Name            *string    `json:"name,omitempty"`
	FullName        *string    `json:"full_name,omitempty"`
	Owner           *User      `json:"owner,omitempty"`
	Private         *bool      `json:"private,omitempty"`
	Description     *string    `json:"description,omitempty"`
	Fork            *bool      `json:"fork,omitempty"`
	CreatedAt       *Timestamp `json:"created_at,omitempty"`
	PushedAt        *Timestamp `json:"pushed_at,omitempty"`
	UpdatedAt       *Timestamp `json:"updated_at,omitempty"`
	Homepage        *string    `json:"homepage,omitempty"`
	PullsURL        *string    `json:"pulls_url,omitempty"`
	Size            *int       `json:"size,omitempty"`
	StargazersCount *int       `json:"stargazers_count,omitempty"`
	WatchersCount   *int       `json:"watchers_count,omitempty"`
	Language        *string    `json:"language,omitempty"`
	HasIssues       *bool      `json:"has_issues,omitempty"`
	HasDownloads    *bool      `json:"has_downloads,omitempty"`
	HasWiki         *bool      `json:"has_wiki,omitempty"`
	HasPages        *bool      `json:"has_pages,omitempty"`
	ForksCount      *int       `json:"forks_count,omitempty"`
	Archived        *bool      `json:"archived,omitempty"`
	Disabled        *bool      `json:"disabled,omitempty"`
	OpenIssuesCount *int       `json:"open_issues_count,omitempty"`
	DefaultBranch   *string    `json:"default_branch,omitempty"`
	MasterBranch    *string    `json:"master_branch,omitempty"`
	Organization    *string    `json:"organization,omitempty"`
	URL             *string    `json:"url,omitempty"`
	ArchiveURL      *string    `json:"archive_url,omitempty"`
	HTMLURL         *string    `json:"html_url,omitempty"`
	StatusesURL     *string    `json:"statuses_url,omitempty"`
	GitURL          *string    `json:"git_url,omitempty"`
	SSHURL          *string    `json:"ssh_url,omitempty"`
	CloneURL        *string    `json:"clone_url,omitempty"`
	SVNURL          *string    `json:"svn_url,omitempty"`
}

// PushEventRepoOwner is a basic representation of user/org in a PushEvent payload.
type PushEventRepoOwner struct {
	Name  *string `json:"name,omitempty"`
	Email *string `json:"email,omitempty"`
}

// ReleaseEvent is triggered when a release is published, unpublished, created,
// edited, deleted, or prereleased.
// The Webhook event name is "release".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#release
type ReleaseEvent struct {
	// Action is the action that was performed. Possible values are: "published", "unpublished",
	// "created", "edited", "deleted", or "prereleased".
	Action  *string            `json:"action,omitempty"`
	Release *RepositoryRelease `json:"release,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// RepositoryEvent is triggered when a repository is created, archived, unarchived,
// renamed, edited, transferred, made public, or made private. Organization hooks are
// also trigerred when a repository is deleted.
// The Webhook event name is "repository".
//
// Events of this type are not visible in timelines, they are only used to
// trigger organization webhooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#repository
type RepositoryEvent struct {
	// Action is the action that was performed. Possible values are: "created",
	// "deleted" (organization hooks only), "archived", "unarchived", "edited", "renamed",
	// "transferred", "publicized", or "privatized".
	Action *string     `json:"action,omitempty"`
	Repo   *Repository `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Changes      *EditChange   `json:"changes,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// RepositoryDispatchEvent is triggered when a client sends a POST request to the repository dispatch event endpoint.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#repository_dispatch
type RepositoryDispatchEvent struct {
	// Action is the event_type that submitted with the repository dispatch payload. Value can be any string.
	Action        *string         `json:"action,omitempty"`
	Branch        *string         `json:"branch,omitempty"`
	ClientPayload json.RawMessage `json:"client_payload,omitempty"`
	Repo          *Repository     `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// RepositoryVulnerabilityAlertEvent is triggered when a security alert is created, dismissed, or resolved.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#repository_vulnerability_alert
type RepositoryVulnerabilityAlertEvent struct {
	// Action is the action that was performed. Possible values are: "create", "dismiss", "resolve".
	Action *string `json:"action,omitempty"`

	//The security alert of the vulnerable dependency.
	Alert *RepositoryVulnerabilityAlert `json:"alert,omitempty"`

	//The repository of the vulnerable dependency.
	Repository *Repository `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// RepositoryVulnerabilityAlert represents a repository security alert.
type RepositoryVulnerabilityAlert struct {
	ID                       *int64     `json:"id,omitempty"`
	AffectedRange            *string    `json:"affected_range,omitempty"`
	AffectedPackageName      *string    `json:"affected_package_name,omitempty"`
	ExternalReference        *string    `json:"external_reference,omitempty"`
	ExternalIdentifier       *string    `json:"external_identifier,omitempty"`
	GitHubSecurityAdvisoryID *string    `json:"ghsa_id,omitempty"`
	Severity                 *string    `json:"severity,omitempty"`
	CreatedAt                *Timestamp `json:"created_at,omitempty"`
	FixedIn                  *string    `json:"fixed_in,omitempty"`
	Dismisser                *User      `json:"dismisser,omitempty"`
	DismissReason            *string    `json:"dismiss_reason,omitempty"`
	DismissedAt              *Timestamp `json:"dismissed_at,omitempty"`
}

// StarEvent is triggered when a star is added or removed from a repository.
// The Webhook event name is "star".
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#star
type StarEvent struct {
	// Action is the action that was performed. Possible values are: "created" or "deleted".
	Action *string `json:"action,omitempty"`

	// StarredAt is the time the star was created. It will be null for the "deleted" action.
	StarredAt *Timestamp `json:"starred_at,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// StatusEvent is triggered when the status of a Git commit changes.
// The Webhook event name is "status".
//
// Events of this type are not visible in timelines, they are only used to
// trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#status
type StatusEvent struct {
	SHA *string `json:"sha,omitempty"`
	// State is the new state. Possible values are: "pending", "success", "failure", "error".
	State       *string   `json:"state,omitempty"`
	Description *string   `json:"description,omitempty"`
	TargetURL   *string   `json:"target_url,omitempty"`
	Branches    []*Branch `json:"branches,omitempty"`

	// The following fields are only populated by Webhook events.
	ID           *int64            `json:"id,omitempty"`
	Name         *string           `json:"name,omitempty"`
	Context      *string           `json:"context,omitempty"`
	Commit       *RepositoryCommit `json:"commit,omitempty"`
	CreatedAt    *Timestamp        `json:"created_at,omitempty"`
	UpdatedAt    *Timestamp        `json:"updated_at,omitempty"`
	Repo         *Repository       `json:"repository,omitempty"`
	Sender       *User             `json:"sender,omitempty"`
	Installation *Installation     `json:"installation,omitempty"`
}

// TeamEvent is triggered when an organization's team is created, modified or deleted.
// The Webhook event name is "team".
//
// Events of this type are not visible in timelines. These events are only used
// to trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#team
type TeamEvent struct {
	Action  *string     `json:"action,omitempty"`
	Team    *Team       `json:"team,omitempty"`
	Changes *TeamChange `json:"changes,omitempty"`
	Repo    *Repository `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// TeamAddEvent is triggered when a repository is added to a team.
// The Webhook event name is "team_add".
//
// Events of this type are not visible in timelines. These events are only used
// to trigger hooks.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#team_add
type TeamAddEvent struct {
	Team *Team       `json:"team,omitempty"`
	Repo *Repository `json:"repository,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// UserEvent is triggered when a user is created or deleted.
// The Webhook event name is "user".
//
// Only global webhooks can subscribe to this event type.
//
// GitHub API docs: https://developer.github.com/enterprise/v3/activity/events/types/#userevent-enterprise
type UserEvent struct {
	User *User `json:"user,omitempty"`
	// The action performed. Possible values are: "created" or "deleted".
	Action     *string     `json:"action,omitempty"`
	Enterprise *Enterprise `json:"enterprise,omitempty"`
	Sender     *User       `json:"sender,omitempty"`

	// The following fields are only populated by Webhook events.
	Installation *Installation `json:"installation,omitempty"`
}

// WatchEvent is related to starring a repository, not watching. See this API
// blog post for an explanation: https://developer.github.com/changes/2012-09-05-watcher-api/
//
// The event’s actor is the user who starred a repository, and the event’s
// repository is the repository that was starred.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#watch
type WatchEvent struct {
	// Action is the action that was performed. Possible value is: "started".
	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// WorkflowDispatchEvent is triggered when someone triggers a workflow run on GitHub or
// sends a POST request to the create a workflow dispatch event endpoint.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#workflow_dispatch
type WorkflowDispatchEvent struct {
	Inputs   json.RawMessage `json:"inputs,omitempty"`
	Ref      *string         `json:"ref,omitempty"`
	Workflow *string         `json:"workflow,omitempty"`

	// The following fields are only populated by Webhook events.
	Repo         *Repository   `json:"repository,omitempty"`
	Org          *Organization `json:"organization,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// WorkflowJobEvent is triggered when a job is queued, started or completed.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#workflow_job
type WorkflowJobEvent struct {
	WorkflowJob *WorkflowJob `json:"workflow_job,omitempty"`

	Action *string `json:"action,omitempty"`

	// The following fields are only populated by Webhook events.

	// Org is not nil when the webhook is configured for an organization or the event
	// occurs from activity in a repository owned by an organization.
	Org          *Organization `json:"organization,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}

// WorkflowRunEvent is triggered when a GitHub Actions workflow run is requested or completed.
//
// GitHub API docs: https://docs.github.com/en/developers/webhooks-and-events/webhook-events-and-payloads#workflow_run
type WorkflowRunEvent struct {
	Action      *string      `json:"action,omitempty"`
	Workflow    *Workflow    `json:"workflow,omitempty"`
	WorkflowRun *WorkflowRun `json:"workflow_run,omitempty"`

	// The following fields are only populated by Webhook events.
	Org          *Organization `json:"organization,omitempty"`
	Repo         *Repository   `json:"repository,omitempty"`
	Sender       *User         `json:"sender,omitempty"`
	Installation *Installation `json:"installation,omitempty"`
}
