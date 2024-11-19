// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "time"

type jiraFieldId struct {
	Id string `json:"id"`
}

// docsIssue contains details about each formatted commit to be committed to the docs repo.
type docsIssue struct {
	Fields docsIssueFields `json:"fields"`
}

type docsIssueFields struct {
	IssueType              jiraFieldId   `json:"issuetype"`
	Project                jiraFieldId   `json:"project"`
	Summary                string        `json:"summary"`
	Reporter               jiraFieldId   `json:"reporter"`
	Description            adfRoot       `json:"description"`
	DocType                jiraFieldId   `json:"customfield_10175"`
	FixVersions            []jiraFieldId `json:"fixVersions"`
	EpicLink               string        `json:"customfield_10014,omitempty"`
	ProductChangePrNumber  string        `json:"customfield_10435"`
	ProductChangeCommitSHA string        `json:"customfield_10436"`
}

type docsIssueBatch struct {
	IssueUpdates []docsIssue `json:"issueUpdates"`
}

type jiraBulkIssueCreateResponse struct {
	Issues []struct {
		Id         string `json:"id"`
		Key        string `json:"key"`
		Self       string `json:"self"`
		Transition struct {
			Status          int `json:"status"`
			ErrorCollection struct {
				ErrorMessages []interface{} `json:"errorMessages"`
				Errors        struct {
				} `json:"errors"`
			} `json:"errorCollection"`
		} `json:"transition,omitempty"`
	} `json:"issues"`
	Errors []interface{} `json:"errors"`
}

// queryParameters stores the GitHub API token, a dry run flag to output the issues it would create, and the
// start and end times of the search.
type queryParameters struct {
	DryRun    bool
	StartTime time.Time
	EndTime   time.Time
}

type apiTokenParameters struct {
	GitHubToken string
	JiraToken   string
}

// pageInfo contains pagination information for querying the GraphQL API.
type pageInfo struct {
	HasNextPage bool   `json:"hasNextPage"`
	EndCursor   string `json:"endCursor"`
}

// gqlCockroachPRCommit contains details about commits within PRs in the cockroach repo.
type gqlCockroachPRCommit struct {
	Data struct {
		Repository struct {
			PullRequest struct {
				Commits struct {
					Edges []struct {
						Node struct {
							Commit struct {
								Oid             string `json:"oid"`
								MessageHeadline string `json:"messageHeadline"`
								MessageBody     string `json:"messageBody"`
							} `json:"commit"`
						} `json:"node"`
					} `json:"edges"`
					PageInfo pageInfo `json:"pageInfo"`
				} `json:"commits"`
			} `json:"pullRequest"`
		} `json:"repository"`
	} `json:"data"`
}

// gqlCockroachPR contains details about PRs within the cockroach repo.
type gqlCockroachPR struct {
	Data struct {
		Search struct {
			Nodes []struct {
				Title       string `json:"title"`
				Number      int    `json:"number"`
				Body        string `json:"body"`
				BaseRefName string `json:"baseRefName"`
				Commits     struct {
					Edges []struct {
						Node struct {
							Commit struct {
								Oid             string `json:"oid"`
								MessageHeadline string `json:"messageHeadline"`
								MessageBody     string `json:"messageBody"`
							} `json:"commit"`
						} `json:"node"`
					} `json:"edges"`
					PageInfo pageInfo `json:"pageInfo"`
				} `json:"commits"`
			} `json:"nodes"`
			PageInfo pageInfo `json:"pageInfo"`
		} `json:"search"`
	} `json:"data"`
}

type gqlSingleIssue struct {
	Data struct {
		Repository struct {
			Issue struct {
				Body string `json:"body"`
			} `json:"issue"`
		} `json:"repository"`
	} `json:"data"`
}

type gqlRef struct {
	Data struct {
		Repository struct {
			Refs struct {
				Edges []struct {
					Node struct {
						Name string `json:"name"`
					} `json:"node"`
				} `json:"edges"`
				PageInfo pageInfo `json:"pageInfo"`
			} `json:"refs"`
		} `json:"repository"`
	} `json:"data"`
}

type jiraIssueSearch struct {
	StartAt    int `json:"startAt"`
	MaxResults int `json:"maxResults"`
	Total      int `json:"total"`
	Issues     []struct {
		Key            string `json:"key"`
		RenderedFields struct {
			Description string `json:"description"`
		} `json:"renderedFields"`
	} `json:"issues"`
}

type jiraIssueCreateMeta struct {
	Projects []struct {
		Issuetypes []struct {
			Fields struct {
				Issuetype struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string        `json:"name"`
					Key             string        `json:"key"`
					HasDefaultValue bool          `json:"hasDefaultValue"`
					Operations      []interface{} `json:"operations"`
					AllowedValues   []struct {
						Self           string `json:"self"`
						Id             string `json:"id"`
						Description    string `json:"description"`
						IconUrl        string `json:"iconUrl"`
						Name           string `json:"name"`
						Subtask        bool   `json:"subtask"`
						AvatarId       int    `json:"avatarId"`
						HierarchyLevel int    `json:"hierarchyLevel"`
					} `json:"allowedValues"`
				} `json:"issuetype"`
				Description struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"description"`
				Project struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
					AllowedValues   []struct {
						Self           string `json:"self"`
						Id             string `json:"id"`
						Key            string `json:"key"`
						Name           string `json:"name"`
						ProjectTypeKey string `json:"projectTypeKey"`
						Simplified     bool   `json:"simplified"`
						AvatarUrls     struct {
							X48 string `json:"48x48"`
							X24 string `json:"24x24"`
							X16 string `json:"16x16"`
							X32 string `json:"32x32"`
						} `json:"avatarUrls"`
						ProjectCategory struct {
							Self        string `json:"self"`
							Id          string `json:"id"`
							Description string `json:"description"`
							Name        string `json:"name"`
						} `json:"projectCategory"`
					} `json:"allowedValues"`
				} `json:"project"`
				DocType struct {
					Required bool `json:"required"`
					Schema   struct {
						Type     string `json:"type"`
						Custom   string `json:"custom"`
						CustomId int    `json:"customId"`
					} `json:"schema"`
					Name            string                                   `json:"name"`
					Key             string                                   `json:"key"`
					HasDefaultValue bool                                     `json:"hasDefaultValue"`
					Operations      []string                                 `json:"operations"`
					AllowedValues   []jiraCreateIssueMetaDocTypeAllowedValue `json:"allowedValues"`
				} `json:"customfield_10175"`
				FixVersions struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						Items  string `json:"items"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string                                       `json:"name"`
					Key             string                                       `json:"key"`
					HasDefaultValue bool                                         `json:"hasDefaultValue"`
					Operations      []string                                     `json:"operations"`
					AllowedValues   []jiraCreateIssueMetaFixVersionsAllowedValue `json:"allowedValues"`
				} `json:"fixVersions"`
				EpicLink struct {
					Required bool `json:"required"`
					Schema   struct {
						Type     string `json:"type"`
						Custom   string `json:"custom"`
						CustomId int    `json:"customId"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"customfield_10014"`
				Summary struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"summary"`
				Reporter struct {
					Required bool `json:"required"`
					Schema   struct {
						Type   string `json:"type"`
						System string `json:"system"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					AutoCompleteUrl string   `json:"autoCompleteUrl"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"reporter"`
				ProductChangePRNumber struct {
					Required bool `json:"required"`
					Schema   struct {
						Type     string `json:"type"`
						Custom   string `json:"custom"`
						CustomId int    `json:"customId"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"customfield_10435"`
				ProductChangeCommitSHA struct {
					Required bool `json:"required"`
					Schema   struct {
						Type     string `json:"type"`
						Custom   string `json:"custom"`
						CustomId int    `json:"customId"`
					} `json:"schema"`
					Name            string   `json:"name"`
					Key             string   `json:"key"`
					HasDefaultValue bool     `json:"hasDefaultValue"`
					Operations      []string `json:"operations"`
				} `json:"customfield_10436"`
			} `json:"fields"`
		} `json:"issuetypes"`
	} `json:"projects"`
}

type jiraCreateIssueMetaDocTypeAllowedValue struct {
	Self  string `json:"self"`
	Value string `json:"value"`
	Id    string `json:"id"`
}

type jiraIssue struct {
	Fields struct {
		Issuetype struct {
			Id string `json:"id"`
		} `json:"issuetype"`
		EpicLink string `json:"customfield_10014"`
	} `json:"fields"`
}

type jiraCreateIssueMetaFixVersionsAllowedValue struct {
	Self            string `json:"self"`
	Id              string `json:"id"`
	Description     string `json:"description,omitempty"`
	Name            string `json:"name"`
	Archived        bool   `json:"archived"`
	Released        bool   `json:"released"`
	ProjectId       int    `json:"projectId"`
	StartDate       string `json:"startDate,omitempty"`
	ReleaseDate     string `json:"releaseDate,omitempty"`
	Overdue         bool   `json:"overdue,omitempty"`
	UserStartDate   string `json:"userStartDate,omitempty"`
	UserReleaseDate string `json:"userReleaseDate,omitempty"`
}

type cockroachPR struct {
	Title       string `json:"title"`
	Number      int    `json:"number"`
	Body        string `json:"body"`
	BaseRefName string `json:"baseRefName"`
	Commits     []cockroachCommit
}

type cockroachCommit struct {
	Sha             string `json:"oid"`
	MessageHeadline string `json:"messageHeadline"`
	MessageBody     string `json:"messageBody"`
}

type epicIssueRefInfo struct {
	epicRefs        map[string]int
	epicNone        bool
	issueCloseRefs  map[string]int
	issueInformRefs map[string]int
}

type adfRoot struct {
	Version int       `json:"version"` // 1
	Type    string    `json:"type"`    // doc
	Content []adfNode `json:"content"`
}

type adfNode struct {
	Type    string    `json:"type"`
	Content []adfNode `json:"content,omitempty"` // block nodes only, not inline nodes
	Text    string    `json:"text,omitempty"`
	Marks   []adfMark `json:"marks,omitempty"` // inline nodes only
}

type adfMark struct {
	Type  string            `json:"type"`
	Attrs map[string]string `json:"attrs"`
}

type httpReqSource int
