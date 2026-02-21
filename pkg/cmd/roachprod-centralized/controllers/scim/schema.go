// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scim

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/controllers/scim/types"
	"github.com/gin-gonic/gin"
)

// ServiceProviderConfig returns identity management service provider configuration.
// GET /scim/v2/ServiceProviderConfig
func (ctrl *Controller) ServiceProviderConfig(c *gin.Context) {
	config := map[string]interface{}{
		"schemas": []string{types.SchemaServiceProviderConfig},
		"patch": map[string]bool{
			"supported": true,
		},
		"bulk": map[string]interface{}{
			"supported":      false,
			"maxOperations":  0,
			"maxPayloadSize": 0,
		},
		"filter": map[string]interface{}{
			"supported":  true,
			"maxResults": 200,
		},
		"changePassword": map[string]bool{
			"supported": false,
		},
		"sort": map[string]bool{
			"supported": true,
		},
		"etag": map[string]bool{
			"supported": false,
		},
		"authenticationSchemes": []map[string]interface{}{
			{
				"type":             "oauthbearertoken",
				"name":             "OAuth Bearer Token",
				"description":      "Authentication using bearer tokens",
				"specUri":          "https://tools.ietf.org/html/rfc6750",
				"documentationUri": "https://tools.ietf.org/html/rfc6750",
			},
		},
	}

	ctrl.renderIdentityResult(c, config, nil)
}

// Schemas returns supported identity management schemas.
// GET /scim/v2/Schemas
func (ctrl *Controller) Schemas(c *gin.Context) {
	schemas := map[string]interface{}{
		"schemas":      []string{types.SchemaListResponse},
		"totalResults": 2,
		"startIndex":   1,
		"itemsPerPage": 2,
		"Resources": []map[string]interface{}{
			{
				"schemas":     []string{types.SchemaSchema},
				"id":          types.SchemaUser,
				"name":        "User",
				"description": "User Account",
				"attributes": []map[string]interface{}{
					{
						"name":        "userName",
						"type":        "string",
						"multiValued": false,
						"required":    true,
						"caseExact":   false,
						"mutability":  "readWrite",
						"returned":    "default",
						"uniqueness":  "server",
						"description": "Unique identifier for the User",
					},
					{
						"name":        "externalId",
						"type":        "string",
						"multiValued": false,
						"required":    false,
						"caseExact":   true,
						"mutability":  "readWrite",
						"returned":    "default",
						"uniqueness":  "none",
						"description": "External identifier from identity provider",
					},
					{
						"name":        "name",
						"type":        "complex",
						"multiValued": false,
						"required":    false,
						"subAttributes": []map[string]interface{}{
							{
								"name":        "formatted",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The full name, including all middle names, titles, and suffixes",
							},
							{
								"name":        "familyName",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The family name of the User, or last name",
							},
							{
								"name":        "givenName",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The given name of the User, or first name",
							},
							{
								"name":        "middleName",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The middle name(s) of the User",
							},
							{
								"name":        "honorificPrefix",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The honorific prefix(es) of the User, or title",
							},
							{
								"name":        "honorificSuffix",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "The honorific suffix(es) of the User",
							},
						},
					},
					{
						"name":        "emails",
						"type":        "complex",
						"multiValued": true,
						"required":    false,
						"subAttributes": []map[string]interface{}{
							{
								"name":        "value",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "Email address",
							},
							{
								"name":        "type",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "Type of email (work, home, etc.)",
							},
							{
								"name":        "primary",
								"type":        "boolean",
								"multiValued": false,
								"required":    false,
								"description": "Whether this is the primary email",
							},
						},
					},
					{
						"name":        "active",
						"type":        "boolean",
						"multiValued": false,
						"required":    false,
					},
					{
						"name":        "groups",
						"type":        "complex",
						"multiValued": true,
						"required":    false,
						"mutability":  "readOnly",
						"subAttributes": []map[string]interface{}{
							{
								"name":        "value",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"mutability":  "readOnly",
								"description": "Identifier of the group",
							},
							{
								"name":           "$ref",
								"type":           "reference",
								"referenceTypes": []string{"Group"},
								"multiValued":    false,
								"required":       false,
								"mutability":     "readOnly",
								"description":    "URI of the group resource",
							},
							{
								"name":        "display",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"mutability":  "readOnly",
								"description": "Display name of the group",
							},
						},
					},
				},
				"meta": map[string]string{
					"resourceType": "Schema",
					"location":     types.ControllerPath + "/Schemas/" + types.SchemaUser,
				},
			},
			{
				"schemas":     []string{types.SchemaSchema},
				"id":          types.SchemaGroup,
				"name":        "Group",
				"description": "Group",
				"attributes": []map[string]interface{}{
					{
						"name":        "displayName",
						"type":        "string",
						"multiValued": false,
						"required":    true,
						"caseExact":   false,
						"mutability":  "readWrite",
						"returned":    "default",
						"uniqueness":  "none",
						"description": "Human-readable name for the Group",
					},
					{
						"name":        "externalId",
						"type":        "string",
						"multiValued": false,
						"required":    false,
						"caseExact":   true,
						"mutability":  "readWrite",
						"returned":    "default",
						"uniqueness":  "none",
						"description": "External identifier from identity provider",
					},
					{
						"name":        "members",
						"type":        "complex",
						"multiValued": true,
						"required":    false,
						"mutability":  "readWrite",
						"returned":    "default",
						"subAttributes": []map[string]interface{}{
							{
								"name":        "value",
								"type":        "string",
								"multiValued": false,
								"required":    true,
								"description": "Identifier of the member",
							},
							{
								"name":           "$ref",
								"type":           "reference",
								"referenceTypes": []string{"User"},
								"multiValued":    false,
								"required":       false,
								"description":    "URI of the member resource",
							},
							{
								"name":        "display",
								"type":        "string",
								"multiValued": false,
								"required":    false,
								"description": "Display name of the member",
							},
						},
					},
				},
				"meta": map[string]string{
					"resourceType": "Schema",
					"location":     types.ControllerPath + "/Schemas/" + types.SchemaGroup,
				},
			},
		},
	}

	ctrl.renderIdentityResult(c, schemas, nil)
}

// ResourceTypes returns supported identity management resource types.
// GET /scim/v2/ResourceTypes
func (ctrl *Controller) ResourceTypes(c *gin.Context) {
	resourceTypes := map[string]interface{}{
		"schemas":      []string{types.SchemaListResponse},
		"totalResults": 2,
		"startIndex":   1,
		"itemsPerPage": 2,
		"Resources": []map[string]interface{}{
			{
				"schemas":     []string{types.SchemaResourceType},
				"id":          "User",
				"name":        "User",
				"endpoint":    "/Users",
				"description": "User Account",
				"schema":      types.SchemaUser,
				"meta": map[string]string{
					"resourceType": "ResourceType",
					"location":     types.ControllerPath + "/ResourceTypes/User",
				},
			},
			{
				"schemas":     []string{types.SchemaResourceType},
				"id":          "Group",
				"name":        "Group",
				"endpoint":    "/Groups",
				"description": "Group",
				"schema":      types.SchemaGroup,
				"meta": map[string]string{
					"resourceType": "ResourceType",
					"location":     types.ControllerPath + "/ResourceTypes/Group",
				},
			},
		},
	}

	ctrl.renderIdentityResult(c, resourceTypes, nil)
}
