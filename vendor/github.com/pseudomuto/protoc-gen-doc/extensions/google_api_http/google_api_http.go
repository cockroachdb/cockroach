package extensions

import (
	"net/http"

	"github.com/pseudomuto/protoc-gen-doc/extensions"
	"google.golang.org/genproto/googleapis/api/annotations"
)

// HTTPRule represents a single HTTP rule from the (google.api.http) method option extension.
type HTTPRule struct {
	Method  string `json:"method"`
	Pattern string `json:"pattern"`
	Body    string `json:"body,omitempty"`
}

// HTTPExtension contains the rules set by the (google.api.http) method option extension.
type HTTPExtension struct {
	Rules []HTTPRule `json:"rules"`
}

func getRule(r *annotations.HttpRule) (rule HTTPRule) {
	switch r.GetPattern().(type) {
	case *annotations.HttpRule_Get:
		rule.Method = http.MethodGet
		rule.Pattern = r.GetGet()
	case *annotations.HttpRule_Put:
		rule.Method = http.MethodPut
		rule.Pattern = r.GetPut()
	case *annotations.HttpRule_Post:
		rule.Method = http.MethodPost
		rule.Pattern = r.GetPost()
	case *annotations.HttpRule_Delete:
		rule.Method = http.MethodDelete
		rule.Pattern = r.GetDelete()
	case *annotations.HttpRule_Patch:
		rule.Method = http.MethodPatch
		rule.Pattern = r.GetPatch()
	case *annotations.HttpRule_Custom:
		custom := r.GetCustom()
		rule.Method = custom.GetKind()
		rule.Pattern = custom.GetPath()
	}
	rule.Body = r.GetBody()
	return
}

func init() {
	extensions.SetTransformer("google.api.http", func(payload interface{}) interface{} {
		var rules []HTTPRule
		rule, ok := payload.(*annotations.HttpRule)
		if !ok {
			return nil
		}

		rules = append(rules, getRule(rule))

		// NOTE: The option can only have one level of nested AdditionalBindings.
		for _, rule := range rule.AdditionalBindings {
			rules = append(rules, getRule(rule))
		}

		return HTTPExtension{Rules: rules}
	})
}
