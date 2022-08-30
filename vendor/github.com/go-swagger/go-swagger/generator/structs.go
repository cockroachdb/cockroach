package generator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/go-openapi/analysis"
	"github.com/go-openapi/spec"
)

// GenCommon contains common properties needed across
// definitions, app and operations
// TargetImportPath may be used by templates to import other (possibly
// generated) packages in the generation path (e.g. relative to GOPATH).
// TargetImportPath is NOT used by standard templates.
type GenCommon struct {
	Copyright        string
	TargetImportPath string
}

// GenDefinition contains all the properties to generate a
// definition from a swagger spec
type GenDefinition struct {
	GenCommon
	GenSchema
	Package        string
	Imports        map[string]string
	DefaultImports map[string]string
	ExtraSchemas   GenSchemaList
	DependsOn      []string
	External       bool
}

// GenDefinitions represents a list of operations to generate
// this implements a sort by operation id
type GenDefinitions []GenDefinition

func (g GenDefinitions) Len() int           { return len(g) }
func (g GenDefinitions) Less(i, j int) bool { return g[i].Name < g[j].Name }
func (g GenDefinitions) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }

// GenSchemaList is a list of schemas for generation.
//
// It can be sorted by name to get a stable struct layout for
// version control and such
type GenSchemaList []GenSchema

// GenSchema contains all the information needed to generate the code
// for a schema
type GenSchema struct {
	resolvedType
	sharedValidations
	Example                    string
	OriginalName               string
	Name                       string
	Suffix                     string
	Path                       string
	ValueExpression            string
	IndexVar                   string
	KeyVar                     string
	Title                      string
	Description                string
	Location                   string
	ReceiverName               string
	Items                      *GenSchema
	AllowsAdditionalItems      bool
	HasAdditionalItems         bool
	AdditionalItems            *GenSchema
	Object                     *GenSchema
	XMLName                    string
	CustomTag                  string
	Properties                 GenSchemaList
	AllOf                      GenSchemaList
	HasAdditionalProperties    bool
	IsAdditionalProperties     bool
	AdditionalProperties       *GenSchema
	StrictAdditionalProperties bool
	ReadOnly                   bool
	IsVirtual                  bool
	IsBaseType                 bool
	HasBaseType                bool
	IsSubType                  bool
	IsExported                 bool
	DiscriminatorField         string
	DiscriminatorValue         string
	Discriminates              map[string]string
	Parents                    []string
	IncludeValidator           bool
	IncludeModel               bool
	Default                    interface{}
	WantsMarshalBinary         bool // do we generate MarshalBinary interface?
	StructTags                 []string
	ExtraImports               map[string]string // non-standard imports detected when using external types
	ExternalDocs               *spec.ExternalDocumentation
}

func (g GenSchema) renderMarshalTag() string {
	if g.HasBaseType {
		return "-"
	}

	var result strings.Builder

	result.WriteString(g.OriginalName)

	if !g.Required && g.IsEmptyOmitted {
		result.WriteString(",omitempty")
	}

	if g.IsJSONString {
		result.WriteString(",string")
	}

	return result.String()
}

// PrintTags takes care of rendering tags for a struct field
func (g GenSchema) PrintTags() string {
	tags := make(map[string]string, 3)
	orderedTags := make([]string, 0, 3)

	tags["json"] = g.renderMarshalTag()
	orderedTags = append(orderedTags, "json")

	if len(g.XMLName) > 0 {
		if !g.Required && g.IsEmptyOmitted {
			tags["xml"] = g.XMLName + ",omitempty"
		} else {
			tags["xml"] = g.XMLName
		}
		orderedTags = append(orderedTags, "xml")
	}

	// Add extra struct tags, only if the tag hasn't already been set, i.e. example.
	// Extra struct tags have the same value has the `json` tag.
	for _, tag := range g.StructTags {
		if _, exists := tags[tag]; exists {
			// dedupe
			continue
		}

		if tag == "example" && len(g.Example) > 0 {
			// only add example tag if it's contained in the struct tags
			tags["example"] = g.Example // json representation of the example object
		} else {
			tags[tag] = tags["json"]
		}

		orderedTags = append(orderedTags, tag)
	}

	// Assemble the tags in key value pairs with the value properly quoted.
	kvPairs := make([]string, 0, len(orderedTags)+1)
	for _, key := range orderedTags {
		kvPairs = append(kvPairs, fmt.Sprintf("%s:%s", key, strconv.Quote(tags[key])))
	}

	if len(g.CustomTag) > 0 {
		kvPairs = append(kvPairs, g.CustomTag)
	}

	// Join the key value pairs by a space.
	completeTag := strings.Join(kvPairs, " ")

	// If the values contain a backtick, we cannot render the tag using backticks because Go does not support
	// escaping backticks in raw string literals.
	valuesHaveBacktick := false
	for _, value := range tags {
		if !strconv.CanBackquote(value) {
			valuesHaveBacktick = true
			break
		}
	}

	if !valuesHaveBacktick {
		return fmt.Sprintf("`%s`", completeTag)
	}

	// We have to escape the tag again to put it in a literal with double quotes as the tag format uses double quotes.
	return strconv.Quote(completeTag)
}

// UnderlyingType tells the go type or the aliased go type
func (g GenSchema) UnderlyingType() string {
	if g.IsAliased {
		return g.AliasedType
	}
	return g.GoType
}

// ToString returns a string conversion expression for the schema
func (g GenSchema) ToString() string {
	return g.resolvedType.ToString(g.ValueExpression)
}

func (g GenSchemaList) Len() int      { return len(g) }
func (g GenSchemaList) Swap(i, j int) { g[i], g[j] = g[j], g[i] }
func (g GenSchemaList) Less(i, j int) bool {
	a, okA := g[i].Extensions[xOrder].(float64)
	b, okB := g[j].Extensions[xOrder].(float64)

	// If both properties have x-order defined, then the one with lower x-order is smaller
	if okA && okB {
		return a < b
	}

	// If only the first property has x-order defined, then it is smaller
	if okA {
		return true
	}

	// If only the second property has x-order defined, then it is smaller
	if okB {
		return false
	}

	// If neither property has x-order defined, then the one with lower lexicographic name is smaller
	return g[i].Name < g[j].Name
}

type sharedValidations struct {
	spec.SchemaValidations

	HasValidations        bool
	HasContextValidations bool
	Required              bool
	HasSliceValidations   bool
	ItemsEnum             []interface{}

	// NOTE: "patternProperties" and "dependencies" not supported by Swagger 2.0
}

// GenResponse represents a response object for code generation
type GenResponse struct {
	Package       string
	ModelsPackage string
	ReceiverName  string
	Name          string
	Description   string

	IsSuccess bool

	Code               int
	Method             string
	Path               string
	Headers            GenHeaders
	Schema             *GenSchema
	AllowsForStreaming bool

	Imports        map[string]string
	DefaultImports map[string]string

	Extensions map[string]interface{}

	StrictResponders bool
	OperationName    string
	Examples         GenResponseExamples
}

// GenResponseExamples is a sortable collection []GenResponseExample
type GenResponseExamples []GenResponseExample

func (g GenResponseExamples) Len() int           { return len(g) }
func (g GenResponseExamples) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenResponseExamples) Less(i, j int) bool { return g[i].MediaType < g[j].MediaType }

// GenResponseExample captures an example provided for a response for some mime type
type GenResponseExample struct {
	MediaType string
	Example   interface{}
}

// GenHeader represents a header on a response for code generation
type GenHeader struct {
	resolvedType
	sharedValidations

	Package      string
	ReceiverName string
	IndexVar     string

	ID              string
	Name            string
	Path            string
	ValueExpression string

	Title       string
	Description string
	Default     interface{}
	HasDefault  bool

	CollectionFormat string

	Child  *GenItems
	Parent *GenItems

	Converter string
	Formatter string

	ZeroValue string
}

// ItemsDepth returns a string "items.items..." with as many items as the level of nesting of the array.
// For a header objects it always returns "".
func (h *GenHeader) ItemsDepth() string {
	// NOTE: this is currently used by templates to generate explicit comments in nested structures
	return ""
}

// ToString returns a string conversion expression for the header
func (h GenHeader) ToString() string {
	return h.resolvedType.ToString(h.ValueExpression)
}

// GenHeaders is a sorted collection of headers for codegen
type GenHeaders []GenHeader

func (g GenHeaders) Len() int           { return len(g) }
func (g GenHeaders) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenHeaders) Less(i, j int) bool { return g[i].Name < g[j].Name }

// HasSomeDefaults returns true is at least one header has a default value set
func (g GenHeaders) HasSomeDefaults() bool {
	// NOTE: this is currently used by templates to avoid empty constructs
	for _, header := range g {
		if header.HasDefault {
			return true
		}
	}
	return false
}

// GenParameter is used to represent
// a parameter or a header for code generation.
type GenParameter struct {
	resolvedType
	sharedValidations

	ID              string
	Name            string
	ModelsPackage   string
	Path            string
	ValueExpression string
	IndexVar        string
	KeyVar          string
	ReceiverName    string
	Location        string
	Title           string
	Description     string
	Converter       string
	Formatter       string

	Schema *GenSchema

	CollectionFormat string

	Child  *GenItems
	Parent *GenItems

	// Unused
	// BodyParam *GenParameter

	Default         interface{}
	HasDefault      bool
	ZeroValue       string
	AllowEmptyValue bool

	// validation strategy for Body params, which may mix model and simple constructs.
	// Distinguish the following cases:
	// - HasSimpleBodyParams: body is an inline simple type
	// - HasModelBodyParams: body is a model objectd
	// - HasSimpleBodyItems: body is an inline array of simple type
	// - HasModelBodyItems: body is an array of model objects
	// - HasSimpleBodyMap: body is a map of simple objects (possibly arrays)
	// - HasModelBodyMap: body is a map of model objects
	HasSimpleBodyParams bool
	HasModelBodyParams  bool
	HasSimpleBodyItems  bool
	HasModelBodyItems   bool
	HasSimpleBodyMap    bool
	HasModelBodyMap     bool

	Extensions map[string]interface{}
}

// IsQueryParam returns true when this parameter is a query param
func (g *GenParameter) IsQueryParam() bool {
	return g.Location == "query"
}

// IsPathParam returns true when this parameter is a path param
func (g *GenParameter) IsPathParam() bool {
	return g.Location == "path"
}

// IsFormParam returns true when this parameter is a form param
func (g *GenParameter) IsFormParam() bool {
	return g.Location == "formData"
}

// IsHeaderParam returns true when this parameter is a header param
func (g *GenParameter) IsHeaderParam() bool {
	return g.Location == "header"
}

// IsBodyParam returns true when this parameter is a body param
func (g *GenParameter) IsBodyParam() bool {
	return g.Location == "body"
}

// IsFileParam returns true when this parameter is a file param
func (g *GenParameter) IsFileParam() bool {
	return g.SwaggerType == "file"
}

// ItemsDepth returns a string "items.items..." with as many items as the level of nesting of the array.
// For a parameter object, it always returns "".
func (g *GenParameter) ItemsDepth() string {
	// NOTE: this is currently used by templates to generate explicit comments in nested structures
	return ""
}

// UnderlyingType tells the go type or the aliased go type
func (g GenParameter) UnderlyingType() string {
	return g.GoType
}

// ToString returns a string conversion expression for the parameter
func (g GenParameter) ToString() string {
	return g.resolvedType.ToString(g.ValueExpression)
}

// GenParameters represents a sorted parameter collection
type GenParameters []GenParameter

func (g GenParameters) Len() int           { return len(g) }
func (g GenParameters) Less(i, j int) bool { return g[i].Name < g[j].Name }
func (g GenParameters) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }

// HasSomeDefaults returns true is at least one parameter has a default value set
func (g GenParameters) HasSomeDefaults() bool {
	// NOTE: this is currently used by templates to avoid empty constructs
	for _, param := range g {
		if param.HasDefault {
			return true
		}
	}
	return false
}

// GenItems represents the collection items for a collection parameter
type GenItems struct {
	sharedValidations
	resolvedType

	Name             string
	Path             string
	ValueExpression  string
	CollectionFormat string
	Child            *GenItems
	Parent           *GenItems
	Converter        string
	Formatter        string

	Location string
	IndexVar string
	KeyVar   string

	// instructs generator to skip the splitting and parsing from CollectionFormat
	SkipParse bool
	// instructs generator that some nested structure needs an higher level loop index
	NeedsIndex bool
}

// ItemsDepth returns a string "items.items..." with as many items as the level of nesting of the array.
func (g *GenItems) ItemsDepth() string {
	// NOTE: this is currently used by templates to generate explicit comments in nested structures
	current := g
	i := 1
	for current.Parent != nil {
		i++
		current = current.Parent
	}
	return strings.Repeat("items.", i)
}

// UnderlyingType tells the go type or the aliased go type
func (g GenItems) UnderlyingType() string {
	return g.GoType
}

// ToString returns a string conversion expression for the item
func (g GenItems) ToString() string {
	return g.resolvedType.ToString(g.ValueExpression)
}

// GenOperationGroup represents a named (tagged) group of operations
type GenOperationGroup struct {
	GenCommon
	Name       string
	Operations GenOperations

	Summary        string
	Description    string
	Imports        map[string]string
	DefaultImports map[string]string
	RootPackage    string
	GenOpts        *GenOpts
	PackageAlias   string
}

// GenOperationGroups is a sorted collection of operation groups
type GenOperationGroups []GenOperationGroup

func (g GenOperationGroups) Len() int           { return len(g) }
func (g GenOperationGroups) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenOperationGroups) Less(i, j int) bool { return g[i].Name < g[j].Name }

// GenStatusCodeResponses a container for status code responses
type GenStatusCodeResponses []GenResponse

func (g GenStatusCodeResponses) Len() int           { return len(g) }
func (g GenStatusCodeResponses) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenStatusCodeResponses) Less(i, j int) bool { return g[i].Code < g[j].Code }

// MarshalJSON marshals these responses to json
//
// This is used by DumpData.
func (g GenStatusCodeResponses) MarshalJSON() ([]byte, error) {
	if g == nil {
		return nil, nil
	}
	responses := make(GenStatusCodeResponses, len(g))
	copy(responses, g)
	// order marshalled output
	sort.Sort(responses)

	var buf bytes.Buffer
	buf.WriteRune('{')
	for i, v := range responses {
		rb, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			buf.WriteRune(',')
		}
		buf.WriteString(fmt.Sprintf("%q:", strconv.Itoa(v.Code)))
		buf.Write(rb)
	}
	buf.WriteRune('}')
	return buf.Bytes(), nil
}

// UnmarshalJSON unmarshals this GenStatusCodeResponses from json
func (g *GenStatusCodeResponses) UnmarshalJSON(data []byte) error {
	var dd map[string]GenResponse
	if err := json.Unmarshal(data, &dd); err != nil {
		return err
	}
	var gg GenStatusCodeResponses
	for _, v := range dd {
		gg = append(gg, v)
	}
	sort.Sort(gg)
	*g = gg
	return nil
}

// GenOperation represents an operation for code generation
type GenOperation struct {
	GenCommon
	Package      string
	ReceiverName string
	Name         string
	Summary      string
	Description  string
	Method       string
	Path         string
	BasePath     string
	Tags         []string
	UseTags      bool
	RootPackage  string

	Imports        map[string]string
	DefaultImports map[string]string
	ExtraSchemas   GenSchemaList
	PackageAlias   string

	Authorized           bool
	Security             []GenSecurityRequirements // resolved security requirements for the operation
	SecurityDefinitions  GenSecuritySchemes
	SecurityRequirements []analysis.SecurityRequirement // original security requirements as per the spec (for doc)
	Principal            string
	PrincipalIsNullable  bool

	SuccessResponse  *GenResponse
	SuccessResponses []GenResponse
	Responses        GenStatusCodeResponses
	DefaultResponse  *GenResponse

	Params               GenParameters
	QueryParams          GenParameters
	PathParams           GenParameters
	HeaderParams         GenParameters
	FormParams           GenParameters
	HasQueryParams       bool
	HasPathParams        bool
	HasHeaderParams      bool
	HasFormParams        bool
	HasFormValueParams   bool
	HasFileParams        bool
	HasBodyParams        bool
	HasStreamingResponse bool

	Schemes              []string
	ExtraSchemes         []string
	SchemeOverrides      []string // original scheme overrides for operation, as per spec (for doc)
	ExtraSchemeOverrides []string // original extra scheme overrides for operation, as per spec (for doc)
	ProducesMediaTypes   []string
	ConsumesMediaTypes   []string
	TimeoutName          string

	Extensions map[string]interface{}

	StrictResponders bool
	ExternalDocs     *spec.ExternalDocumentation
	Produces         []string // original produces for operation (for doc)
	Consumes         []string // original consumes for operation (for doc)
}

// GenOperations represents a list of operations to generate
// this implements a sort by operation id
type GenOperations []GenOperation

func (g GenOperations) Len() int           { return len(g) }
func (g GenOperations) Less(i, j int) bool { return g[i].Name < g[j].Name }
func (g GenOperations) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }

// GenApp represents all the meta data needed to generate an application
// from a swagger spec
type GenApp struct {
	GenCommon
	APIPackage           string
	ServerPackageAlias   string
	APIPackageAlias      string
	Package              string
	ReceiverName         string
	Name                 string
	Principal            string
	PrincipalIsNullable  bool
	DefaultConsumes      string
	DefaultProduces      string
	Host                 string
	BasePath             string
	Info                 *spec.Info
	ExternalDocs         *spec.ExternalDocumentation
	Tags                 []spec.Tag
	Imports              map[string]string
	DefaultImports       map[string]string
	Schemes              []string
	ExtraSchemes         []string
	Consumes             GenSerGroups
	Produces             GenSerGroups
	SecurityDefinitions  GenSecuritySchemes
	SecurityRequirements []analysis.SecurityRequirement // original security requirements as per the spec (for doc)
	Models               []GenDefinition
	Operations           GenOperations
	OperationGroups      GenOperationGroups
	SwaggerJSON          string
	// Embedded specs: this is important for when the generated server adds routes.
	// NOTE: there is a distinct advantage to having this in runtime rather than generated code.
	// We are not ever going to generate the router.
	// If embedding spec is an issue (e.g. memory usage), this can be excluded with the --exclude-spec
	// generation option. Alternative methods to serve spec (e.g. from disk, ...) may be implemented by
	// adding a middleware to the generated API.
	FlatSwaggerJSON string
	ExcludeSpec     bool
	GenOpts         *GenOpts
}

// UseGoStructFlags returns true when no strategy is specified or it is set to "go-flags"
func (g *GenApp) UseGoStructFlags() bool {
	if g.GenOpts == nil {
		return true
	}
	return g.GenOpts.FlagStrategy == "" || g.GenOpts.FlagStrategy == "go-flags"
}

// UsePFlags returns true when the flag strategy is set to pflag
func (g *GenApp) UsePFlags() bool {
	return g.GenOpts != nil && strings.HasPrefix(g.GenOpts.FlagStrategy, "pflag")
}

// UseFlags returns true when the flag strategy is set to flag
func (g *GenApp) UseFlags() bool {
	return g.GenOpts != nil && strings.HasPrefix(g.GenOpts.FlagStrategy, "flag")
}

// UseIntermediateMode for https://wiki.mozilla.org/Security/Server_Side_TLS#Intermediate_compatibility_.28default.29
func (g *GenApp) UseIntermediateMode() bool {
	return g.GenOpts != nil && g.GenOpts.CompatibilityMode == "intermediate"
}

// UseModernMode for https://wiki.mozilla.org/Security/Server_Side_TLS#Modern_compatibility
func (g *GenApp) UseModernMode() bool {
	return g.GenOpts == nil || g.GenOpts.CompatibilityMode == "" || g.GenOpts.CompatibilityMode == "modern"
}

// GenSerGroups sorted representation of serializer groups
type GenSerGroups []GenSerGroup

func (g GenSerGroups) Len() int           { return len(g) }
func (g GenSerGroups) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenSerGroups) Less(i, j int) bool { return g[i].Name < g[j].Name }

// GenSerGroup represents a group of serializers: this links a serializer to a list of
// prioritized media types (mime).
type GenSerGroup struct {
	GenSerializer

	// All media types for this serializer. The redundant representation allows for easier use in templates
	AllSerializers GenSerializers
}

// GenSerializers sorted representation of serializers
type GenSerializers []GenSerializer

func (g GenSerializers) Len() int           { return len(g) }
func (g GenSerializers) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenSerializers) Less(i, j int) bool { return g[i].MediaType < g[j].MediaType }

// GenSerializer represents a single serializer for a particular media type
type GenSerializer struct {
	AppName        string // Application name
	ReceiverName   string
	Name           string   // Name of the Producer/Consumer (e.g. json, yaml, txt, bin)
	MediaType      string   // mime
	Implementation string   // func implementing the Producer/Consumer
	Parameters     []string // parameters supported by this serializer
}

// GenSecurityScheme represents a security scheme for code generation
type GenSecurityScheme struct {
	AppName             string
	ID                  string
	Name                string
	ReceiverName        string
	IsBasicAuth         bool
	IsAPIKeyAuth        bool
	IsOAuth2            bool
	Scopes              []string
	Source              string
	Principal           string
	PrincipalIsNullable bool

	// from spec.SecurityScheme
	Description      string
	Type             string
	In               string
	Flow             string
	AuthorizationURL string
	TokenURL         string
	Extensions       map[string]interface{}
	ScopesDesc       []GenSecurityScope
}

// GenSecuritySchemes sorted representation of serializers
type GenSecuritySchemes []GenSecurityScheme

func (g GenSecuritySchemes) Len() int           { return len(g) }
func (g GenSecuritySchemes) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenSecuritySchemes) Less(i, j int) bool { return g[i].ID < g[j].ID }

// GenSecurityRequirement represents a security requirement for an operation
type GenSecurityRequirement struct {
	Name   string
	Scopes []string
}

// GenSecurityScope represents a scope descriptor for an OAuth2 security scheme
type GenSecurityScope struct {
	Name        string
	Description string
}

// GenSecurityRequirements represents a compounded security requirement specification.
// In a []GenSecurityRequirements complete requirements specification,
// outer elements are interpreted as optional requirements (OR), and
// inner elements are interpreted as jointly required (AND).
type GenSecurityRequirements []GenSecurityRequirement

func (g GenSecurityRequirements) Len() int           { return len(g) }
func (g GenSecurityRequirements) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g GenSecurityRequirements) Less(i, j int) bool { return g[i].Name < g[j].Name }
