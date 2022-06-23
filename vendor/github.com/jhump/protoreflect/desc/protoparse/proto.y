%{
package protoparse

//lint:file-ignore SA4006 generated parser has unused values

import (
	"math"

	"github.com/jhump/protoreflect/desc/protoparse/ast"
)

%}

// fields inside this union end up as the fields in a structure known
// as ${PREFIX}SymType, of which a reference is passed to the lexer.
%union{
	file      *ast.FileNode
	syn       *ast.SyntaxNode
	fileDecl  ast.FileElement
	fileDecls []ast.FileElement
	pkg       *ast.PackageNode
	imprt     *ast.ImportNode
	msg       *ast.MessageNode
	msgDecl   ast.MessageElement
	msgDecls  []ast.MessageElement
	fld       *ast.FieldNode
	mapFld    *ast.MapFieldNode
	mapType   *ast.MapTypeNode
	grp       *ast.GroupNode
	oo        *ast.OneOfNode
	ooDecl    ast.OneOfElement
	ooDecls   []ast.OneOfElement
	ext       *ast.ExtensionRangeNode
	resvd     *ast.ReservedNode
	en        *ast.EnumNode
	enDecl    ast.EnumElement
	enDecls   []ast.EnumElement
	env       *ast.EnumValueNode
	extend    *ast.ExtendNode
	extDecl   ast.ExtendElement
	extDecls  []ast.ExtendElement
	svc       *ast.ServiceNode
	svcDecl   ast.ServiceElement
	svcDecls  []ast.ServiceElement
	mtd       *ast.RPCNode
	rpcType   *ast.RPCTypeNode
	rpcDecl   ast.RPCElement
	rpcDecls  []ast.RPCElement
	opt       *ast.OptionNode
	opts      *compactOptionList
	ref       *ast.FieldReferenceNode
	optNms    *fieldRefList
	cmpctOpts *ast.CompactOptionsNode
	rng       *ast.RangeNode
	rngs      *rangeList
	names     *nameList
	cid       *identList
	tid       ast.IdentValueNode
	sl        *valueList
	msgField  *ast.MessageFieldNode
	msgEntry  *messageFieldEntry
	msgLit    *messageFieldList
	v         ast.ValueNode
	il        ast.IntValueNode
	str       *stringList
	s         *ast.StringLiteralNode
	i         *ast.UintLiteralNode
	f         *ast.FloatLiteralNode
	id        *ast.IdentNode
	b         *ast.RuneNode
	err       error
}

// any non-terminal which returns a value needs a type, which is
// really a field name in the above union struct
%type <file>      file
%type <syn>       syntax
%type <fileDecl>  fileDecl
%type <fileDecls> fileDecls
%type <imprt>     import
%type <pkg>       package
%type <opt>       option compactOption
%type <opts>      compactOptionDecls
%type <rpcDecl>   rpcDecl
%type <rpcDecls>  rpcDecls
%type <ref>       optionNameComponent aggName
%type <optNms>    optionName
%type <cmpctOpts> compactOptions
%type <v>         constant scalarConstant aggregate numLit
%type <il>        intLit
%type <id>        name keyType msgElementName extElementName oneofElementName enumElementName
%type <cid>       ident msgElementIdent extElementIdent oneofElementIdent
%type <tid>       typeIdent msgElementTypeIdent extElementTypeIdent oneofElementTypeIdent
%type <sl>        constantList
%type <msgField>  aggFieldEntry
%type <msgEntry>  aggField
%type <msgLit>    aggFields
%type <fld>       oneofField msgField extField
%type <oo>        oneof
%type <grp>       group oneofGroup
%type <mapFld>    mapField
%type <mapType>   mapType
%type <msg>       message
%type <msgDecl>   messageDecl
%type <msgDecls>  messageDecls
%type <ooDecl>    ooDecl
%type <ooDecls>   ooDecls
%type <names>     fieldNames
%type <resvd>     msgReserved enumReserved reservedNames
%type <rng>       tagRange enumRange
%type <rngs>      tagRanges enumRanges
%type <ext>       extensions
%type <en>        enum
%type <enDecl>    enumDecl
%type <enDecls>   enumDecls
%type <env>       enumValue
%type <extend>    extend
%type <extDecl>   extendDecl
%type <extDecls>  extendDecls
%type <str>       stringLit
%type <svc>       service
%type <svcDecl>   serviceDecl
%type <svcDecls>  serviceDecls
%type <mtd>       rpc
%type <rpcType>   rpcType

// same for terminals
%token <s>   _STRING_LIT
%token <i>   _INT_LIT
%token <f>   _FLOAT_LIT
%token <id>  _NAME
%token <id>  _SYNTAX _IMPORT _WEAK _PUBLIC _PACKAGE _OPTION _TRUE _FALSE _INF _NAN _REPEATED _OPTIONAL _REQUIRED
%token <id>  _DOUBLE _FLOAT _INT32 _INT64 _UINT32 _UINT64 _SINT32 _SINT64 _FIXED32 _FIXED64 _SFIXED32 _SFIXED64
%token <id>  _BOOL _STRING _BYTES _GROUP _ONEOF _MAP _EXTENSIONS _TO _MAX _RESERVED _ENUM _MESSAGE _EXTEND
%token <id>  _SERVICE _RPC _STREAM _RETURNS
%token <err> _ERROR
// we define all of these, even ones that aren't used, to improve error messages
// so it shows the unexpected symbol instead of showing "$unk"
%token <b>   '=' ';' ':' '{' '}' '\\' '/' '?' '.' ',' '>' '<' '+' '-' '(' ')' '[' ']' '*' '&' '^' '%' '$' '#' '@' '!' '~' '`'

%%

file : syntax {
		$$ = ast.NewFileNode($1, nil)
		protolex.(*protoLex).res = $$
	}
	| fileDecls  {
		$$ = ast.NewFileNode(nil, $1)
		protolex.(*protoLex).res = $$
	}
	| syntax fileDecls {
		$$ = ast.NewFileNode($1, $2)
		protolex.(*protoLex).res = $$
	}
	| {
	}

fileDecls : fileDecls fileDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| fileDecl {
		if $1 != nil {
			$$ = []ast.FileElement{$1}
		} else {
			$$ = nil
		}
	}

fileDecl : import {
		$$ = $1
	}
	| package {
		$$ = $1
	}
	| option {
		$$ = $1
	}
	| message {
		$$ = $1
	}
	| enum {
		$$ = $1
	}
	| extend {
		$$ = $1
	}
	| service {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

syntax : _SYNTAX '=' stringLit ';' {
		$$ = ast.NewSyntaxNode($1.ToKeyword(), $2, $3.toStringValueNode(), $4)
	}

import : _IMPORT stringLit ';' {
		$$ = ast.NewImportNode($1.ToKeyword(), nil, nil, $2.toStringValueNode(), $3)
	}
	| _IMPORT _WEAK stringLit ';' {
		$$ = ast.NewImportNode($1.ToKeyword(), nil, $2.ToKeyword(), $3.toStringValueNode(), $4)
	}
	| _IMPORT _PUBLIC stringLit ';' {
		$$ = ast.NewImportNode($1.ToKeyword(), $2.ToKeyword(), nil, $3.toStringValueNode(), $4)
	}

package : _PACKAGE ident ';' {
		$$ = ast.NewPackageNode($1.ToKeyword(), $2.toIdentValueNode(nil), $3)
	}

ident : name {
		$$ = &identList{$1, nil, nil}
	}
	| name '.' ident {
		$$ = &identList{$1, $2, $3}
	}

// to mimic limitations of protoc recursive-descent parser,
// we don't allowed message statement keywords as identifiers
// (or oneof statement keywords [e.g. "option"] below)

msgElementIdent : msgElementName {
		$$ = &identList{$1, nil, nil}
	}
	| msgElementName '.' ident {
		$$ = &identList{$1, $2, $3}
	}

extElementIdent : extElementName {
		$$ = &identList{$1, nil, nil}
	}
	| extElementName '.' ident {
		$$ = &identList{$1, $2, $3}
	}

oneofElementIdent : oneofElementName {
		$$ = &identList{$1, nil, nil}
	}
	| oneofElementName '.' ident {
		$$ = &identList{$1, $2, $3}
	}

option : _OPTION optionName '=' constant ';' {
		refs, dots := $2.toNodes()
		optName := ast.NewOptionNameNode(refs, dots)
		$$ = ast.NewOptionNode($1.ToKeyword(), optName, $3, $4, $5)
	}

optionName : optionNameComponent {
		$$ = &fieldRefList{$1, nil, nil}
	}
	| optionNameComponent '.' optionName {
		$$ = &fieldRefList{$1, $2, $3}
	}

optionNameComponent : name {
		$$ = ast.NewFieldReferenceNode($1)
	}
	| '(' typeIdent ')' {
		$$ = ast.NewExtensionFieldReferenceNode($1, $2, $3)
	}

constant : scalarConstant
	| aggregate

scalarConstant : stringLit {
		$$ = $1.toStringValueNode()
	}
	| numLit
	| name {
		if $1.Val == "true" || $1.Val == "false" {
			$$ = ast.NewBoolLiteralNode($1.ToKeyword())
		} else if $1.Val == "inf" || $1.Val == "nan" {
			$$ = ast.NewSpecialFloatLiteralNode($1.ToKeyword())
		} else {
			$$ = $1
		}
	}

numLit : _FLOAT_LIT {
		$$ = $1
	}
	| '-' _FLOAT_LIT {
		$$ = ast.NewSignedFloatLiteralNode($1, $2)
	}
	| '+' _FLOAT_LIT {
		$$ = ast.NewSignedFloatLiteralNode($1, $2)
	}
	| '+' _INF {
		f := ast.NewSpecialFloatLiteralNode($2.ToKeyword())
		$$ = ast.NewSignedFloatLiteralNode($1, f)
	}
	| '-' _INF {
		f := ast.NewSpecialFloatLiteralNode($2.ToKeyword())
		$$ = ast.NewSignedFloatLiteralNode($1, f)
	}
	| _INT_LIT {
		$$ = $1
	}
	| '+' _INT_LIT {
		$$ = ast.NewPositiveUintLiteralNode($1, $2)
	}
	| '-' _INT_LIT {
		if $2.Val > math.MaxInt64 + 1 {
			// can't represent as int so treat as float literal
			$$ = ast.NewSignedFloatLiteralNode($1, $2)
		} else {
			$$ = ast.NewNegativeIntLiteralNode($1, $2)
		}
	}

stringLit : _STRING_LIT {
		$$ = &stringList{$1, nil}
	}
	| _STRING_LIT stringLit  {
		$$ = &stringList{$1, $2}
	}

aggregate : '{' aggFields '}' {
		fields, delims := $2.toNodes()
		$$ = ast.NewMessageLiteralNode($1, fields, delims, $3)
	}

aggFields : aggField {
		if $1 != nil {
			$$ = &messageFieldList{$1, nil}
		} else {
			$$ = nil
		}
	}
	| aggField aggFields {
		if $1 != nil {
			$$ = &messageFieldList{$1, $2}
		} else {
			$$ = $2
		}
	}
	| {
		$$ = nil
	}

aggField : aggFieldEntry {
		if $1 != nil {
			$$ = &messageFieldEntry{$1, nil}
		} else {
			$$ = nil
		}
	}
	| aggFieldEntry ',' {
		if $1 != nil {
			$$ = &messageFieldEntry{$1, $2}
		} else {
			$$ = nil
		}
	}
	| aggFieldEntry ';' {
		if $1 != nil {
			$$ = &messageFieldEntry{$1, $2}
		} else {
			$$ = nil
		}
	}
	| error ',' {
		$$ = nil
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

aggFieldEntry : aggName ':' scalarConstant {
		if $1 != nil {
			$$ = ast.NewMessageFieldNode($1, $2, $3)
		} else {
			$$ = nil
		}
	}
	| aggName ':' '[' ']' {
		if $1 != nil {
			val := ast.NewArrayLiteralNode($3, nil, nil, $4)
			$$ = ast.NewMessageFieldNode($1, $2, val)
		} else {
			$$ = nil
		}
	}
	| aggName ':' '[' constantList ']' {
		if $1 != nil {
			vals, commas := $4.toNodes()
			val := ast.NewArrayLiteralNode($3, vals, commas, $5)
			$$ = ast.NewMessageFieldNode($1, $2, val)
		} else {
			$$ = nil
		}
	}
	| aggName ':' '[' error ']' {
		$$ = nil
	}
	| aggName ':' aggregate {
		if $1 != nil {
			$$ = ast.NewMessageFieldNode($1, $2, $3)
		} else {
			$$ = nil
		}
	}
	| aggName aggregate {
		if $1 != nil {
			$$ = ast.NewMessageFieldNode($1, nil, $2)
		} else {
			$$ = nil
		}
	}
	| aggName ':' '<' aggFields '>' {
		if $1 != nil {
			fields, delims := $4.toNodes()
			msg := ast.NewMessageLiteralNode($3, fields, delims, $5)
			$$ = ast.NewMessageFieldNode($1, $2, msg)
		} else {
			$$ = nil
		}
	}
	| aggName '<' aggFields '>' {
		if $1 != nil {
			fields, delims := $3.toNodes()
			msg := ast.NewMessageLiteralNode($2, fields, delims, $4)
			$$ = ast.NewMessageFieldNode($1, nil, msg)
		} else {
			$$ = nil
		}
	}
	| aggName ':' '<' error '>' {
		$$ = nil
	}
	| aggName '<' error '>' {
		$$ = nil
	}

aggName : name {
		$$ = ast.NewFieldReferenceNode($1)
	}
	| '[' typeIdent ']' {
		$$ = ast.NewExtensionFieldReferenceNode($1, $2, $3)
	}
	| '[' error ']' {
		$$ = nil
	}

constantList : constant {
		$$ = &valueList{$1, nil, nil}
	}
	| constant ',' constantList {
		$$ = &valueList{$1, $2, $3}
	}
	| '<' aggFields '>' {
		fields, delims := $2.toNodes()
		msg := ast.NewMessageLiteralNode($1, fields, delims, $3)
		$$ = &valueList{msg, nil, nil}
	}
	| '<' aggFields '>' ',' constantList {
		fields, delims := $2.toNodes()
		msg := ast.NewMessageLiteralNode($1, fields, delims, $3)
		$$ = &valueList{msg, $4, $5}
	}
	| '<' error '>' {
		$$ = nil
	}
	| '<' error '>' ',' constantList {
		$$ = $5
	}

typeIdent : ident {
		$$ = $1.toIdentValueNode(nil)
	}
	| '.' ident {
		$$ = $2.toIdentValueNode($1)
	}

msgElementTypeIdent : msgElementIdent {
		$$ = $1.toIdentValueNode(nil)
	}
	| '.' ident {
		$$ = $2.toIdentValueNode($1)
	}

extElementTypeIdent : extElementIdent {
		$$ = $1.toIdentValueNode(nil)
	}
	| '.' ident {
		$$ = $2.toIdentValueNode($1)
	}

oneofElementTypeIdent : oneofElementIdent {
		$$ = $1.toIdentValueNode(nil)
	}
	| '.' ident {
		$$ = $2.toIdentValueNode($1)
	}

msgField : _REQUIRED typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _OPTIONAL typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _REPEATED typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _REQUIRED typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| _OPTIONAL typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| _REPEATED typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| msgElementTypeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, nil, $5)
	}
	| msgElementTypeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, $5, $6)
	}

extField : _REQUIRED typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _OPTIONAL typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _REPEATED typeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, nil, $6)
	}
	| _REQUIRED typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| _OPTIONAL typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| _REPEATED typeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode($1.ToKeyword(), $2, $3, $4, $5, $6, $7)
	}
	| extElementTypeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, nil, $5)
	}
	| extElementTypeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, $5, $6)
	}

compactOptions: '[' compactOptionDecls ']' {
		opts, commas := $2.toNodes()
		$$ = ast.NewCompactOptionsNode($1, opts, commas, $3)
	}

compactOptionDecls : compactOption {
		$$ = &compactOptionList{$1, nil, nil}
	}
	| compactOption ',' compactOptionDecls {
		$$ = &compactOptionList{$1, $2, $3}
	}

compactOption: optionName '=' constant {
		refs, dots := $1.toNodes()
		optName := ast.NewOptionNameNode(refs, dots)
		$$ = ast.NewCompactOptionNode(optName, $2, $3)
	}

group : _REQUIRED _GROUP name '=' _INT_LIT '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, nil, $6, $7, $8)
	}
	| _OPTIONAL _GROUP name '=' _INT_LIT '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, nil, $6, $7, $8)
	}
	| _REPEATED _GROUP name '=' _INT_LIT '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, nil, $6, $7, $8)
	}
	| _REQUIRED _GROUP name '=' _INT_LIT compactOptions '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, $6, $7, $8, $9)
	}
	| _OPTIONAL _GROUP name '=' _INT_LIT compactOptions '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, $6, $7, $8, $9)
	}
	| _REPEATED _GROUP name '=' _INT_LIT compactOptions '{' messageDecls '}' {
		$$ = ast.NewGroupNode($1.ToKeyword(), $2.ToKeyword(), $3, $4, $5, $6, $7, $8, $9)
	}

oneof : _ONEOF name '{' ooDecls '}' {
		$$ = ast.NewOneOfNode($1.ToKeyword(), $2, $3, $4, $5)
	}

ooDecls : ooDecls ooDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| ooDecl {
		if $1 != nil {
			$$ = []ast.OneOfElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

ooDecl : option {
		$$ = $1
	}
	| oneofField {
		$$ = $1
	}
	| oneofGroup {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

oneofField : oneofElementTypeIdent name '=' _INT_LIT ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, nil, $5)
	}
	| oneofElementTypeIdent name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewFieldNode(nil, $1, $2, $3, $4, $5, $6)
	}

oneofGroup : _GROUP name '=' _INT_LIT '{' messageDecls '}' {
		$$ = ast.NewGroupNode(nil, $1.ToKeyword(), $2, $3, $4, nil, $5, $6, $7)
	}
	| _GROUP name '=' _INT_LIT compactOptions '{' messageDecls '}' {
		$$ = ast.NewGroupNode(nil, $1.ToKeyword(), $2, $3, $4, $5, $6, $7, $8)
	}

mapField : mapType name '=' _INT_LIT ';' {
		$$ = ast.NewMapFieldNode($1, $2, $3, $4, nil, $5)
	}
	| mapType name '=' _INT_LIT compactOptions ';' {
		$$ = ast.NewMapFieldNode($1, $2, $3, $4, $5, $6)
	}

mapType : _MAP '<' keyType ',' typeIdent '>' {
		$$ = ast.NewMapTypeNode($1.ToKeyword(), $2, $3, $4, $5, $6)
	}

keyType : _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING

extensions : _EXTENSIONS tagRanges ';' {
		ranges, commas := $2.toNodes()
		$$ = ast.NewExtensionRangeNode($1.ToKeyword(), ranges, commas, nil, $3)
	}
	| _EXTENSIONS tagRanges compactOptions ';' {
		ranges, commas := $2.toNodes()
		$$ = ast.NewExtensionRangeNode($1.ToKeyword(), ranges, commas, $3, $4)
	}

tagRanges : tagRange {
		$$ = &rangeList{$1, nil, nil}
	}
	| tagRange ',' tagRanges {
		$$ = &rangeList{$1, $2, $3}
	}

tagRange : _INT_LIT {
		$$ = ast.NewRangeNode($1, nil, nil, nil)
	}
	| _INT_LIT _TO _INT_LIT {
		$$ = ast.NewRangeNode($1, $2.ToKeyword(), $3, nil)
	}
	| _INT_LIT _TO _MAX {
		$$ = ast.NewRangeNode($1, $2.ToKeyword(), nil, $3.ToKeyword())
	}

enumRanges : enumRange {
		$$ = &rangeList{$1, nil, nil}
	}
	| enumRange ',' enumRanges {
		$$ = &rangeList{$1, $2, $3}
	}

enumRange : intLit {
		$$ = ast.NewRangeNode($1, nil, nil, nil)
	}
	| intLit _TO intLit {
		$$ = ast.NewRangeNode($1, $2.ToKeyword(), $3, nil)
	}
	| intLit _TO _MAX {
		$$ = ast.NewRangeNode($1, $2.ToKeyword(), nil, $3.ToKeyword())
	}

intLit : _INT_LIT {
		$$ = $1
	}
	| '-' _INT_LIT {
		$$ = ast.NewNegativeIntLiteralNode($1, $2)
	}

msgReserved : _RESERVED tagRanges ';' {
		ranges, commas := $2.toNodes()
		$$ = ast.NewReservedRangesNode($1.ToKeyword(), ranges, commas, $3)
	}
	| reservedNames

enumReserved : _RESERVED enumRanges ';' {
		ranges, commas := $2.toNodes()
		$$ = ast.NewReservedRangesNode($1.ToKeyword(), ranges, commas, $3)
	}
	| reservedNames

reservedNames : _RESERVED fieldNames ';' {
		names, commas := $2.toNodes()
		$$ = ast.NewReservedNamesNode($1.ToKeyword(), names, commas, $3)
	}

fieldNames : stringLit {
		$$ = &nameList{$1.toStringValueNode(), nil, nil}
	}
	| stringLit ',' fieldNames {
		$$ = &nameList{$1.toStringValueNode(), $2, $3}
	}

enum : _ENUM name '{' enumDecls '}' {
		$$ = ast.NewEnumNode($1.ToKeyword(), $2, $3, $4, $5)
	}

enumDecls : enumDecls enumDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| enumDecl {
		if $1 != nil {
			$$ = []ast.EnumElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

enumDecl : option {
		$$ = $1
	}
	| enumValue {
		$$ = $1
	}
	| enumReserved {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

enumValue : enumElementName '=' intLit ';' {
		$$ = ast.NewEnumValueNode($1, $2, $3, nil, $4)
	}
	|  enumElementName '=' intLit compactOptions ';' {
		$$ = ast.NewEnumValueNode($1, $2, $3, $4, $5)
	}

message : _MESSAGE name '{' messageDecls '}' {
		$$ = ast.NewMessageNode($1.ToKeyword(), $2, $3, $4, $5)
	}

messageDecls : messageDecls messageDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| messageDecl {
		if $1 != nil {
			$$ = []ast.MessageElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

messageDecl : msgField {
		$$ = $1
	}
	| enum {
		$$ = $1
	}
	| message {
		$$ = $1
	}
	| extend {
		$$ = $1
	}
	| extensions {
		$$ = $1
	}
	| group {
		$$ = $1
	}
	| option {
		$$ = $1
	}
	| oneof {
		$$ = $1
	}
	| mapField {
		$$ = $1
	}
	| msgReserved {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

extend : _EXTEND typeIdent '{' extendDecls '}' {
		$$ = ast.NewExtendNode($1.ToKeyword(), $2, $3, $4, $5)
	}

extendDecls : extendDecls extendDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| extendDecl {
		if $1 != nil {
			$$ = []ast.ExtendElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

extendDecl : extField {
		$$ = $1
	}
	| group {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

service : _SERVICE name '{' serviceDecls '}' {
		$$ = ast.NewServiceNode($1.ToKeyword(), $2, $3, $4, $5)
	}

serviceDecls : serviceDecls serviceDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| serviceDecl {
		if $1 != nil {
			$$ = []ast.ServiceElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

// NB: doc suggests support for "stream" declaration, separate from "rpc", but
// it does not appear to be supported in protoc (doc is likely from grammar for
// Google-internal version of protoc, with support for streaming stubby)
serviceDecl : option {
		$$ = $1
	}
	| rpc {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

rpc : _RPC name rpcType _RETURNS rpcType ';' {
		$$ = ast.NewRPCNode($1.ToKeyword(), $2, $3, $4.ToKeyword(), $5, $6)
	}
	| _RPC name rpcType _RETURNS rpcType '{' rpcDecls '}' {
		$$ = ast.NewRPCNodeWithBody($1.ToKeyword(), $2, $3, $4.ToKeyword(), $5, $6, $7, $8)
	}

rpcType : '(' _STREAM typeIdent ')' {
		$$ = ast.NewRPCTypeNode($1, $2.ToKeyword(), $3, $4)
	}
	| '(' typeIdent ')' {
		$$ = ast.NewRPCTypeNode($1, nil, $2, $3)
	}

rpcDecls : rpcDecls rpcDecl {
		if $2 != nil {
			$$ = append($1, $2)
		} else {
			$$ = $1
		}
	}
	| rpcDecl {
		if $1 != nil {
			$$ = []ast.RPCElement{$1}
		} else {
			$$ = nil
		}
	}
	| {
		$$ = nil
	}

rpcDecl : option {
		$$ = $1
	}
	| ';' {
		$$ = ast.NewEmptyDeclNode($1)
	}
	| error ';' {
		$$ = nil
	}
	| error {
		$$ = nil
	}

// excludes message, enum, oneof, extensions, reserved, extend,
//   option, optional, required, and repeated
msgElementName : _NAME
	| _SYNTAX
	| _IMPORT
	| _WEAK
	| _PUBLIC
	| _PACKAGE
	| _TRUE
	| _FALSE
	| _INF
	| _NAN
	| _DOUBLE
	| _FLOAT
	| _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING
	| _BYTES
	| _GROUP
	| _MAP
	| _TO
	| _MAX
	| _SERVICE
	| _RPC
	| _STREAM
	| _RETURNS

// excludes optional, required, and repeated
extElementName : _NAME
	| _SYNTAX
	| _IMPORT
	| _WEAK
	| _PUBLIC
	| _PACKAGE
	| _OPTION
	| _TRUE
	| _FALSE
	| _INF
	| _NAN
	| _DOUBLE
	| _FLOAT
	| _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING
	| _BYTES
	| _GROUP
	| _ONEOF
	| _MAP
	| _EXTENSIONS
	| _TO
	| _MAX
	| _RESERVED
	| _ENUM
	| _MESSAGE
	| _EXTEND
	| _SERVICE
	| _RPC
	| _STREAM
	| _RETURNS

// excludes reserved, option
enumElementName : _NAME
	| _SYNTAX
	| _IMPORT
	| _WEAK
	| _PUBLIC
	| _PACKAGE
	| _TRUE
	| _FALSE
	| _INF
	| _NAN
	| _REPEATED
	| _OPTIONAL
	| _REQUIRED
	| _DOUBLE
	| _FLOAT
	| _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING
	| _BYTES
	| _GROUP
	| _ONEOF
	| _MAP
	| _EXTENSIONS
	| _TO
	| _MAX
	| _ENUM
	| _MESSAGE
	| _EXTEND
	| _SERVICE
	| _RPC
	| _STREAM
	| _RETURNS

// excludes option, optional, required, and repeated
oneofElementName : _NAME
	| _SYNTAX
	| _IMPORT
	| _WEAK
	| _PUBLIC
	| _PACKAGE
	| _TRUE
	| _FALSE
	| _INF
	| _NAN
	| _DOUBLE
	| _FLOAT
	| _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING
	| _BYTES
	| _GROUP
	| _ONEOF
	| _MAP
	| _EXTENSIONS
	| _TO
	| _MAX
	| _RESERVED
	| _ENUM
	| _MESSAGE
	| _EXTEND
	| _SERVICE
	| _RPC
	| _STREAM
	| _RETURNS

name : _NAME
	| _SYNTAX
	| _IMPORT
	| _WEAK
	| _PUBLIC
	| _PACKAGE
	| _OPTION
	| _TRUE
	| _FALSE
	| _INF
	| _NAN
	| _REPEATED
	| _OPTIONAL
	| _REQUIRED
	| _DOUBLE
	| _FLOAT
	| _INT32
	| _INT64
	| _UINT32
	| _UINT64
	| _SINT32
	| _SINT64
	| _FIXED32
	| _FIXED64
	| _SFIXED32
	| _SFIXED64
	| _BOOL
	| _STRING
	| _BYTES
	| _GROUP
	| _ONEOF
	| _MAP
	| _EXTENSIONS
	| _TO
	| _MAX
	| _RESERVED
	| _ENUM
	| _MESSAGE
	| _EXTEND
	| _SERVICE
	| _RPC
	| _STREAM
	| _RETURNS

%%
