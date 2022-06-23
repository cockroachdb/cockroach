package decorator

import "github.com/dave/dst"

func getDecorationInfo(n dst.Node) (before, after dst.SpaceType, info []decorationInfo) {
	switch n := n.(type) {
	case *dst.ArrayType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Lbrack", n.Decs.Lbrack})
		info = append(info, decorationInfo{"Len", n.Decs.Len})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.AssignStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Tok", n.Decs.Tok})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BadDecl:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BadExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BadStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BasicLit:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BinaryExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"Op", n.Decs.Op})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BlockStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Lbrace", n.Decs.Lbrace})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.BranchStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Tok", n.Decs.Tok})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.CallExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Fun", n.Decs.Fun})
		info = append(info, decorationInfo{"Lparen", n.Decs.Lparen})
		info = append(info, decorationInfo{"Ellipsis", n.Decs.Ellipsis})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.CaseClause:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Case", n.Decs.Case})
		info = append(info, decorationInfo{"Colon", n.Decs.Colon})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ChanType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Begin", n.Decs.Begin})
		info = append(info, decorationInfo{"Arrow", n.Decs.Arrow})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.CommClause:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Case", n.Decs.Case})
		info = append(info, decorationInfo{"Comm", n.Decs.Comm})
		info = append(info, decorationInfo{"Colon", n.Decs.Colon})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.CompositeLit:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Type", n.Decs.Type})
		info = append(info, decorationInfo{"Lbrace", n.Decs.Lbrace})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.DeclStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.DeferStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Defer", n.Decs.Defer})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.Ellipsis:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Ellipsis", n.Decs.Ellipsis})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.EmptyStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ExprStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.Field:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Type", n.Decs.Type})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.FieldList:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Opening", n.Decs.Opening})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.File:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Package", n.Decs.Package})
		info = append(info, decorationInfo{"Name", n.Decs.Name})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ForStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"For", n.Decs.For})
		info = append(info, decorationInfo{"Init", n.Decs.Init})
		info = append(info, decorationInfo{"Cond", n.Decs.Cond})
		info = append(info, decorationInfo{"Post", n.Decs.Post})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.FuncDecl:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Func", n.Decs.Func})
		info = append(info, decorationInfo{"Recv", n.Decs.Recv})
		info = append(info, decorationInfo{"Name", n.Decs.Name})
		info = append(info, decorationInfo{"Params", n.Decs.Params})
		info = append(info, decorationInfo{"Results", n.Decs.Results})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.FuncLit:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Type", n.Decs.Type})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.FuncType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Func", n.Decs.Func})
		info = append(info, decorationInfo{"Params", n.Decs.Params})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.GenDecl:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Tok", n.Decs.Tok})
		info = append(info, decorationInfo{"Lparen", n.Decs.Lparen})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.GoStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Go", n.Decs.Go})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.Ident:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.IfStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"If", n.Decs.If})
		info = append(info, decorationInfo{"Init", n.Decs.Init})
		info = append(info, decorationInfo{"Cond", n.Decs.Cond})
		info = append(info, decorationInfo{"Else", n.Decs.Else})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ImportSpec:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Name", n.Decs.Name})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.IncDecStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.IndexExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"Lbrack", n.Decs.Lbrack})
		info = append(info, decorationInfo{"Index", n.Decs.Index})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.InterfaceType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Interface", n.Decs.Interface})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.KeyValueExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Key", n.Decs.Key})
		info = append(info, decorationInfo{"Colon", n.Decs.Colon})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.LabeledStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Label", n.Decs.Label})
		info = append(info, decorationInfo{"Colon", n.Decs.Colon})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.MapType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Map", n.Decs.Map})
		info = append(info, decorationInfo{"Key", n.Decs.Key})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.Package:
	case *dst.ParenExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Lparen", n.Decs.Lparen})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.RangeStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"For", n.Decs.For})
		info = append(info, decorationInfo{"Key", n.Decs.Key})
		info = append(info, decorationInfo{"Value", n.Decs.Value})
		info = append(info, decorationInfo{"Range", n.Decs.Range})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ReturnStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Return", n.Decs.Return})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.SelectStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Select", n.Decs.Select})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.SelectorExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.SendStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Chan", n.Decs.Chan})
		info = append(info, decorationInfo{"Arrow", n.Decs.Arrow})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.SliceExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"Lbrack", n.Decs.Lbrack})
		info = append(info, decorationInfo{"Low", n.Decs.Low})
		info = append(info, decorationInfo{"High", n.Decs.High})
		info = append(info, decorationInfo{"Max", n.Decs.Max})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.StarExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Star", n.Decs.Star})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.StructType:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Struct", n.Decs.Struct})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.SwitchStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Switch", n.Decs.Switch})
		info = append(info, decorationInfo{"Init", n.Decs.Init})
		info = append(info, decorationInfo{"Tag", n.Decs.Tag})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.TypeAssertExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"X", n.Decs.X})
		info = append(info, decorationInfo{"Lparen", n.Decs.Lparen})
		info = append(info, decorationInfo{"Type", n.Decs.Type})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.TypeSpec:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Name", n.Decs.Name})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.TypeSwitchStmt:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Switch", n.Decs.Switch})
		info = append(info, decorationInfo{"Init", n.Decs.Init})
		info = append(info, decorationInfo{"Assign", n.Decs.Assign})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.UnaryExpr:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Op", n.Decs.Op})
		info = append(info, decorationInfo{"End", n.Decs.End})
	case *dst.ValueSpec:
		before = n.Decs.Before
		after = n.Decs.After
		info = append(info, decorationInfo{"Start", n.Decs.Start})
		info = append(info, decorationInfo{"Assign", n.Decs.Assign})
		info = append(info, decorationInfo{"End", n.Decs.End})
	}
	return
}
