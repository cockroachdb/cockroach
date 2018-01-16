package xform

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func TestLogicalScanProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()

	// (Scan a)
	factory := newFactory(gen.mem, 0)
	scanGroup := factory.ConstructScan(factory.InternPrivate(a))

	expected := "columns: a.x:1* a.y:2\n"
	testLogicalProps(t, gen.mem, scanGroup, expected)
}

func TestLogicalSelectProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()

	// (Select (Scan a) (True))
	factory := newFactory(gen.mem, 0)
	scanGroup := factory.ConstructScan(factory.InternPrivate(a))
	filterGroup := factory.ConstructTrue()
	selGroup := factory.ConstructSelect(scanGroup, filterGroup)

	expected := "columns: a.x:1* a.y:2\n"
	testLogicalProps(t, gen.mem, selGroup, expected)
}

func TestLogicalProjectProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	b := gen.addTableB()

	// (Project (Scan b) (Projections [(Variable b.y) (False)]))
	factory := newFactory(gen.mem, 0)
	scanGroup := factory.ConstructScan(factory.InternPrivate(b))

	col1 := factory.mem.metadata.TableColumn(b, 1)
	varGroup := factory.ConstructVariable(factory.InternPrivate(col1))
	col2 := gen.mem.metadata.AddColumn("false")
	items := factory.StoreList([]GroupID{varGroup, factory.ConstructFalse()})
	cols := util.MakeFastIntSet(int(col1), int(col2))
	projectionsGroup := factory.ConstructProjections(items, factory.InternPrivate(&cols))

	projectGroup := factory.ConstructProject(scanGroup, projectionsGroup)

	expected := "columns: b.z:2* false:3\n"
	testLogicalProps(t, gen.mem, projectGroup, expected)

	// Make sure that not null columns are subset of output columns.
	notNullCols := gen.mem.lookupGroup(projectGroup).logical.Relational.NotNullCols
	if notNullCols.String() != "(2)" {
		t.Fatalf("unexpected not null cols: %s", notNullCols.String())
	}
}

func TestLogicalJoinProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()
	b := gen.addTableB()

	joinFunc := func(op Operator, expected string) {
		// (Join (Scan a) (Scan b) (True))
		factory := newFactory(gen.mem, 0)
		leftGroup := factory.ConstructScan(factory.InternPrivate(a))
		rightGroup := factory.ConstructScan(factory.InternPrivate(b))
		onGroup := factory.ConstructTrue()
		joinGroup := factory.DynamicConstruct(op, []GroupID{leftGroup, rightGroup, onGroup}, 0)

		testLogicalProps(t, gen.mem, joinGroup, expected)
	}

	joinFunc(InnerJoinOp, "columns: a.x:1* a.y:2 b.x:3* b.z:4*\n")
	joinFunc(InnerJoinApplyOp, "columns: a.x:1* a.y:2 b.x:3* b.z:4*\n")
	joinFunc(LeftJoinOp, "columns: a.x:1* a.y:2 b.x:3 b.z:4\n")
	joinFunc(LeftJoinApplyOp, "columns: a.x:1* a.y:2 b.x:3 b.z:4\n")
	joinFunc(RightJoinOp, "columns: a.x:1 a.y:2 b.x:3* b.z:4*\n")
	joinFunc(RightJoinApplyOp, "columns: a.x:1 a.y:2 b.x:3* b.z:4*\n")
	joinFunc(FullJoinOp, "columns: a.x:1 a.y:2 b.x:3 b.z:4\n")
	joinFunc(FullJoinApplyOp, "columns: a.x:1 a.y:2 b.x:3 b.z:4\n")
	joinFunc(SemiJoinOp, "columns: a.x:1* a.y:2\n")
	joinFunc(SemiJoinApplyOp, "columns: a.x:1* a.y:2\n")
	joinFunc(AntiJoinOp, "columns: a.x:1* a.y:2\n")
	joinFunc(AntiJoinApplyOp, "columns: a.x:1* a.y:2\n")
}

func TestLogicalGroupByProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()

	// (GroupBy (Scan a) (Projections [(Variable a.y)]) (Projections [(False)]))
	factory := newFactory(gen.mem, 0)
	scanGroup := factory.ConstructScan(factory.InternPrivate(a))

	col1 := factory.mem.metadata.TableColumn(a, 1)
	varGroup := factory.ConstructVariable(factory.InternPrivate(col1))
	items1 := factory.StoreList([]GroupID{varGroup})
	cols1 := util.MakeFastIntSet(int(col1))
	groupingsGroup := factory.ConstructProjections(items1, factory.InternPrivate(&cols1))

	col2 := gen.mem.metadata.AddColumn("false")
	items2 := factory.StoreList([]GroupID{factory.ConstructFalse()})
	cols2 := util.MakeFastIntSet(int(col2))
	aggsGroup := factory.ConstructProjections(items2, factory.InternPrivate(&cols2))

	groupByGroup := factory.ConstructGroupBy(scanGroup, groupingsGroup, aggsGroup)

	expected := "columns: a.y:2 false:3\n"
	testLogicalProps(t, gen.mem, groupByGroup, expected)
}

func TestLogicalSetProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()
	b := gen.addTableB()

	// (Union (Scan b) (Scan a))
	factory := newFactory(gen.mem, 0)
	leftGroup := factory.ConstructScan(factory.InternPrivate(b))
	rightGroup := factory.ConstructScan(factory.InternPrivate(a))

	a0 := factory.mem.metadata.TableColumn(a, 0)
	a1 := factory.mem.metadata.TableColumn(a, 1)
	b0 := factory.mem.metadata.TableColumn(b, 0)
	b1 := factory.mem.metadata.TableColumn(b, 1)
	colMap := factory.InternPrivate(&ColMap{b0: a1, b1: a0})

	unionGroup := factory.ConstructUnion(leftGroup, rightGroup, colMap)

	expected := "columns: b.x:3 b.z:4*\n"
	testLogicalProps(t, gen.mem, unionGroup, expected)
}

func TestLogicalValuesProps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	gen := newTestMetadataGen()
	a := gen.addTableA()

	// (Values)
	factory := newFactory(gen.mem, 0)
	rows := factory.StoreList(nil)
	a0 := factory.mem.metadata.TableColumn(a, 0)
	a1 := factory.mem.metadata.TableColumn(a, 1)
	cols := util.MakeFastIntSet(int(a0), int(a1))
	valuesGroup := factory.ConstructValues(rows, factory.InternPrivate(&cols))

	expected := "columns: a.x:1 a.y:2\n"
	testLogicalProps(t, gen.mem, valuesGroup, expected)
}

func testLogicalProps(t *testing.T, mem *memo, group GroupID, expected string) {
	t.Helper()

	tp := treeprinter.New()
	mem.lookupGroup(group).logical.format(mem, tp)
	actual := tp.String()

	if actual != expected {
		t.Fatalf("\nexpected: %sactual  : %s", expected, actual)
	}
}
