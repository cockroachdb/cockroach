package opgen

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/eav"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/errors"
)

func NewRegistry() *Registry {
	return &Registry{}
}

type Registry struct {
	targets []target
}

func (r *Registry) BuildGraph(initial scpb.State) (*scgraph.Graph, error) {
	g, err := scgraph.New(initial)
	if err != nil {
		return nil, err
	}
	tr := eav.NewTree(scpb.AttrSchema(), [][]eav.Attribute{
		{scpb.AttrElementType, scpb.AttrDirection},
	})
	for _, n := range initial {
		tr.Insert(n)
	}
	v := eav.GetValues()
	defer v.Release()
	for _, t := range r.targets {
		v.Set(scpb.AttrElementType, scpb.GetElementType(t.element))
		v.Set(scpb.AttrDirection, (*eav.Int32)(&t.dir))
		_ = tr.Iterate(v, eav.IteratorFunc(func(container eav.Entity) error {
			n := container.(*scpb.Node)
			var in bool
			for _, op := range t.ops {
				if in = in || op.From == n.Status; !in {
					g.AddOpEdges(n.Target, op.From, op.From, true)
				} else {
					if op.To == scpb.Status_UNKNOWN {
						panic(errors.Errorf("here %T %s", n.Target.GetElement(), op.From))
					}
					g.AddOpEdges(n.Target, op.From, op.To, op.Revertible, op.Ops(n.GetElement())...)
				}
			}
			return nil
		}))
	}
	return g, nil
}

func (r *Registry) Register(
	e scpb.Element, dir scpb.Target_Direction, initialStatus scpb.Status, specs ...TransitionSpec,
) {
	target, err := makeTarget(e, dir, initialStatus, specs...)
	if err != nil {
		panic(errors.Wrapf(err, "generating ops for %T:%s", e, dir))
	}
	r.targets = append(r.targets, target)
}

// target represents a set of transitions for a target.
type target struct {
	element scpb.Element
	dir     scpb.Target_Direction
	ops     []ops
}

func (t *target) Sequence() []scpb.Status {
	seq := make([]scpb.Status, len(t.ops)+1)
	seq[0] = t.ops[0].From
	for i, o := range t.ops {
		seq[i+1] = o.To
	}
	return seq
}

type ops struct {
	From, To   scpb.Status
	Revertible bool
	Ops        func(element scpb.Element) []scop.Op
}

func makeTarget(
	e scpb.Element, dir scpb.Target_Direction, initialStatus scpb.Status, specs ...TransitionSpec,
) (target, error) {
	transitions := make([]ops, len(specs))
	from := func(i int) scpb.Status {
		if i == 0 {
			return initialStatus
		}
		return transitions[i-1].To
	}
	for i, s := range specs {
		var err error
		transitions[i], err = s.generate(e, from(i))
		if err != nil {
			return target{}, err
		}
	}
	return target{
		element: e,
		dir:     dir,
		ops:     transitions,
	}, nil
}

func To(to scpb.Status, properties ...Property) TransitionSpec {
	ts := transitionSpec{
		to:         to,
		revertible: false,
	}
	for _, p := range properties {
		p.apply(&ts)
	}
	return &ts
}

func Revertible(b bool) Property {
	return revertible(b)
}

func Phase(p scop.Phase) Property {
	return phase(p)
}

type phase scop.Phase

func (p phase) apply(spec *transitionSpec) {
	spec.phase = scop.Phase(p)
}

type revertible bool

func (r revertible) apply(spec *transitionSpec) {
	spec.revertible = bool(r)
}

var _ Property = revertible(true)

func Emit(fn interface{}) Property {
	return emitFn{fn}
}

type emitFn struct {
	fn interface{}
}

func (e emitFn) apply(spec *transitionSpec) {
	spec.emitFns = append(spec.emitFns, e.fn)
}

type TransitionSpec interface {
	generate(el scpb.Element, from scpb.Status) (ops, error)
}

type transitionSpec struct {
	from, to   scpb.Status
	revertible bool
	phase      scop.Phase
	emitFns    []interface{}
}

func (t transitionSpec) generate(el scpb.Element, from scpb.Status) (ops, error) {
	t.from = from
	fn, err := makeOpsFunc(el, t.emitFns)
	if err != nil {
		return ops{}, err
	}
	return ops{
		From:       from,
		To:         t.to,
		Revertible: t.revertible,
		Ops:        fn,
	}, nil
}

type OpsFunc func(scpb.Element) []scop.Op

type opFunc func(scpb.Element) scop.Op

func makeOpsFunc(el scpb.Element, fns []interface{}) (OpsFunc, error) {
	var opFuncs []opFunc
	for _, fn := range fns {
		opFunc, err := makeOpFunc(el, fn)
		if err != nil {
			return nil, err
		}
		opFuncs = append(opFuncs, opFunc)
	}
	return func(element scpb.Element) []scop.Op {
		ret := make([]scop.Op, len(opFuncs))
		for i, fn := range opFuncs {
			ret[i] = fn(element)
		}
		return ret
	}, nil
}

var opType = reflect.TypeOf((*scop.Op)(nil)).Elem()

func makeOpFunc(el scpb.Element, fn interface{}) (opFunc, error) {
	fnV := reflect.ValueOf(fn)
	fnT := fnV.Type()
	if fnT.Kind() != reflect.Func {
		return nil, errors.Errorf(
			"%v is a %s, expected %s", fnT, fnT.Kind(), reflect.Func,
		)
	}
	elType := reflect.TypeOf(el)
	if fnT.NumIn() != 1 || fnT.In(0) != elType {
		return nil, errors.Errorf(
			"expected %v to be a func with one argument of type %s", fnT, elType,
		)
	}
	if fnT.NumOut() != 1 || fnT.Out(0) != opType {
		return nil, errors.Errorf(
			"expected %v to be a func with one return value of type %s", fnT, opType,
		)
	}
	return func(element scpb.Element) scop.Op {
		ev := reflect.ValueOf(element)
		if ev.Type() != fnT.In(0) {
			panic(errors.AssertionFailedf(
				"expected %T to be %v", element, fnT.In(0),
			))
		}
		res := fnV.Call([]reflect.Value{ev})
		return res[0].Interface().(scop.Op)
	}, nil
}

type Property interface {
	apply(spec *transitionSpec)
}
