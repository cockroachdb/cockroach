package backoff

import (
	"context"
	"time"
)

type ConstantInterval struct {
	interval time.Duration
	jitter   jitter
}

func NewConstantInterval(options ...ConstantOption) *ConstantInterval {
	jitterFactor := 0.0
	interval := time.Minute
	var rng Random

	for _, option := range options {
		switch option.Ident() {
		case identInterval{}:
			interval = option.Value().(time.Duration)
		case identJitterFactor{}:
			jitterFactor = option.Value().(float64)
		case identRNG{}:
			rng = option.Value().(Random)
		}
	}

	return &ConstantInterval{
		interval: interval,
		jitter:   newJitter(jitterFactor, rng),
	}
}

func (g *ConstantInterval) Next() time.Duration {
	return time.Duration(g.jitter.apply(float64(g.interval)))
}

type ConstantPolicy struct {
	cOptions  []ControllerOption
	igOptions []ConstantOption
}

func NewConstantPolicy(options ...Option) *ConstantPolicy {
	var cOptions []ControllerOption
	var igOptions []ConstantOption

	for _, option := range options {
		switch opt := option.(type) {
		case ControllerOption:
			cOptions = append(cOptions, opt)
		default:
			igOptions = append(igOptions, opt.(ConstantOption))
		}
	}

	return &ConstantPolicy{
		cOptions:  cOptions,
		igOptions: igOptions,
	}
}

func (p *ConstantPolicy) Start(ctx context.Context) Controller {
	ig := NewConstantInterval(p.igOptions...)
	return newController(ctx, ig, p.cOptions...)
}
