package otel

// Tag wraps opts in one Option that carries tag, for a package that
// wraps otel and hands out its own destinations (a store's sink, a
// viewer's endpoint): TagOf recognises the option among the ones it is
// handed, and a pipeline lists tag among its Tags for every destination
// of opts it built — nested in Options, Fallback or Environment
// included, never one Install skipped or a Fallback left unused. The
// tag is opaque to otel; a pointer the wrapper owns is the usual one.
func Tag(tag any, opts ...Option) Option {
	return &tagged{tag: tag, opts: append([]Option(nil), opts...)}
}

// TagOf returns the tag of an option Tag built, else nil. It looks at
// o itself only, not inside an Options set.
func TagOf(o Option) any {
	if t, ok := o.(*tagged); ok {
		return t.tag
	}
	return nil
}

type tagged struct {
	tag  any
	opts []Option
}

func (t *tagged) apply(c *config) {
	nd, nf, ne := len(c.dests), len(c.fallback), len(c.env)
	for _, o := range t.opts {
		o.apply(c)
	}
	mark := func(ds []dest) {
		for i := range ds {
			ds[i].tags = append(append([]any(nil), ds[i].tags...), t.tag)
		}
	}
	mark(c.dests[nd:])
	mark(c.fallback[nf:])
	mark(c.env[ne:])
}

// Environment marks opts' destinations as environment destinations, for
// a package that reads variables of its own (a viewer's URL and token):
// like OTEL_EXPORTER_OTLP_ENDPOINT's, they are dropped under NoEnv,
// de-duplicated by URL against the destinations named in code (the
// explicit one wins), and they do not count as named in code — under
// Install, environment destinations that all fail to build leave the
// Fallback in charge. Only the destinations among opts count; any other
// option inside it is ignored.
func Environment(opts ...Option) Option {
	return optionFunc(func(c *config) {
		var env config
		for _, o := range opts {
			o.apply(&env)
		}
		c.env = append(c.env, env.dests...)
	})
}

// Tags returns the tags (Tag) of the destinations the pipeline built,
// in destination order, a destination's innermost tag first.
func (p *Pipeline) Tags() []any {
	var out []any
	for _, rt := range p.dests {
		out = append(out, rt.tags...)
	}
	return out
}
