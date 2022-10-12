package httpcc

type RequestDirective struct {
	maxAge       *uint64
	maxStale     *uint64
	minFresh     *uint64
	noCache      bool
	noStore      bool
	noTransform  bool
	onlyIfCached bool
	extensions   map[string]string
}

func (d *RequestDirective) MaxAge() (uint64, bool) {
	if v := d.maxAge; v != nil {
		return *v, true
	}
	return 0, false
}

func (d *RequestDirective) MaxStale() (uint64, bool) {
	if v := d.maxStale; v != nil {
		return *v, true
	}
	return 0, false
}

func (d *RequestDirective) MinFresh() (uint64, bool) {
	if v := d.minFresh; v != nil {
		return *v, true
	}
	return 0, false
}

func (d *RequestDirective) NoCache() bool {
	return d.noCache
}

func (d *RequestDirective) NoStore() bool {
	return d.noStore
}

func (d *RequestDirective) NoTransform() bool {
	return d.noTransform
}

func (d *RequestDirective) OnlyIfCached() bool {
	return d.onlyIfCached
}

func (d *RequestDirective) Extensions() map[string]string {
	return d.extensions
}

func (d *RequestDirective) Extension(s string) string {
	return d.extensions[s]
}

type ResponseDirective struct {
	maxAge          *uint64
	noCache         []string
	noStore         bool
	noTransform     bool
	public          bool
	private         []string
	proxyRevalidate bool
	sMaxAge         *uint64
	extensions      map[string]string
}

func (d *ResponseDirective) MaxAge() (uint64, bool) {
	if v := d.maxAge; v != nil {
		return *v, true
	}
	return 0, false
}

func (d *ResponseDirective) NoCache() []string {
	return d.noCache
}

func (d *ResponseDirective) NoStore() bool {
	return d.noStore
}

func (d *ResponseDirective) NoTransform() bool {
	return d.noTransform
}

func (d *ResponseDirective) Public() bool {
	return d.public
}

func (d *ResponseDirective) Private() []string {
	return d.private
}

func (d *ResponseDirective) ProxyRevalidate() bool {
	return d.proxyRevalidate
}

func (d *ResponseDirective) SMaxAge() (uint64, bool) {
	if v := d.sMaxAge; v != nil {
		return *v, true
	}
	return 0, false
}

func (d *ResponseDirective) Extensions() map[string]string {
	return d.extensions
}

func (d *ResponseDirective) Extension(s string) string {
	return d.extensions[s]
}


