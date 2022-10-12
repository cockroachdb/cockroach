httpcc
======

Parses HTTP/1.1 Cache-Control header, and returns a struct that is convenient
for the end-user to do what they will with.

# Parsing the HTTP Request

```go
dir, err := httpcc.ParseRequest(req.Header.Get(`Cache-Control`))
// dir.MaxAge()       uint64, bool
// dir.MaxStale()     uint64, bool
// dir.MinFresh()     uint64, bool
// dir.NoCache()      bool
// dir.NoStore()      bool
// dir.NoTransform()  bool
// dir.OnlyIfCached() bool
// dir.Extensions()   map[string]string
```

# Parsing the HTTP Response

```go
directives, err := httpcc.ParseResponse(res.Header.Get(`Cache-Control`))
// dir.MaxAge()         uint64, bool
// dir.MustRevalidate() bool
// dir.NoCache()        []string
// dir.NoStore()        bool
// dir.NoTransform()    bool
// dir.Public()         bool
// dir.Private()        bool
// dir.SMaxAge()        uint64, bool
// dir.Extensions()     map[string]string
```

