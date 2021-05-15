package mixer

import (
	"strings"

	"github.com/ewe-studios/sabuhp"
	"github.com/ewe-studios/sabuhp/utils"
)

type Handler interface {
	Handle(message *sabuhp.Message) (*sabuhp.Message, error)
}

type HandlerFunc func(message *sabuhp.Message) (*sabuhp.Message, error)

func (h HandlerFunc) Handle(message *sabuhp.Message) (*sabuhp.Message, error) {
	return h(message)
}

type MuxConfig struct {
	RootPath string
	NotFound Handler
}

// Mux is Request multiplexer.
// It matches an event name or http url pattern to
// a specific TransportHandler which will be registered
// to the provided transport for handling specific events.
type Mux struct {
	config   MuxConfig
	rootPath string
	trie     *Trie
	NotFound Handler
}

func NewMux(config MuxConfig) *Mux {
	return &Mux{
		config:   config,
		rootPath: config.RootPath,
		trie:     NewTrie(),
		NotFound: config.NotFound,
	}
}

func (m *Mux) Serve(route string, handler Handler) {
	var searchRoute = utils.ReduceMultipleSlashToOne(m.rootPath + route)
	m.trie.Insert(searchRoute, WithHandler(handler))
}

// Match implements the Matcher interface.
//
// Allow a mux to be used as a matcher and handler elsewhere.
func (m *Mux) Match(target string) bool {
	var handler = m.trie.Search(target, sabuhp.Params{})
	return handler != nil
}

func (m *Mux) ServeRoute(d *sabuhp.Message) (*sabuhp.Message, error) {
	var reqPath = d.Path
	if len(reqPath) > 1 && strings.HasSuffix(reqPath, "/") {
		// remove trailing slash and client-permanent rule for redirection,
		// if configuration allows that and reqPath has an extra slash.

		// update the new reqPath and redirect.
		// use Trim to ensure there is no open redirect due to two leading slashes
		reqPath = pathSep + strings.Trim(reqPath, pathSep)
	}

	// r.URL.Query() is slow and will allocate a lot, although
	// the first idea was to not introduce a new type to the end-developers
	// so they are using this library as the std one, but we will have to do it
	// for the params, we keep that rule so a new ResponseWriter, which is an interface,
	// and it will be compatible with net/http will be introduced to store the params at least,
	// we don't want to add a third parameter or a global state to this library.
	var targetNode = m.trie.Search(reqPath, d.Params)
	if targetNode == nil {
		return m.NotFound.Handle(d)
	}
	return targetNode.Handler.Handle(d)
}

func indexOfList(vs []string, m string) int {
	for index, v := range vs {
		if v == m {
			return index
		}
	}
	return -1
}
func toLower(vs []string) []string {
	for index, v := range vs {
		vs[index] = strings.ToLower(v)
	}
	return vs
}
