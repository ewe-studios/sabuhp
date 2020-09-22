# Mux

https://www.gorillatoolkit.org/pkg/mux

Package `gorilla/mux` implements a request router and dispatcher for matching incoming requests to
their respective handler.

---

## Install

```sh
go get -u github.com/gorilla/mux
```

## Examples

Let's start registering a couple of URL paths and handlers:

```go
func main() {
    r := mux.NewRouter()
    r.HandleFunc("/", HomeHandler)
    r.HandleFunc("/products", ProductsHandler)
    r.HandleFunc("/articles", ArticlesHandler)
    http.Handle("/", r)
}
```

