package api

import (
	_ "embed"
	"io"
	"net/http"
)

// The documentation is static and embedded at build time: it names no device or
// point, so it needs no token.
var (
	//go:embed docs/index.html
	indexHTML string
	//go:embed docs/openapi.json
	openAPISpec string
)

// serveDoc returns a handler that writes an embedded document.
func (w *Writer) serveDoc(body, contentType string) http.HandlerFunc {
	return func(rw http.ResponseWriter, _ *http.Request) {
		rw.Header().Set("Content-Type", contentType)
		// The documents change with an upgrade; clients revalidate instead of caching.
		rw.Header().Set("Cache-Control", "no-cache")
		// A failed write means the client went away; there is nobody left to tell.
		if _, err := io.WriteString(rw, body); err != nil {
			w.logger.Debug().Err(err).Msg("Failed to write API documentation")
		}
	}
}
