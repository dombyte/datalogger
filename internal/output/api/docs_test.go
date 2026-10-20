package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDocs(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, "secret") // the documentation needs no token

	tests := []struct {
		name        string
		path        string
		status      int
		contentType string
	}{
		{name: "index", path: "/", status: 200, contentType: "text/html; charset=utf-8"},
		{name: "openapi", path: "/openapi.json", status: 200, contentType: "application/json"},
		{name: "unknown path", path: "/other", status: 404},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := serve(w, http.MethodGet, tt.path, "")
			assert.Equal(t, tt.status, rec.Code)
			if tt.contentType != "" {
				assert.Equal(t, tt.contentType, rec.Header().Get("Content-Type"))
				assert.Equal(t, "no-cache", rec.Header().Get("Cache-Control"))
				assert.NotEmpty(t, rec.Body.String())
			}
		})
	}
}

// openAPIDoc is the part of the spec the tests compare with the handlers.
type openAPIDoc struct {
	Paths map[string]struct {
		Get struct {
			Responses map[string]openAPIResponse `json:"responses"`
		} `json:"get"`
	} `json:"paths"`
	Components struct {
		Responses map[string]openAPIResponse `json:"responses"`
	} `json:"components"`
}

type openAPIResponse struct {
	Content struct {
		JSON struct {
			Example json.RawMessage `json:"example"`
		} `json:"application/json"`
	} `json:"content"`
}

func parseSpec(t *testing.T) openAPIDoc {
	t.Helper()
	var doc openAPIDoc
	require.NoError(t, json.Unmarshal([]byte(openAPISpec), &doc))
	return doc
}

// TestSpecMatchesHandlers requests every documented path and checks that the examples
// have exactly the fields of the response types, so docs and code cannot drift apart.
func TestSpecMatchesHandlers(t *testing.T) {
	t.Parallel()
	w := newTestWriter(t, "", point("inverter", "power", 1234.5, "W"))
	// The body type of every route the server has; nil: no body.
	bodies := map[string]func() any{
		"/api/devices":                  func() any { return &devicesResponse{} },
		"/api/devices/{device}":         func() any { return &deviceResponse{} },
		"/api/devices/{device}/{point}": func() any { return &singlePointResponse{} },
		"/healthz":                      nil,
	}
	doc := parseSpec(t)
	assert.ElementsMatch(t, mapKeys(bodies), mapKeys(doc.Paths), "documented paths")

	for path, op := range doc.Paths {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			url := strings.NewReplacer("{device}", "inverter", "{point}", "power").Replace(path)
			assert.Equal(t, http.StatusOK, serve(w, http.MethodGet, url, "").Code)

			newBody, ok := bodies[path]
			if !ok || newBody == nil {
				return
			}
			assertExampleFits(t, op.Get.Responses["200"].Content.JSON.Example, newBody())
		})
	}
}

func TestSpecErrorExamples(t *testing.T) {
	t.Parallel()
	for name, resp := range parseSpec(t).Components.Responses {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assertExampleFits(t, resp.Content.JSON.Example, &errorResponse{})
		})
	}
}

// assertExampleFits decodes example into body and encodes it again: an unknown field
// fails the decoding, a missing one appears in the encoding.
func assertExampleFits(t *testing.T, example json.RawMessage, body any) {
	t.Helper()
	require.NotEmpty(t, example, "example")
	dec := json.NewDecoder(bytes.NewReader(example))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(body))
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	assert.JSONEq(t, string(example), string(encoded))
}

func mapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
