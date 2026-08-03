package message

import "net/http"

// HTTPRequest is message carrying an HTTP request.
// It is produced by the HTTP ingress stage.
type HTTPRequest struct {
	RemoteAddr string
	Method     string
	Path       string
	Query      string
	Header     http.Header
	Body       []byte
}

// Destroy cleans up the message.
func (m *HTTPRequest) Destroy() {
	m.RemoteAddr = ""
	m.Method = ""
	m.Path = ""
	m.Query = ""
	m.Header = nil
	m.Body = nil
}

// HTTPResponse is message carrying an HTTP response.
// It is produced by the HTTP egress stage,
// and consumed by the HTTP ingress stage through the link and futures.
type HTTPResponse struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

// Destroy cleans up the message.
func (m *HTTPResponse) Destroy() {
	m.StatusCode = 0
	m.Header = nil
	m.Body = nil
}
