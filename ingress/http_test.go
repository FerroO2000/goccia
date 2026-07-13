package ingress

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_HTTPRunner_ReadBodyCountsObservedBytes(t *testing.T) {
	tests := []struct {
		name          string
		body          string
		maxBodySize   int64
		wantBody      string
		wantBytesRead int64
		wantOK        bool
		wantStatus    int
	}{
		{
			name:          "empty body",
			maxBodySize:   4,
			wantBytesRead: 0,
			wantOK:        true,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "body within limit",
			body:          "data",
			maxBodySize:   4,
			wantBody:      "data",
			wantBytesRead: 4,
			wantOK:        true,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "body over limit",
			body:          "excess",
			maxBodySize:   4,
			wantBytesRead: 4,
			wantOK:        false,
			wantStatus:    http.StatusRequestEntityTooLarge,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner := newHTTPRunner(nil)
			runner.SetEnvironment(&httpEnv{maxRequestBodySize: tt.maxBodySize})
			resWriter := httptest.NewRecorder()

			body, bytesRead, ok := runner.readBody(
				resWriter, io.NopCloser(strings.NewReader(tt.body)),
			)

			assert.Equal(t, tt.wantBody, string(body))
			assert.Equal(t, tt.wantBytesRead, bytesRead)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantStatus, resWriter.Code)
		})
	}
}

func Test_HTTPRequestScheme(t *testing.T) {
	runner := &httpRunner{}

	tests := []struct {
		name string
		tls  *tls.ConnectionState
		want string
	}{
		{name: "HTTP", want: "http"},
		{name: "HTTPS", tls: &tls.ConnectionState{}, want: "https"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{TLS: tt.tls}
			assert.Equal(t, tt.want, runner.getURLcheme(req))
		})
	}
}

func Test_HTTPRequestProtocolVersion(t *testing.T) {
	runner := &httpRunner{}

	tests := []struct {
		name       string
		protoMajor int
		protoMinor int
		want       string
	}{
		{name: "HTTP/1.0", protoMajor: 1, protoMinor: 0, want: "1.0"},
		{name: "HTTP/1.1", protoMajor: 1, protoMinor: 1, want: "1.1"},
		{name: "HTTP/2", protoMajor: 2, protoMinor: 0, want: "2"},
		{name: "HTTP/3", protoMajor: 3, protoMinor: 0, want: "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &http.Request{ProtoMajor: tt.protoMajor, ProtoMinor: tt.protoMinor}
			assert.Equal(t, tt.want, runner.getProtocolVersion(req))
		})
	}
}

func Test_HTTPEnv_InitTLSLeavesTLSDisabledByDefault(t *testing.T) {
	cfg := NewHTTPConfig()
	e := newHTTPEnv(nil, cfg)
	e.initServer()

	require.NoError(t, e.initTLS())
	assert.False(t, cfg.TLSEnabled)
	assert.False(t, e.tlsEnabled)
	assert.Nil(t, e.server.TLSConfig)
}

func Test_HTTPEnv_InitTLSRequiresConfigurationWhenEnabled(t *testing.T) {
	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true

	e := newHTTPEnv(nil, cfg)
	e.initServer()

	assert.Error(t, e.initTLS())
}

func Test_HTTPEnv_InitTLSRequiresCertificateSource(t *testing.T) {
	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true
	cfg.TLSConfig = &tls.Config{}

	e := newHTTPEnv(nil, cfg)
	e.initServer()

	assert.Error(t, e.initTLS())
}

func Test_HTTPEnv_InitTLSClonesConfigAndAppliesDefaultMinimumVersion(t *testing.T) {
	originalTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{makeTestTLSCertificate(t)},
	}

	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true
	cfg.TLSConfig = originalTLSConfig

	e := newHTTPEnv(nil, cfg)
	e.initServer()

	require.NoError(t, e.initTLS())
	assert.True(t, e.tlsEnabled)
	assert.NotSame(t, originalTLSConfig, e.server.TLSConfig)
	assert.Equal(t, uint16(tls.VersionTLS12), e.server.TLSConfig.MinVersion)
	assert.Len(t, e.server.TLSConfig.Certificates, 1)
	assert.Zero(t, originalTLSConfig.MinVersion)
}

func Test_HTTPEnv_InitTLSAcceptsDynamicCertificate(t *testing.T) {
	cfg := NewHTTPConfig()
	cfg.TLSEnabled = true
	cfg.TLSConfig = &tls.Config{
		GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
			return nil, nil
		},
	}

	e := newHTTPEnv(nil, cfg)
	e.initServer()

	require.NoError(t, e.initTLS())
	assert.True(t, e.tlsEnabled)
	assert.Equal(t, uint16(tls.VersionTLS12), e.server.TLSConfig.MinVersion)
}

func makeTestTLSCertificate(t *testing.T) tls.Certificate {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certificateDER, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	require.NoError(t, err)

	return tls.Certificate{
		Certificate: [][]byte{certificateDER},
		PrivateKey:  privateKey,
	}
}
