package ingress

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
