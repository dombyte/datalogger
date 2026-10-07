package mqtt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
)

const (
	// keepAliveSeconds is the MQTT keep-alive interval sent in CONNECT.
	keepAliveSeconds = 30

	// connectTimeout bounds the TCP/TLS dial and the CONNECT handshake.
	connectTimeout = 10 * time.Second

	// publishTimeout bounds one publish: with QoS 1 and 2 it waits for the broker's
	// acknowledgement, which a broker that hangs would delay until the keep-alive fails.
	publishTimeout = 10 * time.Second
)

// Publisher is an open MQTT session; Dial returns a *paho.Client with a publish timeout.
type Publisher interface {
	Publish(ctx context.Context, p *paho.Publish) (*paho.PublishResponse, error)
	Disconnect(d *paho.Disconnect) error
	Done() <-chan struct{} // closed when the connection is gone
}

// Dialer opens a new MQTT session.
type Dialer interface {
	Dial(ctx context.Context) (Publisher, error)
}

// ConnSettings configures the broker connection.
type ConnSettings struct {
	Address  string // tcp://host:port, tls:// or ssl://; no scheme means tcp
	ClientID string
	Username string // optional
	Password string // optional
	Insecure bool   // TLS without certificate verification (also turns tcp:// into TLS)
}

// PahoDialer dials with github.com/eclipse/paho.golang (MQTT v5).
type PahoDialer struct {
	settings       ConnSettings
	host           string // host:port
	useTLS         bool
	publishTimeout time.Duration
}

// NewDialer checks the address without connecting.
func NewDialer(s ConnSettings) (*PahoDialer, error) {
	address := s.Address
	if !strings.Contains(address, "://") {
		address = "tcp://" + address
	}
	u, err := url.Parse(address)
	if err != nil {
		return nil, fmt.Errorf("mqtt: address: %w", err)
	}
	tlsScheme, ok := schemeUsesTLS(u.Scheme)
	if !ok {
		return nil, fmt.Errorf("mqtt: address: unsupported scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("mqtt: address %q has no host", s.Address)
	}
	return &PahoDialer{
		settings: s, host: u.Host, useTLS: tlsScheme || s.Insecure,
		publishTimeout: publishTimeout,
	}, nil
}

// schemeUsesTLS reports whether a supported scheme means TLS; ok is false for
// unsupported schemes.
func schemeUsesTLS(scheme string) (useTLS, ok bool) {
	switch scheme {
	case "tcp", "mqtt":
		return false, true
	case "tls", "ssl", "mqtts":
		return true, true
	default:
		return false, false
	}
}

// Dial connects to the broker and completes the CONNECT handshake.
func (d *PahoDialer) Dial(ctx context.Context) (Publisher, error) {
	ctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	conn, err := d.dialConn(ctx)
	if err != nil {
		return nil, err
	}

	client := paho.NewClient(paho.ClientConfig{ClientID: d.settings.ClientID, Conn: conn})
	connect := &paho.Connect{
		ClientID:   d.settings.ClientID,
		KeepAlive:  keepAliveSeconds,
		CleanStart: true,
	}
	if d.settings.Username != "" {
		connect.Username, connect.UsernameFlag = d.settings.Username, true
		connect.Password, connect.PasswordFlag = []byte(d.settings.Password), true
	}
	if _, err := client.Connect(ctx, connect); err != nil {
		_ = conn.Close() // the CONNECT error is the one to report
		return nil, fmt.Errorf("connect: %w", err)
	}
	return session{Client: client, publishTimeout: d.publishTimeout}, nil
}

// session is a paho client whose publishes time out.
type session struct {
	*paho.Client
	publishTimeout time.Duration
}

// Publish sends p and waits at most publishTimeout for the broker's acknowledgement.
func (s session) Publish(ctx context.Context, p *paho.Publish) (*paho.PublishResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, s.publishTimeout)
	defer cancel()
	resp, err := s.Client.Publish(ctx, p)
	if err != nil {
		return resp, fmt.Errorf("publish: %w", err)
	}
	return resp, nil
}

// dialConn opens the TCP connection, wrapped in TLS when configured.
func (d *PahoDialer) dialConn(ctx context.Context) (net.Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", d.host)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	if !d.useTLS {
		return conn, nil
	}

	serverName, _, err := net.SplitHostPort(d.host)
	if err != nil {
		serverName = d.host
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: d.settings.Insecure,
		MinVersion:         tls.VersionTLS12,
	})
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		_ = conn.Close() // the handshake error is the one to report
		return nil, fmt.Errorf("tls handshake: %w", err)
	}
	return tlsConn, nil
}
