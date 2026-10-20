package mqtt_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dombyte/datalogger/internal/output/mqtt"
)

func TestNewDialerChecksAddress(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{
		"localhost:1883", "tcp://broker:1883", "tls://broker:8883",
		"ssl://broker:8883", "mqtt://broker",
	} {
		_, err := mqtt.NewDialer(mqtt.ConnSettings{Address: ok})
		assert.NoError(t, err, ok)
	}
	for _, bad := range []string{"ftp://broker:21", "tcp://", "tcp://bro ker:1"} {
		_, err := mqtt.NewDialer(mqtt.ConnSettings{Address: bad})
		assert.Error(t, err, bad)
	}
}

// fakeBroker accepts one connection, answers CONNECT and PINGREQ and forwards every
// packet except pings.
func fakeBroker(t *testing.T) (string, <-chan *packets.ControlPacket) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })

	received := make(chan *packets.ControlPacket, 10)
	go func() {
		defer close(received)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			cp, err := packets.ReadPacket(conn)
			if err != nil || answer(conn, cp) != nil {
				return
			}
			if cp.Type != packets.PINGREQ {
				received <- cp
			}
		}
	}()
	return listener.Addr().String(), received
}

// answer replies to CONNECT and PINGREQ like a broker.
func answer(conn net.Conn, cp *packets.ControlPacket) error {
	var err error
	switch cp.Type {
	case packets.CONNECT:
		_, err = (&packets.Connack{}).WriteTo(conn)
	case packets.PINGREQ:
		_, err = (&packets.Pingresp{}).WriteTo(conn)
	}
	return err
}

func TestDialConnectsWithCredentialsAndPublishes(t *testing.T) {
	t.Parallel()
	addr, received := fakeBroker(t)
	d, err := mqtt.NewDialer(mqtt.ConnSettings{
		Address: addr, ClientID: "logger-1", Username: "user", Password: "secret",
	})
	require.NoError(t, err)

	session, err := d.Dial(context.Background())
	require.NoError(t, err)

	connect, ok := (<-received).Content.(*packets.Connect)
	require.True(t, ok)
	assert.Equal(t, "logger-1", connect.ClientID)
	assert.Equal(t, "user", connect.Username)
	assert.Equal(t, []byte("secret"), connect.Password)

	_, err = session.Publish(context.Background(), &paho.Publish{Topic: "a/b", Payload: []byte("x")})
	require.NoError(t, err)
	cp := <-received
	require.NotNil(t, cp, "broker connection closed")
	publish, ok := cp.Content.(*packets.Publish)
	require.True(t, ok, "got packet type %d", cp.Type)
	assert.Equal(t, "a/b", publish.Topic)

	require.NoError(t, session.Disconnect(&paho.Disconnect{}))
}

func TestDialFailsWhenNothingListens(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := listener.Addr().String()
	require.NoError(t, listener.Close())

	d, err := mqtt.NewDialer(mqtt.ConnSettings{Address: addr})
	require.NoError(t, err)
	_, err = d.Dial(context.Background())
	assert.Error(t, err)
}

// tlsBroker is fakeBroker behind TLS with a self-signed certificate for 127.0.0.1.
func tlsBroker(t *testing.T) string {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil) // only for its test certificate
	srv.StartTLS()
	certs := srv.TLS.Certificates
	srv.Close()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener = tls.NewListener(listener, &tls.Config{Certificates: certs, MinVersion: tls.VersionTLS12})
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				for {
					cp, err := packets.ReadPacket(conn)
					if err != nil || answer(conn, cp) != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

func TestDialTLS(t *testing.T) {
	t.Parallel()
	addr := tlsBroker(t)

	insecure, err := mqtt.NewDialer(mqtt.ConnSettings{Address: "tls://" + addr, Insecure: true})
	require.NoError(t, err)
	session, err := insecure.Dial(context.Background())
	require.NoError(t, err, "insecure skips the certificate check")
	require.NoError(t, session.Disconnect(&paho.Disconnect{}))

	verified, err := mqtt.NewDialer(mqtt.ConnSettings{Address: "tls://" + addr})
	require.NoError(t, err)
	_, err = verified.Dial(context.Background())
	assert.ErrorContains(t, err, "tls handshake", "the self-signed certificate is rejected")
}

// tlsTopicBroker is a TLS broker for one connection that forwards the topic of every
// PUBLISH it reads; the channel is closed when it reads a broken packet.
func tlsTopicBroker(t *testing.T) (string, <-chan string) {
	t.Helper()
	srv := httptest.NewUnstartedServer(nil) // only for its test certificate
	srv.StartTLS()
	certs := srv.TLS.Certificates
	srv.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	listener = tls.NewListener(listener, &tls.Config{Certificates: certs, MinVersion: tls.VersionTLS12})
	t.Cleanup(func() { listener.Close() })

	topics := make(chan string, 1000)
	go func() {
		defer close(topics)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			cp, err := packets.ReadPacket(conn)
			if err != nil || answer(conn, cp) != nil {
				return
			}
			if p, ok := cp.Content.(*packets.Publish); ok {
				topics <- p.Topic
			}
		}
	}()
	return listener.Addr().String(), topics
}

// Regression: paho writes a packet in several Write calls and from several goroutines;
// on a tls.Conn, which only locks per Write, frames of parallel writes interleaved and
// the broker read garbage. The connection now serialises whole packets.
func TestParallelPublishesOverTLSStayIntact(t *testing.T) {
	t.Parallel()
	addr, topics := tlsTopicBroker(t)
	d, err := mqtt.NewDialer(mqtt.ConnSettings{Address: "tls://" + addr, Insecure: true})
	require.NoError(t, err)
	session, err := d.Dial(context.Background())
	require.NoError(t, err)

	const senders, each = 16, 50
	var wg sync.WaitGroup
	for range senders {
		wg.Go(func() {
			for range each {
				_, err := session.Publish(context.Background(),
					&paho.Publish{Topic: "datalogger/meter/power", Payload: []byte(`{"value":1}`)})
				assert.NoError(t, err)
			}
		})
	}
	wg.Wait()
	for range senders * each {
		topic, ok := <-topics
		require.True(t, ok, "the broker read a broken packet")
		require.Equal(t, "datalogger/meter/power", topic)
	}
	require.NoError(t, session.Disconnect(&paho.Disconnect{}))
}

func TestDialFailsWhenBrokerRejectsConnect(t *testing.T) {
	t.Parallel()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		_, _ = packets.ReadPacket(conn)
		_, _ = (&packets.Connack{ReasonCode: packets.ConnackNotAuthorized}).WriteTo(conn)
		conn.Close()
	}()

	d, err := mqtt.NewDialer(mqtt.ConnSettings{Address: listener.Addr().String()})
	require.NoError(t, err)
	_, err = d.Dial(context.Background())
	assert.ErrorContains(t, err, "connect:")
}

// Regression: a QoS 1 publish waited for a PUBACK without a timeout, so a broker that
// kept the connection but never answered blocked the writer until the keep-alive failed.
func TestPublishTimesOutWithoutAcknowledgement(t *testing.T) {
	t.Parallel()
	addr, received := fakeBroker(t) // never sends PUBACK
	d, err := mqtt.NewDialer(mqtt.ConnSettings{Address: addr})
	require.NoError(t, err)
	d.SetPublishTimeout(50 * time.Millisecond)

	session, err := d.Dial(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { _ = session.Disconnect(&paho.Disconnect{}) })
	<-received // CONNECT

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = session.Publish(ctx, &paho.Publish{Topic: "a/b", QoS: 1, Payload: []byte("x")})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NoError(t, ctx.Err(), "the publish timeout ended the publish, not the test")
}
