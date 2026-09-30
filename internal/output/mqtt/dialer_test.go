package mqtt_test

import (
	"context"
	"net"
	"testing"

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
