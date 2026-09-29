// Package mqtt provides MQTT output functionality.
package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/eclipse/paho.golang/paho"
	"github.com/rs/zerolog"

	"github.com/dombyte/datalogger/config"
	"github.com/dombyte/datalogger/datasource"
)

const (
	// keepAliveSeconds is the MQTT keep-alive interval sent in CONNECT.
	keepAliveSeconds = 30

	// drainTimeout bounds how long queued points are still published on shutdown.
	drainTimeout = 10 * time.Second
)

// Writer writes DataPoints to an MQTT broker.
type Writer struct {
	config     config.Output
	logger     zerolog.Logger
	client     *paho.Client
	devices    []string
	lastPubErr error // Track if previous publish failed
}

// New creates a new Writer.
func New(outputConfig config.Output, logger *zerolog.Logger) (*Writer, error) {
	w := &Writer{
		config:  outputConfig,
		logger:  logger.With().Str("output", "mqtt").Str("name", outputConfig.Name).Logger(),
		devices: outputConfig.Devices,
	}

	return w, w.createClient()
}

// createClient creates and configures the MQTT client.
func (w *Writer) createClient() error {
	mqttConfig := w.config.OutputSpecific.Mqtt

	// Parse the broker URL
	brokerURL, err := parseURL(mqttConfig.Address)
	if err != nil {
		return fmt.Errorf("failed to parse MQTT broker URL: %w", err)
	}

	// Create connection
	conn, err := net.Dial(brokerURL.Scheme, brokerURL.Host)
	if err != nil {
		return fmt.Errorf("failed to dial MQTT broker: %w", err)
	}

	// Wrap in TLS if needed
	if brokerURL.Scheme == "tls" || brokerURL.Scheme == "ssl" || mqttConfig.Insecure {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: mqttConfig.Insecure,
		}
		conn = tls.Client(conn, tlsConfig)
	}

	// Create client configuration
	clientConfig := paho.ClientConfig{
		ClientID: mqttConfig.ClientID,
		Conn:     conn,
	}

	// Create the client
	w.client = paho.NewClient(clientConfig)

	return nil
}

// parseURL parses the MQTT broker URL.
func parseURL(address string) (*url.URL, error) {
	// If address doesn't have a scheme, default to tcp
	if !strings.Contains(address, "://") {
		address = "tcp://" + address
	}
	return url.Parse(address)
}

// Name returns the output name.
func (w *Writer) Name() string {
	return w.config.Name
}

// Devices returns the list of device names this output accepts.
func (w *Writer) Devices() []string {
	return w.devices
}

// Validate validates the MQTT writer configuration.
func (w *Writer) Validate() error {
	// Configuration was already validated when creating the writer
	return nil
}

// Start connects to the broker and starts the MQTT writer goroutine.
func (w *Writer) Start(ctx context.Context, input <-chan datasource.DataPoint) <-chan error {
	errCh := make(chan error, 1)

	if err := w.connect(ctx); err != nil {
		w.logger.Error().Err(err).Msg("Failed to connect to MQTT broker")
		errCh <- err
		return errCh
	}

	go w.run(ctx, input)

	return errCh
}

// connect sends CONNECT with the configured client ID and credentials.
func (w *Writer) connect(ctx context.Context) error {
	mqttConfig := w.config.OutputSpecific.Mqtt
	connect := &paho.Connect{
		ClientID:   mqttConfig.ClientID,
		KeepAlive:  keepAliveSeconds,
		CleanStart: true,
	}

	if mqttConfig.Username != "" {
		connect.Username = mqttConfig.Username
		connect.Password = []byte(mqttConfig.Password)
		connect.UsernameFlag = true
		connect.PasswordFlag = true
	}

	if _, err := w.client.Connect(ctx, connect); err != nil {
		return err
	}

	w.logger.Info().Str("broker", mqttConfig.Address).Msg("Connected to MQTT broker")
	return nil
}

// run publishes points until the input is closed or ctx is cancelled, then disconnects.
func (w *Writer) run(ctx context.Context, input <-chan datasource.DataPoint) {
	defer w.disconnect()

	for {
		select {
		case <-ctx.Done():
			w.drainRemainingPoints(input)
			return
		case dp, ok := <-input:
			if !ok {
				return
			}
			w.logger.Debug().
				Str("device", dp.DeviceName).
				Str("point", dp.PointName).
				Msg("MQTT writer: publishing point")
			w.publish(ctx, dp)
		}
	}
}

// disconnect sends DISCONNECT to the broker.
func (w *Writer) disconnect() {
	if err := w.client.Disconnect(&paho.Disconnect{}); err != nil {
		w.logger.Warn().Err(err).Msg("MQTT disconnect failed")
	}
}

// drainRemainingPoints drains remaining points from the input channel during shutdown.
func (w *Writer) drainRemainingPoints(input <-chan datasource.DataPoint) {
	w.logger.Info().Msg("MQTT writer: shutdown started, draining remaining points")
	drainCtx, drainCancel := context.WithTimeout(context.Background(), drainTimeout)
	defer drainCancel()

	w.logger.Info().Msg("MQTT writer: draining remaining points")
	for {
		select {
		case dp, ok := <-input:
			if !ok {
				return
			}
			w.publish(drainCtx, dp)
		default:
			return
		}
	}
}

// publish publishes a DataPoint to <topic>/<device>/<point> with a JSON payload.
func (w *Writer) publish(ctx context.Context, dp datasource.DataPoint) {
	mqttConfig := w.config.OutputSpecific.Mqtt
	topic := fmt.Sprintf("%s/%s/%s", mqttConfig.Topic, dp.DeviceName, dp.PointName)

	payload, err := json.Marshal(map[string]interface{}{
		"value":     dp.Value,
		"unit":      dp.Unit,
		"timestamp": dp.Timestamp.Format(time.RFC3339Nano),
	})
	if err != nil {
		w.logger.Error().Err(err).Msg("Failed to marshal MQTT payload")
		return
	}

	_, err = w.client.Publish(ctx, &paho.Publish{
		Topic:   topic,
		Payload: payload,
		QoS:     byte(mqttConfig.QoS),
		Retain:  mqttConfig.Retain,
	})
	w.logPublishResult(topic, err)
}

// logPublishResult logs a failed publish, and the first success after a failure at info.
func (w *Writer) logPublishResult(topic string, err error) {
	switch {
	case err != nil:
		w.logger.Error().Err(err).Str("topic", topic).Msg("Failed to publish MQTT message")
	case w.lastPubErr != nil:
		w.logger.Info().Str("topic", topic).
			Msg("Published MQTT message (recovered from previous error)")
	default:
		w.logger.Debug().Str("topic", topic).Msg("Successfully published MQTT message")
	}
	w.lastPubErr = err
}
