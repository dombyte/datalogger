package mqtt

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/eclipse/paho.golang/paho"

	"github.com/dombyte/datalogger/internal/datasource"
)

// Home Assistant MQTT discovery: a retained config message per point tells Home
// Assistant the state topic, the unit and how to read the value from the payload.
const (
	componentSensor = "sensor"
	componentBinary = "binary_sensor"

	// The value templates read the "value" of the payload; a bool becomes ON or OFF,
	// the default payloads of a binary sensor.
	sensorTemplate = "{{ value_json.value }}"
	binaryTemplate = "{{ 'ON' if value_json.value else 'OFF' }}"
)

// discoveryConfig is the config message of one point.
type discoveryConfig struct {
	Name              string          `json:"name"`
	UniqueID          string          `json:"unique_id"`
	StateTopic        string          `json:"state_topic"`
	ValueTemplate     string          `json:"value_template"`
	Unit              string          `json:"unit_of_measurement,omitempty"`
	AvailabilityTopic string          `json:"availability_topic,omitempty"`
	Device            discoveryDevice `json:"device"`
}

// discoveryDevice groups the points of one device in Home Assistant.
type discoveryDevice struct {
	Identifiers []string `json:"identifiers"`
	Name        string   `json:"name"`
}

// discover publishes the config message of every point of the poll that was not
// announced on this session yet. A failed publish is logged and tried again with the
// next poll; the data publishes notice a broken connection.
func (w *Writer) discover(ctx context.Context, points []datasource.DataPoint) {
	if w.settings.DiscoveryPrefix == "" {
		return
	}
	for _, dp := range points {
		key := topicKey{device: dp.DeviceName, point: dp.PointName}
		if w.announced[key] {
			continue
		}
		topic, body := w.discoveryMessage(dp)
		_, err := w.session.Publish(ctx, &paho.Publish{
			Topic: topic, Payload: body, QoS: 1, Retain: true,
		})
		if err != nil {
			w.logger.Warn().Err(err).Str("topic", topic).Msg("Failed to publish discovery config")
			continue
		}
		w.announced[key] = true
	}
}

// discoveryMessage returns the config topic and message of a point:
// <prefix>/<component>/<topic>/<device>_<point>/config, IDs reduced to the characters
// Home Assistant allows. A bool point is a binary sensor, anything else a sensor.
func (w *Writer) discoveryMessage(dp datasource.DataPoint) (string, []byte) {
	component, template := componentSensor, sensorTemplate
	if _, ok := dp.Value.(bool); ok {
		component, template = componentBinary, binaryTemplate
	}
	node := discoveryID(w.settings.Topic)
	object := discoveryID(dp.DeviceName + "_" + dp.PointName)
	body, err := json.Marshal(discoveryConfig{
		Name:              dp.PointName,
		UniqueID:          node + "_" + object,
		StateTopic:        w.topic(dp),
		ValueTemplate:     template,
		Unit:              dp.Unit,
		AvailabilityTopic: w.settings.AvailabilityTopic,
		Device: discoveryDevice{
			Identifiers: []string{node + "_" + discoveryID(dp.DeviceName)},
			Name:        dp.DeviceName,
		},
	})
	if err != nil {
		//nolint:forbidigo // invariant: the struct holds only strings
		panic("mqtt: encode discovery config: " + err.Error())
	}
	topic := strings.Join([]string{
		w.settings.DiscoveryPrefix, component, node, object, "config",
	}, "/")
	return topic, body
}

// discoveryID keeps letters, digits, _ and - (the characters of a Home Assistant ID)
// and replaces every other character with _.
func discoveryID(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, s)
}
