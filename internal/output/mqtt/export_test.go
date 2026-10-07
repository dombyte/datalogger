package mqtt

import "time"

// SetPublishTimeout shortens the publish timeout, so a test need not wait 10 s.
func (d *PahoDialer) SetPublishTimeout(timeout time.Duration) {
	d.publishTimeout = timeout
}
