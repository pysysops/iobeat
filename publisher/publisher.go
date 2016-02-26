package publisher

import (
	"time"

	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/publisher"
)

type IoPublisher struct {
	client publisher.Client
}

func NewIoPublisher(c publisher.Client) *IoPublisher {
	return &IoPublisher{client: c}
}

func (fp *IoPublisher) Publish(data map[string]interface{}) {
	fp.client.PublishEvent(common.MapStr{
		"@timestamp": common.Time(time.Now()),
		"type":       "iostats",
		"iostats":     data,
	})
}
