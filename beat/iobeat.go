package beat

import (
	"net/url"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/pysysops/iobeat/collector"
	"github.com/pysysops/iobeat/publisher"
)

const selector = "iobeat"

type IoBeat struct {
	IobConfig ConfigSettings
	period    time.Duration
	done      chan struct{}
}

func NewIoBeat() *IoBeat {
	return &IoBeat{}
}

func (fb *IoBeat) Config(b *beat.Beat) error {
	err := cfgfile.Read(&fb.IobConfig, "")
	if err != nil {
		logp.Err("Error reading configuration file: %v", err)
		return err
	}

	// Polling interval
	if fb.IoConfig.Input.Period != nil {
		fb.period = time.Duration(*fb.IoConfig.Input.Period) * time.Second
	} else {
		fb.period = 10 * time.Second
	}

	logp.Debug(selector, "Period %v", fb.period)

	return nil
}

func (fb *IoBeat) Setup(b *beat.Beat) error {
	fb.done = make(chan struct{})
	return nil
}

func (fb *IoBeat) Run(b *beat.Beat) error {
	logp.Debug(selector, "Run iobeat")

	var err error

	ticker := time.NewTicker(fb.period)
	defer ticker.Stop()

	c := collector.NewIoCollector()
	p := publisher.NewIoPublisher(b.Events)

	// TODO: Different scheme
	for {
		select {
		case <-fb.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		s, err := c.Collect()

		if err != nil {
			logp.Err("Failed to read iostats: %v", err)
		} else {
			p.Publish(s)
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > fb.period.Nanoseconds() {
			logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
		}
	}

	return err
}

func (fb *IoBeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (fb *IoBeat) Stop() {
	logp.Debug(selector, "Stop iobeat")
	close(fb.done)
}
