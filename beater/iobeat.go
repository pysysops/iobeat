package beater

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ulricqin/goutils/filetool"
	"io"
	"io/ioutil"
	"strconv"
	"strings"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/cfgfile"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"
)

type Iobeat struct {
	period           time.Duration
	IbConfig         ConfigSettings
	events           publisher.Client
	done chan struct{}
}

type DiskStats struct {
	Major             int
	Minor             int
	Device            string
	ReadRequests      uint64 // Total number of reads completed successfully.
	ReadMerged        uint64 // Adjacent read requests merged in a single req.
	ReadSectors       uint64 // Total number of sectors read successfully.
	MsecRead          uint64 // Total number of ms spent by all reads.
	WriteRequests     uint64 // total number of writes completed successfully.
	WriteMerged       uint64 // Adjacent write requests merged in a single req.
	WriteSectors      uint64 // total number of sectors written successfully.
	MsecWrite         uint64 // Total number of ms spent by all writes.
	IosInProgress     uint64 // Number of actual I/O requests currently in flight.
	MsecTotal         uint64 // Amount of time during which ios_in_progress >= 1.
	MsecWeightedTotal uint64 // Measure of recent I/O completion time and backlog.
}

func New() *Iobeat {
	return &Iobeat{}
}

func (ib *Iobeat) Config(b *beat.Beat) error {

	err := cfgfile.Read(&ib.IbConfig, "")
	if err != nil {
		logp.Err("Error reading configuration file: %v", err)
		return err
	}

	if ib.IoConfig.Input.Period != nil {
		ib.period = time.Duration(*tb.TbConfig.Input.Period) * time.Second
	} else {
		ib.period = 10 * time.Second
	}

	logp.Debug("iobeat", "File system statistics %t\n", tb.fsStats)

	return nil
}

func (ib *Iobeat) Setup(b *beat.Beat) error {
	ib.events = b.Events
	ib.done = make(chan struct{})
	return nil
}

func (i *Iobeat) Run(b *beat.Beat) error {
	var err error

	ticker := time.NewTicker(i.period)
	defer ticker.Stop()

	for {
		select {
		case <-i.done:
			return nil
		case <-ticker.C:
		}

		timerStart := time.Now()

		err = i.exportIoStats()
		if err != nil {
			logp.Err("Error reading io stats: %v", err)
			break
		}

		timerEnd := time.Now()
		duration := timerEnd.Sub(timerStart)
		if duration.Nanoseconds() > t.period.Nanoseconds() {
			logp.Warn("Ignoring tick(s) due to processing taking longer than one period")
		}
	}

	return err
}

func (ib *Iobeat) Cleanup(b *beat.Beat) error {
	return nil
}

func (i *Iobeat) Stop() {
	close(i.done)
}

func (i *Iobeat) exportIoStats() error {
	i.events.PublishEvents(collectIoStats())
	return nil
}

func collectIoStats() []common.MapStr {

	proc_diskstats := "/proc/diskstats"
	if !filetool.IsExist(proc_diskstats) {
		return nil, fmt.Errorf("%s not exists", proc_diskstats)
	}

	contents, err := ioutil.ReadFile(proc_diskstats)
	if err != nil {
		return nil, err
	}

	events := make([]common.MapStr, 0, len(fss))

	for {
		line, _, err := reader.ReadLine()
		if err == io.EOF {
			break
		}

		fields := strings.Fields(string(line))
		// shortcut the deduper and just skip disks that
		// haven't done a single read.  This elimiates a bunch
		// of loopback, ramdisk, and cdrom devices but still
		// lets us report on the rare case that we actually use
		// a ramdisk.
		if fields[3] == "0" {
			continue
		}

		size := len(fields)
		// kernel version too low
		if size != 14 {
			continue
		}


		item := &DiskStats{}
		for i := 0; i < size; i++ {
			if item.Major, err = strconv.Atoi(fields[0]); err != nil {
				return nil, err
			}

			if item.Minor, err = strconv.Atoi(fields[1]); err != nil {
				return nil, err
			}

			item.Device = fields[2]

			if item.ReadRequests, err = strconv.ParseUint(fields[3], 10, 64); err != nil {
				return nil, err
			}

			if item.ReadMerged, err = strconv.ParseUint(fields[4], 10, 64); err != nil {
				return nil, err
			}

			if item.ReadSectors, err = strconv.ParseUint(fields[5], 10, 64); err != nil {
				return nil, err
			}

			if item.MsecRead, err = strconv.ParseUint(fields[6], 10, 64); err != nil {
				return nil, err
			}

			if item.WriteRequests, err = strconv.ParseUint(fields[7], 10, 64); err != nil {
				return nil, err
			}

			if item.WriteMerged, err = strconv.ParseUint(fields[8], 10, 64); err != nil {
				return nil, err
			}

			if item.WriteSectors, err = strconv.ParseUint(fields[9], 10, 64); err != nil {
				return nil, err
			}

			if item.MsecWrite, err = strconv.ParseUint(fields[10], 10, 64); err != nil {
				return nil, err
			}

			if item.IosInProgress, err = strconv.ParseUint(fields[11], 10, 64); err != nil {
				return nil, err
			}

			if item.MsecTotal, err = strconv.ParseUint(fields[12], 10, 64); err != nil {
				return nil, err
			}

			if item.MsecWeightedTotal, err = strconv.ParseUint(fields[13], 10, 64); err != nil {
				return nil, err
			}

		}

		event := common.MapStr{
			"@timestamp": common.Time(time.Now()),
			"type":       "iostats",
			"count":      1,
			"device": common.MapStr{
				"major":                item.Major,
				"minor":                item.Minor,
				"device":               item.Device,
				"read_requests":        item.ReadRequests,
				"read_merged":          item.ReadMerged,
				"read_sectors":         item.ReadSectors,
				"msec_read":            item.MsecRead,
				"write_requests":       item.WriteRequests,
				"write_merged":         item.WriteMerged,
				"write_sectors":        item.WriteSectors,
				"msec_write":           item.MsecWrite,
				"ios_in_progress":      item.IosInProgress,
				"msec_total":           item.MsecTotal,
				"msec_weighted_total":  item.MsecWeightedTotal,
			},
		}

		events = append(events, event)
	}
	return events
}
