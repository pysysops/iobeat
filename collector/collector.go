package collector

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/ulricqin/goutils/filetool"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"encoding/json"
)

type IoStats struct {
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

type IoCollector struct{}

func NewIoCollector() *IoCollector {
	return &IoCollector{}
}

func (c *IoCollector) Collect() (map[string]interface{}, error) {
	var (
		err error
		s IoStats
		v map[string]interface{}
	)

	proc_diskstats := "/proc/diskstats"

	if !filetool.IsExist(proc_diskstats) {
		return nil, fmt.Errorf("%s not exists", proc_diskstats)
	}

	contents, err := ioutil.ReadFile(proc_diskstats)
	if err != nil {
		return nil, err
	}

	ret := make([]*IoStats, 0)

	reader := bufio.NewReader(bytes.NewBuffer(contents))

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

		item := &IoStats{}

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
		v += map[string]interface{}{
			strings.Join(append(item.Device, "read_requests"), ""):      item.ReadRequests,
			strings.Join(append(item.Device, "read_merged"), ""):         item.ReadMerged,
			strings.Join(append(item.Device, "read_sectors"), ""):        item.ReadSectors,
			strings.Join(append(item.Device, "msec_read"), ""):           item.MsecRead,
			strings.Join(append(item.Device, "write_requests"), ""):      item.WriteRequests,
			strings.Join(append(item.Device, "write_merged"), ""):        item.WriteMerged,
			strings.Join(append(item.Device, "write_sectors"), ""):       item.WriteSectors,
			strings.Join(append(item.Device, "msec_write"), ""):          item.MsecWrite,
			strings.Join(append(item.Device, "ios_in_progress"), ""):     item.IosInProgress,
			strings.Join(append(item.Device, "msec_total"), ""):          item.MsecTotal,
			strings.Join(append(item.Device, "msec_weighted_total"), ""): item.MsecWeightedTotal,
		}
	}
	return v, nil
}
