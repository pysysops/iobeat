package main

import (
	iobeat "github.com/pysysops/iobeat/beat"

	"github.com/elastic/beats/libbeat/beat"
)

var Version = "1.0.0-beta1"
var Name = "iobeat"

func main() {
	beat.Run(Name, Version, iobeat.NewIoBeat())
}
