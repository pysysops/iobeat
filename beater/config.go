package beater

type IoConfig struct {
	Period *int64
	Disks  *[]string
}

type ConfigSettings struct {
	Input IoConfig
}
