package config

var (
	defaultLogFileSizeThresholdInBytes       = 5_000
	defaultCompactionWorkerSleepTimeInMillis = 10_000
)

type Config struct {
	DbName                            string
	LogFileSizeThresholdInBytes       int
	CompactionWorkerSleepTimeInMillis int
}

func Default(dbName string) *Config {
	return &Config{
		DbName:                            dbName,
		LogFileSizeThresholdInBytes:       defaultLogFileSizeThresholdInBytes,
		CompactionWorkerSleepTimeInMillis: defaultCompactionWorkerSleepTimeInMillis,
	}
}
