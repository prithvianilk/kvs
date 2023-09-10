package config

var (
	DefaultLogFileSizeThresholdInBytes       = 5_000
	DefaultCompactionWorkerSleepTimeInMillis = int64(10_000)
)

type Config struct {
	DbName                            string
	LogFileSizeThresholdInBytes       int
	CompactionWorkerSleepTimeInMillis int64
}

func Default(dbName string) *Config {
	return &Config{
		DbName:                            dbName,
		LogFileSizeThresholdInBytes:       DefaultLogFileSizeThresholdInBytes,
		CompactionWorkerSleepTimeInMillis: DefaultCompactionWorkerSleepTimeInMillis,
	}
}
