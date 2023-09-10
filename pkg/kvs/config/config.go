package config

var (
	defaultLogFileSizeThresholdInBytes       = 5_000
	defaultCompactionWorkerSleepTimeInMillis = int64(10_000)
)

type Config struct {
	DbName                            string
	LogFileSizeThresholdInBytes       int
	CompactionWorkerSleepTimeInMillis int64
}

func Default(dbName string) *Config {
	return &Config{
		DbName:                            dbName,
		LogFileSizeThresholdInBytes:       defaultLogFileSizeThresholdInBytes,
		CompactionWorkerSleepTimeInMillis: defaultCompactionWorkerSleepTimeInMillis,
	}
}
