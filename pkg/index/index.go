package index

type Index interface {
	Set([]byte, int64) error
	Get([]byte) (int64, error)
	Size() int
}
