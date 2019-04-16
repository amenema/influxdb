package reads

import "github.com/influxdata/influxdb/storage/reads/datatypes"

type TagValueStreamReader interface {
	Recv() (*datatypes.TagsResponse, error)
}

type StringIteratorStreamReader struct {
	stream   TagValueStreamReader
	response *datatypes.TagsResponse
	i        int

	err error
}

func NewStringIteratorStreamReader(stream TagValueStreamReader) *StringIteratorStreamReader {
	return &StringIteratorStreamReader{
		stream: stream,
	}
}

func (r *StringIteratorStreamReader) Err() error {
	return r.err
}

func (r *StringIteratorStreamReader) Next() bool {
	if r.err != nil {
		return false
	}

	if r.response == nil || len(r.response.Values) - 1 <= r.i {
		r.response, r.err = r.stream.Recv()
		if r.err != nil {
			return false
		}
		r.i = 0

	} else {
		r.i++
	}

	return len(r.response.Values) > r.i
}

func (r *StringIteratorStreamReader) Value() string {
	if len(r.response.Values) > r.i {
		return string(r.response.Values[r.i])
	}

	// Better than panic.
	return ""
}
