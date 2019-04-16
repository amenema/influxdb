package reads_test

import (
	"reflect"
	"testing"

	"github.com/influxdata/influxdb/storage/reads"
	"github.com/influxdata/influxdb/storage/reads/datatypes"
)

type mockStringIteratorStream struct {
	responsesSent []*datatypes.TagsResponse
}

func (s *mockStringIteratorStream) Send(response *datatypes.TagsResponse) error {
	responseCopy := &datatypes.TagsResponse{
		Values: make([][]byte, len(response.Values)),
	}
	for i := range response.Values {
		responseCopy.Values[i] = response.Values[i]
	}
	s.responsesSent = append(s.responsesSent, responseCopy)
	return nil
}

func TestStringIteratorWriter(t *testing.T) {
	mockStream := &mockStringIteratorStream{}
	w := reads.NewStringIteratorWriter(mockStream, 0)
	si := newMockStringIterator("foo", "bar")
	err := w.WriteStringIterator(si)
	if err != nil {
		t.Fatal(err)
	}
	w.Flush()

	var got []string
	for _, response := range mockStream.responsesSent {
		for _, v := range response.Values {
			got = append(got, string(v))
		}
	}

	expect := []string{"foo", "bar"}
	if !reflect.DeepEqual(expect, got) {
		t.Errorf("expected %v got %v", expect, got)
	}
}
