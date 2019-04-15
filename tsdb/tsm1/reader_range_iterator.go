package tsm1

import (
	"github.com/influxdata/influxdb/tsdb"
)

// TimeRangeIterator allows iterating over each block in a TSM file in order.  It provides
// raw access to the block bytes without decoding them.
type TimeRangeIterator struct {
	r    *TSMReader
	iter *TSMIndexIterator
	tr   TimeRange
	err  error

	// temporary storage
	trbuf []TimeRange
	buf   []byte
	a     tsdb.BooleanArray
}

func (b *TimeRangeIterator) Err() error {
	if b.err != nil {
		return b.err
	}
	return b.iter.Err()
}

// Next advances the iterator and reports if it is still valid.
func (b *TimeRangeIterator) Next() bool {
	if b.Err() != nil {
		return false
	}

	return b.iter.Next()
}

// Key reports the current key.
func (b *TimeRangeIterator) Key() []byte {
	return b.iter.Key()
}

// HasData reports true if the current key has data for the time range.
func (b *TimeRangeIterator) HasData() bool {
	e := excludeEntries(b.iter.Entries(), b.tr)

	b.trbuf = b.r.TombstoneRange(b.iter.Key(), b.trbuf[:0])
	if len(b.trbuf) == 0 {
		if intersectsEntry(e, b.tr) {
			return true
		}

		for i := range e {
			_, b.buf, b.err = b.r.ReadBytes(&e[i], b.buf)
			b.err = DecodeTimestampsArrayBlock(b.buf, &b.a)
			if b.a.Contains(b.tr.Min, b.tr.Max) {
				return true
			}
		}
	} else {
		for i := range e {
			_, b.buf, b.err = b.r.ReadBytes(&e[i], b.buf)
			b.err = DecodeTimestampsArrayBlock(b.buf, &b.a)
			excludeTombstonesBooleanArray(b.trbuf, &b.a)
			if b.a.Contains(b.tr.Min, b.tr.Max) {
				return true
			}
		}
	}

	return false
}

/*
intersectsEntry determines whether the range [min, max].

          +------------------+
          |    IndexEntry    |
+---------+------------------+---------+
|  RANGE  |                  |  RANGE  |
+-+-------+-+           +----+----+----+
  |  RANGE  |           |  RANGE  |
  +----+----+-----------+---------+
       |          RANGE           |
       +--------------------------+
*/

// intersectsEntry determines if tr overlaps one or both boundaries
// of at least one element of e. If that is the case,
// and the block has no tombstones,
func intersectsEntry(e []IndexEntry, tr TimeRange) bool {
	for i := range e {
		min, max := e[i].MinTime, e[i].MaxTime
		if tr.Overlaps(min, max) && !tr.Within(min, max) {
			return true
		}
	}
	return false
}

func excludeEntries(e []IndexEntry, tr TimeRange) []IndexEntry {
	for i := range e {
		if e[i].OverlapsTimeRange(tr.Min, tr.Max) {
			e = e[i:]
			break
		}
	}

	for i := range e {
		if !e[i].OverlapsTimeRange(tr.Min, tr.Max) {
			e = e[:i]
			break
		}
	}

	return e
}
