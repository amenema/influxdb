package tsm1

import (
	"bytes"
	"context"

	"github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/cursors"
	"github.com/influxdata/influxql"
)

// TagValues returns an iterator which enumerates the values for the specific
// tagKey in the given bucket matching the predicate within the
// time range (start, end].
func (e *Engine) TagValues(ctx context.Context, orgID, bucketID influxdb.ID, tagKey string, start, end int64, predicate influxql.Expr) (cursors.StringIterator, error) {
	prefix := tsdb.EncodeName(orgID, bucketID)
	name := prefix[:]

	tsmValues := make(map[string]struct{})

	var m1 [][]byte
	_ = m1
	tagKeyBytes := []byte(tagKey)

	var tags models.Tags

	e.FileStore.ForEachFile(func(f TSMFile) bool {
		if f.OverlapsTimeRange(start, end) && f.OverlapsKeyRange(name, name) {
			// TODO(sgc): create f.IteratorRange(minKey, maxKey)?
			iter := f.Iterator(name)
			for i := 0; iter.Next(); i++ {
				sfkey := iter.Key()
				if !bytes.HasPrefix(sfkey, name) {
					// end of org+bucket
					break
				}

				key, _ := SeriesAndFieldFromCompositeKey(sfkey)
				_, tags = models.ParseKeyBytesWithTags(key, tags)
				curVal := tags.Get(tagKeyBytes)
				if len(curVal) == 0 {
					// series does not have tagKey
					continue
				}

				if _, ok := tsmValues[string(curVal)]; ok {
					// already found overlapping value
					continue
				}

			}
		}
		return true
	})

	// check cache
	_ = e.Cache.ApplyEntryFn(func(key []byte, entry *entry) error {
		if bytes.HasPrefix(key, name) {
			// i, j := entry.values.FindRange(start, end)
		}
		return nil
	})

	// This method would be invoked when the consumer wants to get the following schema information for an arbitrary
	// time range in a single bucket:
	//
	// * All measurement names, i.e. tagKey == _measurement);
	// * All field names for a specific measurement using a predicate
	//     * i.e. tagKey is "_field", predicate _measurement == "<measurement>"
	//
	return cursors.EmptyStringIterator, nil
}
