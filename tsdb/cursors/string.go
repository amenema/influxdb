package cursors

// StringIterator describes the behavior for enumerating a sequence of
// string values.
type StringIterator interface {
	// Next advances the StringIterator to the next value. It returns false
	// when there are no more values.
	Next() bool

	// Value returns the current value.
	Value() string

	Stats() CursorStats
}

// EmptyStringIterator is an implementation of StringIterator that returns
// no values.
var EmptyStringIterator StringIterator = &stringIterator{}

type stringIterator struct{}

func (*stringIterator) Next() bool         { return false }
func (*stringIterator) Value() string      { return "" }
func (*stringIterator) Stats() CursorStats { return CursorStats{} }
