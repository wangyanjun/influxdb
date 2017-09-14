package query

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"regexp"

	"github.com/gogo/protobuf/proto"
	"github.com/influxdata/influxdb/influxql"
	internal "github.com/influxdata/influxdb/query/internal"
)

// ErrUnknownCall is returned when operating on an unknown function call.
var ErrUnknownCall = errors.New("unknown call")

const (
	// secToNs is the number of nanoseconds in a second.
	secToNs = int64(time.Second)
)

// Iterator represents a generic interface for all Iterators.
// Most iterator operations are done on the typed sub-interfaces.
type Iterator interface {
	Stats() IteratorStats
	Close() error
}

// Iterators represents a list of iterators.
type Iterators []Iterator

// Stats returns the aggregation of all iterator stats.
func (a Iterators) Stats() IteratorStats {
	var stats IteratorStats
	for _, itr := range a {
		stats.Add(itr.Stats())
	}
	return stats
}

// Close closes all iterators.
func (a Iterators) Close() error {
	for _, itr := range a {
		itr.Close()
	}
	return nil
}

// filterNonNil returns a slice of iterators that removes all nil iterators.
func (a Iterators) filterNonNil() []Iterator {
	other := make([]Iterator, 0, len(a))
	for _, itr := range a {
		if itr == nil {
			continue
		}
		other = append(other, itr)
	}
	return other
}

// castType determines what type to cast the set of iterators to.
// An iterator type is chosen using this hierarchy:
//   float > integer > string > boolean
func (a Iterators) castType() influxql.DataType {
	if len(a) == 0 {
		return influxql.Unknown
	}

	typ := influxql.DataType(influxql.Boolean)
	for _, input := range a {
		switch input.(type) {
		case FloatIterator:
			// Once a float iterator is found, short circuit the end.
			return influxql.Float
		case IntegerIterator:
			if typ > influxql.Integer {
				typ = influxql.Integer
			}
		case StringIterator:
			if typ > influxql.String {
				typ = influxql.String
			}
		case BooleanIterator:
			// Boolean is the lowest type.
		}
	}
	return typ
}

// cast casts an array of iterators to a single type.
// Iterators that are not compatible or cannot be cast to the
// chosen iterator type are closed and dropped.
func (a Iterators) cast() interface{} {
	typ := a.castType()
	switch typ {
	case influxql.Float:
		return newFloatIterators(a)
	case influxql.Integer:
		return newIntegerIterators(a)
	case influxql.String:
		return newStringIterators(a)
	case influxql.Boolean:
		return newBooleanIterators(a)
	}
	return a
}

// Merge combines all iterators into a single iterator.
// A sorted merge iterator or a merge iterator can be used based on opt.
func (a Iterators) Merge(opt IteratorOptions) (Iterator, error) {
	// Check if this is a call expression.
	call, ok := opt.Expr.(*influxql.Call)

	// Merge into a single iterator.
	if !ok && opt.MergeSorted() {
		itr := NewSortedMergeIterator(a, opt)
		if itr != nil && opt.InterruptCh != nil {
			itr = NewInterruptIterator(itr, opt.InterruptCh)
		}
		return itr, nil
	}

	// We do not need an ordered output so use a merge iterator.
	itr := NewMergeIterator(a, opt)
	if itr == nil {
		return nil, nil
	}

	if opt.InterruptCh != nil {
		itr = NewInterruptIterator(itr, opt.InterruptCh)
	}

	if !ok {
		// This is not a call expression so do not use a call iterator.
		return itr, nil
	}

	// When merging the count() function, use sum() to sum the counted points.
	if call.Name == "count" {
		opt.Expr = &influxql.Call{
			Name: "sum",
			Args: call.Args,
		}
	}
	return NewCallIterator(itr, opt)
}

// NewMergeIterator returns an iterator to merge itrs into one.
// Inputs must either be merge iterators or only contain a single name/tag in
// sorted order. The iterator will output all points by window, name/tag, then
// time. This iterator is useful when you need all of the points for an
// interval.
func NewMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if n := len(inputs); n == 0 {
		return nil
	} else if n == 1 {
		return inputs[0]
	}

	// Aggregate functions can use a more relaxed sorting so that points
	// within a window are grouped. This is much more efficient.
	switch inputs := Iterators(inputs).cast().(type) {
	case []FloatIterator:
		return newFloatMergeIterator(inputs, opt)
	case []IntegerIterator:
		return newIntegerMergeIterator(inputs, opt)
	case []StringIterator:
		return newStringMergeIterator(inputs, opt)
	case []BooleanIterator:
		return newBooleanMergeIterator(inputs, opt)
	default:
		panic(fmt.Sprintf("unsupported merge iterator type: %T", inputs))
	}
}

// NewParallelMergeIterator returns an iterator that breaks input iterators
// into groups and processes them in parallel.
func NewParallelMergeIterator(inputs []Iterator, opt IteratorOptions, parallelism int) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return nil
	} else if len(inputs) == 1 {
		return inputs[0]
	}

	// Limit parallelism to the number of inputs.
	if len(inputs) < parallelism {
		parallelism = len(inputs)
	}

	// Determine the number of inputs per output iterator.
	n := len(inputs) / parallelism

	// Group iterators together.
	outputs := make([]Iterator, parallelism)
	for i := range outputs {
		var slice []Iterator
		if i < len(outputs)-1 {
			slice = inputs[i*n : (i+1)*n]
		} else {
			slice = inputs[i*n:]
		}

		outputs[i] = newParallelIterator(NewMergeIterator(slice, opt))
	}

	// Merge all groups together.
	return NewMergeIterator(outputs, opt)
}

// NewSortedMergeIterator returns an iterator to merge itrs into one.
// Inputs must either be sorted merge iterators or only contain a single
// name/tag in sorted order. The iterator will output all points by name/tag,
// then time. This iterator is useful when you need all points for a name/tag
// to be in order.
func NewSortedMergeIterator(inputs []Iterator, opt IteratorOptions) Iterator {
	inputs = Iterators(inputs).filterNonNil()
	if len(inputs) == 0 {
		return nil
	} else if len(inputs) == 1 {
		return inputs[0]
	}

	switch inputs := Iterators(inputs).cast().(type) {
	case []FloatIterator:
		return newFloatSortedMergeIterator(inputs, opt)
	case []IntegerIterator:
		return newIntegerSortedMergeIterator(inputs, opt)
	case []StringIterator:
		return newStringSortedMergeIterator(inputs, opt)
	case []BooleanIterator:
		return newBooleanSortedMergeIterator(inputs, opt)
	default:
		panic(fmt.Sprintf("unsupported sorted merge iterator type: %T", inputs))
	}
}

// newParallelIterator returns an iterator that runs in a separate goroutine.
func newParallelIterator(input Iterator) Iterator {
	if input == nil {
		return nil
	}

	switch itr := input.(type) {
	case FloatIterator:
		return newFloatParallelIterator(itr)
	case IntegerIterator:
		return newIntegerParallelIterator(itr)
	case StringIterator:
		return newStringParallelIterator(itr)
	case BooleanIterator:
		return newBooleanParallelIterator(itr)
	default:
		panic(fmt.Sprintf("unsupported parallel iterator type: %T", itr))
	}
}

// NewLimitIterator returns an iterator that limits the number of points per grouping.
func NewLimitIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatLimitIterator(input, opt)
	case IntegerIterator:
		return newIntegerLimitIterator(input, opt)
	case StringIterator:
		return newStringLimitIterator(input, opt)
	case BooleanIterator:
		return newBooleanLimitIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported limit iterator type: %T", input))
	}
}

// NewFilterIterator returns an iterator that filters the points based on the
// condition. This iterator is not nearly as efficient as filtering points
// within the query engine and is only used when filtering subqueries.
func NewFilterIterator(input Iterator, cond influxql.Expr, opt IteratorOptions) Iterator {
	if input == nil {
		return nil
	}

	switch input := input.(type) {
	case FloatIterator:
		return newFloatFilterIterator(input, cond, opt)
	case IntegerIterator:
		return newIntegerFilterIterator(input, cond, opt)
	case StringIterator:
		return newStringFilterIterator(input, cond, opt)
	case BooleanIterator:
		return newBooleanFilterIterator(input, cond, opt)
	default:
		panic(fmt.Sprintf("unsupported filter iterator type: %T", input))
	}
}

// NewDedupeIterator returns an iterator that only outputs unique points.
// This iterator maintains a serialized copy of each row so it is inefficient
// to use on large datasets. It is intended for small datasets such as meta queries.
func NewDedupeIterator(input Iterator) Iterator {
	if input == nil {
		return nil
	}

	switch input := input.(type) {
	case FloatIterator:
		return newFloatDedupeIterator(input)
	case IntegerIterator:
		return newIntegerDedupeIterator(input)
	case StringIterator:
		return newStringDedupeIterator(input)
	case BooleanIterator:
		return newBooleanDedupeIterator(input)
	default:
		panic(fmt.Sprintf("unsupported dedupe iterator type: %T", input))
	}
}

// NewFillIterator returns an iterator that fills in missing points in an aggregate.
func NewFillIterator(input Iterator, expr influxql.Expr, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatFillIterator(input, expr, opt)
	case IntegerIterator:
		return newIntegerFillIterator(input, expr, opt)
	case StringIterator:
		return newStringFillIterator(input, expr, opt)
	case BooleanIterator:
		return newBooleanFillIterator(input, expr, opt)
	default:
		panic(fmt.Sprintf("unsupported fill iterator type: %T", input))
	}
}

// NewIntervalIterator returns an iterator that sets the time on each point to the interval.
func NewIntervalIterator(input Iterator, opt IteratorOptions) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatIntervalIterator(input, opt)
	case IntegerIterator:
		return newIntegerIntervalIterator(input, opt)
	case StringIterator:
		return newStringIntervalIterator(input, opt)
	case BooleanIterator:
		return newBooleanIntervalIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported interval iterator type: %T", input))
	}
}

// NewInterruptIterator returns an iterator that will stop producing output
// when the passed-in channel is closed.
func NewInterruptIterator(input Iterator, closing <-chan struct{}) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatInterruptIterator(input, closing)
	case IntegerIterator:
		return newIntegerInterruptIterator(input, closing)
	case StringIterator:
		return newStringInterruptIterator(input, closing)
	case BooleanIterator:
		return newBooleanInterruptIterator(input, closing)
	default:
		panic(fmt.Sprintf("unsupported interrupt iterator type: %T", input))
	}
}

// NewCloseInterruptIterator returns an iterator that will invoke the Close() method on an
// iterator when the passed-in channel has been closed.
func NewCloseInterruptIterator(input Iterator, closing <-chan struct{}) Iterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatCloseInterruptIterator(input, closing)
	case IntegerIterator:
		return newIntegerCloseInterruptIterator(input, closing)
	case StringIterator:
		return newStringCloseInterruptIterator(input, closing)
	case BooleanIterator:
		return newBooleanCloseInterruptIterator(input, closing)
	default:
		panic(fmt.Sprintf("unsupported close iterator iterator type: %T", input))
	}
}

// AuxIterator represents an iterator that can split off separate auxiliary iterators.
type AuxIterator interface {
	Iterator

	// Auxilary iterator
	Iterator(name string, typ influxql.DataType) Iterator

	// Start starts writing to the created iterators.
	Start()

	// Backgrounds the iterator so that, when start is called, it will
	// continuously read from the iterator.
	Background()
}

// NewAuxIterator returns a new instance of AuxIterator.
func NewAuxIterator(input Iterator, opt IteratorOptions) AuxIterator {
	switch input := input.(type) {
	case FloatIterator:
		return newFloatAuxIterator(input, opt)
	case IntegerIterator:
		return newIntegerAuxIterator(input, opt)
	case StringIterator:
		return newStringAuxIterator(input, opt)
	case BooleanIterator:
		return newBooleanAuxIterator(input, opt)
	default:
		panic(fmt.Sprintf("unsupported aux iterator type: %T", input))
	}
}

// auxIteratorField represents an auxilary field within an AuxIterator.
type auxIteratorField struct {
	name string            // field name
	typ  influxql.DataType // detected data type
	itrs []Iterator        // auxillary iterators
	mu   sync.Mutex
	opt  IteratorOptions
}

func (f *auxIteratorField) append(itr Iterator) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.itrs = append(f.itrs, itr)
}

func (f *auxIteratorField) close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, itr := range f.itrs {
		itr.Close()
	}
}

type auxIteratorFields struct {
	fields     []*auxIteratorField
	dimensions []string
}

// newAuxIteratorFields returns a new instance of auxIteratorFields from a list of field names.
func newAuxIteratorFields(opt IteratorOptions) *auxIteratorFields {
	fields := make([]*auxIteratorField, len(opt.Aux))
	for i, ref := range opt.Aux {
		fields[i] = &auxIteratorField{name: ref.Val, typ: ref.Type, opt: opt}
	}
	return &auxIteratorFields{
		fields:     fields,
		dimensions: opt.GetDimensions(),
	}
}

func (a *auxIteratorFields) close() {
	for _, f := range a.fields {
		f.close()
	}
}

// iterator creates a new iterator for a named auxilary field.
func (a *auxIteratorFields) iterator(name string, typ influxql.DataType) Iterator {
	for _, f := range a.fields {
		// Skip field if it's name doesn't match.
		// Exit if no points were received by the iterator.
		if f.name != name || (typ != influxql.Unknown && f.typ != typ) {
			continue
		}

		// Create channel iterator by data type.
		switch f.typ {
		case influxql.Float:
			itr := &floatChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case influxql.Integer:
			itr := &integerChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case influxql.String, influxql.Tag:
			itr := &stringChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		case influxql.Boolean:
			itr := &booleanChanIterator{cond: sync.NewCond(&sync.Mutex{})}
			f.append(itr)
			return itr
		default:
			break
		}
	}

	return &nilFloatIterator{}
}

// send sends a point to all field iterators.
func (a *auxIteratorFields) send(p Point) (ok bool) {
	values := p.aux()
	for i, f := range a.fields {
		var v interface{}
		if i < len(values) {
			v = values[i]
		}

		tags := p.tags()
		tags = tags.Subset(a.dimensions)

		// Send new point for each aux iterator.
		// Primitive pointers represent nil values.
		for _, itr := range f.itrs {
			switch itr := itr.(type) {
			case *floatChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *integerChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *stringChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			case *booleanChanIterator:
				ok = itr.setBuf(p.name(), tags, p.time(), v) || ok
			default:
				panic(fmt.Sprintf("invalid aux itr type: %T", itr))
			}
		}
	}
	return ok
}

func (a *auxIteratorFields) sendError(err error) {
	for _, f := range a.fields {
		for _, itr := range f.itrs {
			switch itr := itr.(type) {
			case *floatChanIterator:
				itr.setErr(err)
			case *integerChanIterator:
				itr.setErr(err)
			case *stringChanIterator:
				itr.setErr(err)
			case *booleanChanIterator:
				itr.setErr(err)
			default:
				panic(fmt.Sprintf("invalid aux itr type: %T", itr))
			}
		}
	}
}

// DrainIterator reads and discards all points from itr.
func DrainIterator(itr Iterator) {
	defer itr.Close()
	switch itr := itr.(type) {
	case FloatIterator:
		for p, _ := itr.Next(); p != nil; p, _ = itr.Next() {
		}
	case IntegerIterator:
		for p, _ := itr.Next(); p != nil; p, _ = itr.Next() {
		}
	case StringIterator:
		for p, _ := itr.Next(); p != nil; p, _ = itr.Next() {
		}
	case BooleanIterator:
		for p, _ := itr.Next(); p != nil; p, _ = itr.Next() {
		}
	default:
		panic(fmt.Sprintf("unsupported iterator type for draining: %T", itr))
	}
}

// DrainIterators reads and discards all points from itrs.
func DrainIterators(itrs []Iterator) {
	defer Iterators(itrs).Close()
	for {
		var hasData bool

		for _, itr := range itrs {
			switch itr := itr.(type) {
			case FloatIterator:
				if p, _ := itr.Next(); p != nil {
					hasData = true
				}
			case IntegerIterator:
				if p, _ := itr.Next(); p != nil {
					hasData = true
				}
			case StringIterator:
				if p, _ := itr.Next(); p != nil {
					hasData = true
				}
			case BooleanIterator:
				if p, _ := itr.Next(); p != nil {
					hasData = true
				}
			default:
				panic(fmt.Sprintf("unsupported iterator type for draining: %T", itr))
			}
		}

		// Exit once all iterators return a nil point.
		if !hasData {
			break
		}
	}
}

// NewReaderIterator returns an iterator that streams from a reader.
func NewReaderIterator(r io.Reader, typ influxql.DataType, stats IteratorStats) Iterator {
	switch typ {
	case influxql.Float:
		return newFloatReaderIterator(r, stats)
	case influxql.Integer:
		return newIntegerReaderIterator(r, stats)
	case influxql.String:
		return newStringReaderIterator(r, stats)
	case influxql.Boolean:
		return newBooleanReaderIterator(r, stats)
	default:
		return &nilFloatReaderIterator{r: r}
	}
}

// IteratorCreator is an interface to create Iterators.
type IteratorCreator interface {
	// Creates a simple iterator for use in an InfluxQL query.
	CreateIterator(source *influxql.Measurement, opt IteratorOptions) (Iterator, error)

	// Determines the potential cost for creating an iterator.
	IteratorCost(source *influxql.Measurement, opt IteratorOptions) (IteratorCost, error)
}

// IteratorOptions is an object passed to CreateIterator to specify creation options.
type IteratorOptions struct {
	// Expression to iterate for.
	// This can be VarRef or a Call.
	Expr influxql.Expr

	// Auxilary tags or values to also retrieve for the point.
	Aux []influxql.VarRef

	// Data sources from which to receive data. This is only used for encoding
	// measurements over RPC and is no longer used in the open source version.
	Sources []influxql.Source

	// Group by interval and tags.
	Interval   Interval
	Dimensions []string            // The final dimensions of the query (stays the same even in subqueries).
	GroupBy    map[string]struct{} // Dimensions to group points by in intermediate iterators.
	Location   *time.Location

	// Fill options.
	Fill      influxql.FillOption
	FillValue interface{}

	// Condition to filter by.
	Condition influxql.Expr

	// Time range for the iterator.
	StartTime int64
	EndTime   int64

	// Sorted in time ascending order if true.
	Ascending bool

	// Limits the number of points per series.
	Limit, Offset int

	// Limits the number of series.
	SLimit, SOffset int

	// Removes the measurement name. Useful for meta queries.
	StripName bool

	// Removes duplicate rows from raw queries.
	Dedupe bool

	// Determines if this is a query for raw data or an aggregate/selector.
	Ordered bool

	// Limits on the creation of iterators.
	MaxSeriesN int

	// If this channel is set and is closed, the iterator should try to exit
	// and close as soon as possible.
	InterruptCh <-chan struct{}

	// Authorizer can limit access to data
	Authorizer Authorizer
}

// newIteratorOptionsStmt creates the iterator options from stmt.
func newIteratorOptionsStmt(stmt *influxql.SelectStatement, sopt SelectOptions) (opt IteratorOptions, err error) {
	// Determine time range from the condition.
	valuer := &influxql.NowValuer{Location: stmt.Location}
	condition, timeRange, err := influxql.ConditionExpr(stmt.Condition, valuer)
	if err != nil {
		return IteratorOptions{}, err
	}

	if !timeRange.Min.IsZero() {
		opt.StartTime = timeRange.Min.UnixNano()
	} else {
		opt.StartTime = influxql.MinTime
	}
	if !timeRange.Max.IsZero() {
		opt.EndTime = timeRange.Max.UnixNano()
	} else {
		opt.EndTime = influxql.MaxTime
	}
	opt.Location = stmt.Location

	// Determine group by interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return opt, err
	}
	// Set duration to zero if a negative interval has been used.
	if interval < 0 {
		interval = 0
	} else if interval > 0 {
		opt.Interval.Offset, err = stmt.GroupByOffset()
		if err != nil {
			return opt, err
		}
	}
	opt.Interval.Duration = interval

	// Always request an ordered output for the top level iterators.
	// The emitter will always emit points as ordered.
	opt.Ordered = true

	// Determine dimensions.
	opt.GroupBy = make(map[string]struct{}, len(opt.Dimensions))
	for _, d := range stmt.Dimensions {
		if d, ok := d.Expr.(*influxql.VarRef); ok {
			opt.Dimensions = append(opt.Dimensions, d.Val)
			opt.GroupBy[d.Val] = struct{}{}
		}
	}

	opt.Condition = condition
	opt.Ascending = stmt.TimeAscending()
	opt.Dedupe = stmt.Dedupe
	opt.StripName = stmt.StripName

	opt.Fill, opt.FillValue = stmt.Fill, stmt.FillValue
	if opt.Fill == influxql.NullFill && stmt.Target != nil {
		// Set the fill option to none if a target has been given.
		// Null values will get ignored when being written to the target
		// so fill(null) wouldn't write any null values to begin with.
		opt.Fill = influxql.NoFill
	}
	opt.Limit, opt.Offset = stmt.Limit, stmt.Offset
	opt.SLimit, opt.SOffset = stmt.SLimit, stmt.SOffset
	opt.MaxSeriesN = sopt.MaxSeriesN
	opt.InterruptCh = sopt.InterruptCh
	opt.Authorizer = sopt.Authorizer

	return opt, nil
}

func newIteratorOptionsSubstatement(stmt *influxql.SelectStatement, opt IteratorOptions) (IteratorOptions, error) {
	subOpt, err := newIteratorOptionsStmt(stmt, SelectOptions{})
	if err != nil {
		return IteratorOptions{}, err
	}

	if subOpt.StartTime < opt.StartTime {
		subOpt.StartTime = opt.StartTime
	}
	if subOpt.EndTime > opt.EndTime {
		subOpt.EndTime = opt.EndTime
	}
	// Propagate the dimensions to the inner subquery.
	subOpt.Dimensions = opt.Dimensions
	for d := range opt.GroupBy {
		subOpt.GroupBy[d] = struct{}{}
	}
	subOpt.InterruptCh = opt.InterruptCh

	// Extract the time range and condition from the condition.
	cond, t, err := influxql.ConditionExpr(stmt.Condition, nil)
	if err != nil {
		return IteratorOptions{}, err
	}
	subOpt.Condition = cond
	// If the time range is more constrained, use it instead. A less constrained time
	// range should be ignored.
	if !t.Min.IsZero() && t.MinTime() > opt.StartTime {
		subOpt.StartTime = t.MinTime()
	}
	if !t.Max.IsZero() && t.MaxTime() < opt.EndTime {
		subOpt.EndTime = t.MaxTime()
	}

	// Propagate the SLIMIT and SOFFSET from the outer query.
	subOpt.SLimit += opt.SLimit
	subOpt.SOffset += opt.SOffset

	// Propagate the ordering from the parent query.
	subOpt.Ascending = opt.Ascending

	// If the inner query uses a null fill option, switch it to none so we
	// don't hit an unnecessary penalty from the fill iterator. Null values
	// will end up getting stripped by an outer query anyway so there's no
	// point in having them here. We still need all other types of fill
	// iterators because they can affect the result of the outer query.
	if subOpt.Fill == influxql.NullFill {
		subOpt.Fill = influxql.NoFill
	}

	// Inherit the ordering method from the outer query.
	subOpt.Ordered = opt.Ordered

	// If there is no interval for this subquery, but the outer query has an
	// interval, inherit the parent interval.
	interval, err := stmt.GroupByInterval()
	if err != nil {
		return IteratorOptions{}, err
	} else if interval == 0 {
		subOpt.Interval = opt.Interval
	}
	return subOpt, nil
}

// MergeSorted returns true if the options require a sorted merge.
func (opt IteratorOptions) MergeSorted() bool {
	return opt.Ordered
}

// SeekTime returns the time the iterator should start from.
// For ascending iterators this is the start time, for descending iterators it's the end time.
func (opt IteratorOptions) SeekTime() int64 {
	if opt.Ascending {
		return opt.StartTime
	}
	return opt.EndTime
}

// Window returns the time window [start,end) that t falls within.
func (opt IteratorOptions) Window(t int64) (start, end int64) {
	if opt.Interval.IsZero() {
		return opt.StartTime, opt.EndTime + 1
	}

	// Subtract the offset to the time so we calculate the correct base interval.
	t -= int64(opt.Interval.Offset)

	// Retrieve the zone offset for the start time.
	var zone int64
	if opt.Location != nil {
		_, zone = opt.Zone(t)
	}

	// Truncate time by duration.
	dt := (t + zone) % int64(opt.Interval.Duration)
	if dt < 0 {
		// Negative modulo rounds up instead of down, so offset
		// with the duration.
		dt += int64(opt.Interval.Duration)
	}

	// Find the start time.
	if influxql.MinTime+dt >= t {
		start = influxql.MinTime
	} else {
		start = t - dt
	}

	// Look for the start offset again because the first time may have been
	// after the offset switch. Now that we are at midnight in UTC, we can
	// lookup the zone offset again to get the real starting offset.
	if opt.Location != nil {
		_, startOffset := opt.Zone(start)
		// Do not adjust the offset if the offset change is greater than or
		// equal to the duration.
		if o := zone - startOffset; o != 0 && abs(o) < int64(opt.Interval.Duration) {
			start += o
		}
	}
	start += int64(opt.Interval.Offset)

	// Find the end time.
	if dt := int64(opt.Interval.Duration) - dt; influxql.MaxTime-dt <= t {
		end = influxql.MaxTime
	} else {
		end = t + dt
	}

	// Retrieve the zone offset for the end time.
	if opt.Location != nil {
		_, endOffset := opt.Zone(end)
		// Adjust the end time if the offset is different from the start offset.
		// Only apply the offset if it is smaller than the duration.
		// This prevents going back in time and creating time windows
		// that don't make any sense.
		if o := zone - endOffset; o != 0 && abs(o) < int64(opt.Interval.Duration) {
			// If the offset is greater than 0, that means we are adding time.
			// Added time goes into the previous interval because the clocks
			// move backwards. If the offset is less than 0, then we are skipping
			// time. Skipped time comes after the switch so if we have a time
			// interval that lands on the switch, it comes from the next
			// interval and not the current one. For this reason, we need to know
			// when the actual switch happens by seeing if the time switch is within
			// the current interval. We calculate the zone offset with the offset
			// and see if the value is the same. If it is, we apply the
			// offset.
			if o > 0 {
				end += o
			} else if _, z := opt.Zone(end + o); z == endOffset {
				end += o
			}
		}
	}
	end += int64(opt.Interval.Offset)
	return
}

// DerivativeInterval returns the time interval for the derivative function.
func (opt IteratorOptions) DerivativeInterval() Interval {
	// Use the interval on the derivative() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	// Otherwise use the group by interval, if specified.
	if opt.Interval.Duration > 0 {
		return Interval{Duration: opt.Interval.Duration}
	}

	return Interval{Duration: time.Second}
}

// ElapsedInterval returns the time interval for the elapsed function.
func (opt IteratorOptions) ElapsedInterval() Interval {
	// Use the interval on the elapsed() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	return Interval{Duration: time.Nanosecond}
}

// IntegralInterval returns the time interval for the integral function.
func (opt IteratorOptions) IntegralInterval() Interval {
	// Use the interval on the integral() call, if specified.
	if expr, ok := opt.Expr.(*influxql.Call); ok && len(expr.Args) == 2 {
		return Interval{Duration: expr.Args[1].(*influxql.DurationLiteral).Val}
	}

	return Interval{Duration: time.Second}
}

// GetDimensions retrieves the dimensions for this query.
func (opt IteratorOptions) GetDimensions() []string {
	if len(opt.GroupBy) > 0 {
		dimensions := make([]string, 0, len(opt.GroupBy))
		for dim := range opt.GroupBy {
			dimensions = append(dimensions, dim)
		}
		return dimensions
	}
	return opt.Dimensions
}

// Zone returns the zone information for the given time. The offset is in nanoseconds.
func (opt *IteratorOptions) Zone(ns int64) (string, int64) {
	if opt.Location == nil {
		return "", 0
	}

	t := time.Unix(0, ns).In(opt.Location)
	name, offset := t.Zone()
	return name, secToNs * int64(offset)
}

// MarshalBinary encodes opt into a binary format.
func (opt *IteratorOptions) MarshalBinary() ([]byte, error) {
	return proto.Marshal(encodeIteratorOptions(opt))
}

// UnmarshalBinary decodes from a binary format in to opt.
func (opt *IteratorOptions) UnmarshalBinary(buf []byte) error {
	var pb internal.IteratorOptions
	if err := proto.Unmarshal(buf, &pb); err != nil {
		return err
	}

	other, err := decodeIteratorOptions(&pb)
	if err != nil {
		return err
	}
	*opt = *other

	return nil
}

func encodeIteratorOptions(opt *IteratorOptions) *internal.IteratorOptions {
	pb := &internal.IteratorOptions{
		Interval:   encodeInterval(opt.Interval),
		Dimensions: opt.Dimensions,
		Fill:       proto.Int32(int32(opt.Fill)),
		StartTime:  proto.Int64(opt.StartTime),
		EndTime:    proto.Int64(opt.EndTime),
		Ascending:  proto.Bool(opt.Ascending),
		Limit:      proto.Int64(int64(opt.Limit)),
		Offset:     proto.Int64(int64(opt.Offset)),
		SLimit:     proto.Int64(int64(opt.SLimit)),
		SOffset:    proto.Int64(int64(opt.SOffset)),
		StripName:  proto.Bool(opt.StripName),
		Dedupe:     proto.Bool(opt.Dedupe),
		MaxSeriesN: proto.Int64(int64(opt.MaxSeriesN)),
		Ordered:    proto.Bool(opt.Ordered),
	}

	// Set expression, if set.
	if opt.Expr != nil {
		pb.Expr = proto.String(opt.Expr.String())
	}

	// Set the location, if set.
	if opt.Location != nil {
		pb.Location = proto.String(opt.Location.String())
	}

	// Convert and encode aux fields as variable references.
	if opt.Aux != nil {
		pb.Fields = make([]*internal.VarRef, len(opt.Aux))
		pb.Aux = make([]string, len(opt.Aux))
		for i, ref := range opt.Aux {
			pb.Fields[i] = encodeVarRef(ref)
			pb.Aux[i] = ref.Val
		}
	}

	// Encode group by dimensions from a map.
	if opt.GroupBy != nil {
		dimensions := make([]string, 0, len(opt.GroupBy))
		for dim := range opt.GroupBy {
			dimensions = append(dimensions, dim)
		}
		pb.GroupBy = dimensions
	}

	// Convert and encode sources to measurements.
	if opt.Sources != nil {
		sources := make([]*internal.Measurement, len(opt.Sources))
		for i, source := range opt.Sources {
			mm := source.(*influxql.Measurement)
			sources[i] = encodeMeasurement(mm)
		}
		pb.Sources = sources
	}

	// Fill value can only be a number. Set it if available.
	if v, ok := opt.FillValue.(float64); ok {
		pb.FillValue = proto.Float64(v)
	}

	// Set condition, if set.
	if opt.Condition != nil {
		pb.Condition = proto.String(opt.Condition.String())
	}

	return pb
}

func decodeIteratorOptions(pb *internal.IteratorOptions) (*IteratorOptions, error) {
	opt := &IteratorOptions{
		Interval:   decodeInterval(pb.GetInterval()),
		Dimensions: pb.GetDimensions(),
		Fill:       influxql.FillOption(pb.GetFill()),
		StartTime:  pb.GetStartTime(),
		EndTime:    pb.GetEndTime(),
		Ascending:  pb.GetAscending(),
		Limit:      int(pb.GetLimit()),
		Offset:     int(pb.GetOffset()),
		SLimit:     int(pb.GetSLimit()),
		SOffset:    int(pb.GetSOffset()),
		StripName:  pb.GetStripName(),
		Dedupe:     pb.GetDedupe(),
		MaxSeriesN: int(pb.GetMaxSeriesN()),
		Ordered:    pb.GetOrdered(),
	}

	// Set expression, if set.
	if pb.Expr != nil {
		expr, err := influxql.ParseExpr(pb.GetExpr())
		if err != nil {
			return nil, err
		}
		opt.Expr = expr
	}

	if pb.Location != nil {
		loc, err := time.LoadLocation(pb.GetLocation())
		if err != nil {
			return nil, err
		}
		opt.Location = loc
	}

	// Convert and decode variable references.
	if fields := pb.GetFields(); fields != nil {
		opt.Aux = make([]influxql.VarRef, len(fields))
		for i, ref := range fields {
			opt.Aux[i] = decodeVarRef(ref)
		}
	} else if aux := pb.GetAux(); aux != nil {
		opt.Aux = make([]influxql.VarRef, len(aux))
		for i, name := range aux {
			opt.Aux[i] = influxql.VarRef{Val: name}
		}
	}

	// Convert and decode sources to measurements.
	if pb.Sources != nil {
		sources := make([]influxql.Source, len(pb.GetSources()))
		for i, source := range pb.GetSources() {
			mm, err := decodeMeasurement(source)
			if err != nil {
				return nil, err
			}
			sources[i] = mm
		}
		opt.Sources = sources
	}

	// Convert group by dimensions to a map.
	if pb.GroupBy != nil {
		dimensions := make(map[string]struct{}, len(pb.GroupBy))
		for _, dim := range pb.GetGroupBy() {
			dimensions[dim] = struct{}{}
		}
		opt.GroupBy = dimensions
	}

	// Set the fill value, if set.
	if pb.FillValue != nil {
		opt.FillValue = pb.GetFillValue()
	}

	// Set condition, if set.
	if pb.Condition != nil {
		expr, err := influxql.ParseExpr(pb.GetCondition())
		if err != nil {
			return nil, err
		}
		opt.Condition = expr
	}

	return opt, nil
}

func encodeMeasurement(mm *influxql.Measurement) *internal.Measurement {
	pb := &internal.Measurement{
		Database:        proto.String(mm.Database),
		RetentionPolicy: proto.String(mm.RetentionPolicy),
		Name:            proto.String(mm.Name),
		IsTarget:        proto.Bool(mm.IsTarget),
	}
	if mm.Regex != nil {
		pb.Regex = proto.String(mm.Regex.Val.String())
	}
	return pb
}

func decodeMeasurement(pb *internal.Measurement) (*influxql.Measurement, error) {
	mm := &influxql.Measurement{
		Database:        pb.GetDatabase(),
		RetentionPolicy: pb.GetRetentionPolicy(),
		Name:            pb.GetName(),
		IsTarget:        pb.GetIsTarget(),
	}

	if pb.Regex != nil {
		regex, err := regexp.Compile(pb.GetRegex())
		if err != nil {
			return nil, fmt.Errorf("invalid binary measurement regex: value=%q, err=%s", pb.GetRegex(), err)
		}
		mm.Regex = &influxql.RegexLiteral{Val: regex}
	}

	return mm, nil
}

// selectInfo represents an object that stores info about select fields.
type selectInfo struct {
	calls map[*influxql.Call]struct{}
	refs  map[*influxql.VarRef]struct{}
}

// newSelectInfo creates a object with call and var ref info from stmt.
func newSelectInfo(stmt *influxql.SelectStatement) *selectInfo {
	info := &selectInfo{
		calls: make(map[*influxql.Call]struct{}),
		refs:  make(map[*influxql.VarRef]struct{}),
	}
	influxql.Walk(info, stmt.Fields)
	return info
}

func (v *selectInfo) Visit(n influxql.Node) influxql.Visitor {
	switch n := n.(type) {
	case *influxql.Call:
		v.calls[n] = struct{}{}
		return nil
	case *influxql.VarRef:
		v.refs[n] = struct{}{}
		return nil
	}
	return v
}

// FindSelector returns a selector from the selectInfo. This will only
// return a selector if the Call is a selector and it's the only function
// in the selectInfo.
func (v *selectInfo) FindSelector() *influxql.Call {
	if len(v.calls) != 1 {
		return nil
	}

	for s := range v.calls {
		if influxql.IsSelector(s) {
			return s
		}
	}
	return nil
}

// Interval represents a repeating interval for a query.
type Interval struct {
	Duration time.Duration
	Offset   time.Duration
}

// IsZero returns true if the interval has no duration.
func (i Interval) IsZero() bool { return i.Duration == 0 }

func encodeInterval(i Interval) *internal.Interval {
	return &internal.Interval{
		Duration: proto.Int64(i.Duration.Nanoseconds()),
		Offset:   proto.Int64(i.Offset.Nanoseconds()),
	}
}

func decodeInterval(pb *internal.Interval) Interval {
	return Interval{
		Duration: time.Duration(pb.GetDuration()),
		Offset:   time.Duration(pb.GetOffset()),
	}
}

func encodeVarRef(ref influxql.VarRef) *internal.VarRef {
	return &internal.VarRef{
		Val:  proto.String(ref.Val),
		Type: proto.Int32(int32(ref.Type)),
	}
}

func decodeVarRef(pb *internal.VarRef) influxql.VarRef {
	return influxql.VarRef{
		Val:  pb.GetVal(),
		Type: influxql.DataType(pb.GetType()),
	}
}

type nilFloatIterator struct{}

func (*nilFloatIterator) Stats() IteratorStats       { return IteratorStats{} }
func (*nilFloatIterator) Close() error               { return nil }
func (*nilFloatIterator) Next() (*FloatPoint, error) { return nil, nil }

type nilFloatReaderIterator struct {
	r io.Reader
}

func (*nilFloatReaderIterator) Stats() IteratorStats { return IteratorStats{} }
func (itr *nilFloatReaderIterator) Close() error {
	if r, ok := itr.r.(io.ReadCloser); ok {
		itr.r = nil
		return r.Close()
	}
	return nil
}
func (*nilFloatReaderIterator) Next() (*FloatPoint, error) { return nil, nil }

// integerFloatTransformIterator executes a function to modify an existing point for every
// output of the input iterator.
type integerFloatTransformIterator struct {
	input IntegerIterator
	fn    integerFloatTransformFunc
}

// Stats returns stats from the input iterator.
func (itr *integerFloatTransformIterator) Stats() IteratorStats { return itr.input.Stats() }

// Close closes the iterator and all child iterators.
func (itr *integerFloatTransformIterator) Close() error { return itr.input.Close() }

// Next returns the minimum value for the next available interval.
func (itr *integerFloatTransformIterator) Next() (*FloatPoint, error) {
	p, err := itr.input.Next()
	if err != nil {
		return nil, err
	} else if p != nil {
		return itr.fn(p), nil
	}
	return nil, nil
}

// integerFloatTransformFunc creates or modifies a point.
// The point passed in may be modified and returned rather than allocating a
// new point if possible.
type integerFloatTransformFunc func(p *IntegerPoint) *FloatPoint

type integerFloatCastIterator struct {
	input IntegerIterator
}

func (itr *integerFloatCastIterator) Stats() IteratorStats { return itr.input.Stats() }
func (itr *integerFloatCastIterator) Close() error         { return itr.input.Close() }
func (itr *integerFloatCastIterator) Next() (*FloatPoint, error) {
	p, err := itr.input.Next()
	if p == nil || err != nil {
		return nil, err
	}

	return &FloatPoint{
		Name:  p.Name,
		Tags:  p.Tags,
		Time:  p.Time,
		Nil:   p.Nil,
		Value: float64(p.Value),
		Aux:   p.Aux,
	}, nil
}

// IteratorStats represents statistics about an iterator.
// Some statistics are available immediately upon iterator creation while
// some are derived as the iterator processes data.
type IteratorStats struct {
	SeriesN int // series represented
	PointN  int // points returned
}

// Add aggregates fields from s and other together. Overwrites s.
func (s *IteratorStats) Add(other IteratorStats) {
	s.SeriesN += other.SeriesN
	s.PointN += other.PointN
}

func encodeIteratorStats(stats *IteratorStats) *internal.IteratorStats {
	return &internal.IteratorStats{
		SeriesN: proto.Int64(int64(stats.SeriesN)),
		PointN:  proto.Int64(int64(stats.PointN)),
	}
}

func decodeIteratorStats(pb *internal.IteratorStats) IteratorStats {
	return IteratorStats{
		SeriesN: int(pb.GetSeriesN()),
		PointN:  int(pb.GetPointN()),
	}
}

// IteratorCost contains statistics retrieved for explaining what potential
// cost may be incurred by instantiating an iterator.
type IteratorCost struct {
	// The total number of shards that are touched by this query.
	NumShards int64

	// The total number of non-unique series that are accessed by this query.
	// This number matches the number of cursors created by the query since
	// one cursor is created for every series.
	NumSeries int64

	// CachedValues returns the number of cached values that may be read by this
	// query.
	CachedValues int64

	// The total number of non-unique files that may be accessed by this query.
	// This will count the number of files accessed by each series so files
	// will likely be double counted.
	NumFiles int64

	// The number of blocks that had the potential to be accessed.
	BlocksRead int64

	// The amount of data that can be potentially read.
	BlockSize int64
}

// Combine combines the results of two IteratorCost structures into one.
func (c IteratorCost) Combine(other IteratorCost) IteratorCost {
	return IteratorCost{
		NumShards:    c.NumShards + other.NumShards,
		NumSeries:    c.NumSeries + other.NumSeries,
		CachedValues: c.CachedValues + other.CachedValues,
		NumFiles:     c.NumFiles + other.NumFiles,
		BlocksRead:   c.BlocksRead + other.BlocksRead,
		BlockSize:    c.BlockSize + other.BlockSize,
	}
}

// floatFastDedupeIterator outputs unique points where the point has a single aux field.
type floatFastDedupeIterator struct {
	input FloatIterator
	m     map[fastDedupeKey]struct{} // lookup of points already sent
}

// newFloatFastDedupeIterator returns a new instance of floatFastDedupeIterator.
func newFloatFastDedupeIterator(input FloatIterator) *floatFastDedupeIterator {
	return &floatFastDedupeIterator{
		input: input,
		m:     make(map[fastDedupeKey]struct{}),
	}
}

// Stats returns stats from the input iterator.
func (itr *floatFastDedupeIterator) Stats() IteratorStats { return itr.input.Stats() }

// Close closes the iterator and all child iterators.
func (itr *floatFastDedupeIterator) Close() error { return itr.input.Close() }

// Next returns the next unique point from the input iterator.
func (itr *floatFastDedupeIterator) Next() (*FloatPoint, error) {
	for {
		// Read next point.
		// Skip if there are not any aux fields.
		p, err := itr.input.Next()
		if p == nil || err != nil {
			return nil, err
		} else if len(p.Aux) == 0 {
			continue
		}

		// If the point has already been output then move to the next point.
		key := fastDedupeKey{name: p.Name}
		key.values[0] = p.Aux[0]
		if len(p.Aux) > 1 {
			key.values[1] = p.Aux[1]
		}
		if _, ok := itr.m[key]; ok {
			continue
		}

		// Otherwise mark it as emitted and return point.
		itr.m[key] = struct{}{}
		return p, nil
	}
}

type fastDedupeKey struct {
	name   string
	values [2]interface{}
}

type reverseStringSlice []string

func (p reverseStringSlice) Len() int           { return len(p) }
func (p reverseStringSlice) Less(i, j int) bool { return p[i] > p[j] }
func (p reverseStringSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}
