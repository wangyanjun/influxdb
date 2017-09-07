package tsm1

import (
	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
)

func (e *Engine) CreateCursor(r *tsdb.CursorRequest) (tsdb.Cursor, error) {
	// Look up fields for measurement.
	mf := e.fieldset.Fields(r.Measurement)
	if mf == nil {
		return nil, nil
	}

	// Find individual field.
	f := mf.Field(r.Field)
	if f == nil {
		// field doesn't exist for this measurement
		return nil, nil
	}

	var opt query.IteratorOptions
	opt.Ascending = r.Ascending
	opt.StartTime = r.StartTime
	opt.EndTime = r.EndTime
	var t int64
	if r.Ascending {
		t = r.EndTime
	} else {
		t = r.StartTime
	}

	// Return appropriate cursor based on type.
	switch f.Type {
	case influxql.Float:
		return newFloatRangeBatchCursor(r.Series, t, r.Ascending, e.buildFloatBatchCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.Integer:
		return newIntegerRangeBatchCursor(r.Series, t, r.Ascending, e.buildIntegerBatchCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.Unsigned:
		return newUnsignedRangeBatchCursor(r.Series, t, r.Ascending, e.buildUnsignedBatchCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.String:
		return newStringRangeBatchCursor(r.Series, t, r.Ascending, e.buildStringBatchCursor(r.Measurement, r.Series, r.Field, opt)), nil

	case influxql.Boolean:
		return newBooleanRangeBatchCursor(r.Series, t, r.Ascending, e.buildBooleanBatchCursor(r.Measurement, r.Series, r.Field, opt)), nil

	default:
		panic("unreachable")
	}
}
