package storage

import (
	"bytes"

	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb"
)

type readRequest struct {
	start, end int64
	asc        bool
	limit      uint64
	aggregate  *Aggregate
}

type ResultSet struct {
	req readRequest
	cur seriesCursor
	row seriesRow
}

func (r *ResultSet) Close() {
	r.row.shards = nil
	r.cur.Close()
}

func (r *ResultSet) Next() bool {
	row := r.cur.Next()
	if row == nil {
		return false
	}

	r.row = *row

	return true
}

func (r *ResultSet) Cursor() tsdb.Cursor {
	cur := newMultiShardBatchCursor(r.row, &r.req)
	if r.req.aggregate != nil {
		cur = newAggregateBatchCursor(r.req.aggregate, cur)
	}
	return cur
}

func (r *ResultSet) Tags() models.Tags {
	return r.row.tags
}

func (r *ResultSet) SeriesKey() string {
	// TODO(sgc): this must escape
	var buf bytes.Buffer
	for _, tag := range r.row.tags {
		buf.Write(tag.Key)
		buf.WriteByte(':')
		buf.Write(tag.Value)
		buf.WriteByte(',')
	}
	s := buf.String()
	return s[:len(s)-1]
}
