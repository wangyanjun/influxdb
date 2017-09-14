package storage

import (
	"errors"

	"bytes"
	"sort"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/query"
	"github.com/influxdata/influxdb/tsdb"
)

var (
	measurementKey = []byte("_measurement")
	fieldKey       = []byte("_field")
)

type seriesCursor interface {
	Close()
	Next() *seriesRow
	Err() error
}

type seriesRow struct {
	measurement, key, field string
	tags                    models.Tags
	shards                  []*tsdb.Shard
	valueCond               influxql.Expr
}

type mapValuer map[string]string

var _ influxql.Valuer = mapValuer(nil)

func (vs mapValuer) Value(key string) (interface{}, bool) {
	v, ok := vs[key]
	return v, ok
}

type indexSeriesCursor struct {
	shards          []*tsdb.Shard
	sitr            query.FloatIterator
	fields          []string
	nf              []string
	err             error
	eof             bool
	tags            models.Tags
	filterset       mapValuer
	cond            influxql.Expr
	measurementCond influxql.Expr
	row             seriesRow
}

func newIndexSeriesCursor(req *ReadRequest, shards []*tsdb.Shard) (*indexSeriesCursor, error) {
	opt := query.IteratorOptions{
		Aux:        []influxql.VarRef{{Val: "key"}},
		Authorizer: query.OpenAuthorizer{},
		Ordered:    true,
	}
	p := &indexSeriesCursor{row: seriesRow{shards: shards}, shards: shards}

	var err error

	if root := req.Predicate.GetRoot(); root != nil {
		p.cond, err = NodeToExpr(root)
		if err != nil {
			return nil, err
		}

		if !HasFieldKeyOrValue(p.cond) {
			p.measurementCond = p.cond
			opt.Condition = p.cond
		} else {
			p.measurementCond = influxql.Reduce(RewriteExprRemoveFieldValue(influxql.CloneExpr(p.cond)), nil)
			if isBooleanLiteral(p.measurementCond) {
				p.measurementCond = nil
			}

			opt.Condition = influxql.Reduce(RewriteExprRemoveFieldKeyAndValue(influxql.CloneExpr(p.cond)), nil)
			if isBooleanLiteral(opt.Condition) {
				opt.Condition = nil
			}
		}
	}

	sg := tsdb.Shards(shards)
	if itr, err := sg.CreateIterator("_series", opt); err != nil {
		return nil, err
	} else {
		// TODO(sgc): need to rethink how we enumerate series across shards; dedupe is inefficient
		itr = query.NewDedupeIterator(itr)

		p.sitr, err = toFloatIterator(itr, nil)
		if err != nil {
			return nil, err
		}
	}

	if itr, err := sg.CreateIterator("_fieldKeys", opt); err != nil {
		return nil, err
	} else {
		fitr, err := toFloatIterator(itr, nil)
		if err != nil {
			return nil, err
		}

		p.fields = extractFields(fitr)
		fitr.Close()
	}

	return p, nil
}

func (c *indexSeriesCursor) Close() {
	c.eof = true
	c.sitr.Close()
	c.sitr = nil
}

// Next returns the next series
func (c *indexSeriesCursor) Next() *seriesRow {
	if c.eof {
		return nil
	}

RETRY:
	if len(c.nf) == 0 {
		// next series key
		fp, err := c.sitr.Next()
		if err != nil {
			c.err = err
			c.eof = true
			return nil
		} else if fp == nil {
			c.eof = true
			return nil
		}

		key, ok := fp.Aux[0].(string)
		if !ok {
			c.err = errors.New("expected string for series key")
			c.eof = true
			return nil
		}
		keyb := []byte(key)
		mm, _ := models.ParseName(keyb)
		c.row.measurement = string(mm)
		c.tags, _ = models.ParseTags(keyb)

		c.filterset = mapValuer{"_name": c.row.measurement}
		for _, tag := range c.tags {
			c.filterset[string(tag.Key)] = string(tag.Value)
		}

		c.tags.Set(measurementKey, mm)
		c.row.key = key
		c.nf = c.fields
	}

	c.row.field, c.nf = c.nf[0], c.nf[1:]
	c.filterset["_field"] = c.row.field

	if c.measurementCond != nil && !evalExprBool(c.measurementCond, c.filterset) {
		goto RETRY
	}

	c.tags.Set(fieldKey, []byte(c.row.field))

	if c.cond != nil {
		// TODO(sgc): lazily evaluate valueCond
		c.row.valueCond = influxql.Reduce(influxql.CloneExpr(c.cond), c.filterset)
		if isBooleanLiteral(c.row.valueCond) {
			// we've reduced the expression to "true"
			c.row.valueCond = nil
		}
	}

	c.row.tags = c.tags.Clone()

	return &c.row
}

func (c *indexSeriesCursor) Err() error {
	return c.err
}

type limitSeriesCursor struct {
	seriesCursor
	n, o, c uint64
}

func newLimitSeriesCursor(cur seriesCursor, n, o uint64) *limitSeriesCursor {
	return &limitSeriesCursor{seriesCursor: cur, o: o, n: n}
}

func (c *limitSeriesCursor) Next() *seriesRow {
	if c.o > 0 {
		for i := uint64(0); i < c.o; i++ {
			if c.seriesCursor.Next() == nil {
				break
			}
		}
		c.o = 0
	}

	if c.c >= c.n {
		return nil
	}
	c.c++
	return c.seriesCursor.Next()
}

type groupSeriesCursor struct {
	seriesCursor
	rows []seriesRow
	keys [][]byte
	f    bool
}

func newGroupSeriesCursor(cur seriesCursor, keys []string) *groupSeriesCursor {
	g := &groupSeriesCursor{seriesCursor: cur}

	g.keys = make([][]byte, 0, len(keys))
	for _, k := range keys {
		g.keys = append(g.keys, []byte(k))
	}

	return g
}

func (c *groupSeriesCursor) Next() *seriesRow {
	if !c.f {
		c.sort()
	}

	if len(c.rows) > 0 {
		row := &c.rows[0]
		c.rows = c.rows[1:]
		return row
	}

	return nil
}

func (c *groupSeriesCursor) sort() {
	var rows []seriesRow
	row := c.seriesCursor.Next()
	for row != nil {
		rows = append(rows, *row)
		row = c.seriesCursor.Next()
	}

	sort.Slice(rows, func(i, j int) bool {
		for _, k := range c.keys {
			ik := rows[i].tags.Get(k)
			jk := rows[j].tags.Get(k)
			cmp := bytes.Compare(ik, jk)
			if cmp == 0 {
				continue
			}
			return cmp == -1
		}

		return false
	})

	c.rows = rows

	// free early
	c.seriesCursor.Close()
	c.seriesCursor = nil
	c.f = true
}

func isBooleanLiteral(expr influxql.Expr) bool {
	_, ok := expr.(*influxql.BooleanLiteral)
	return ok
}

func toFloatIterator(iter query.Iterator, err error) (query.FloatIterator, error) {
	if err != nil {
		return nil, err
	}

	sitr, ok := iter.(query.FloatIterator)
	if !ok {
		return nil, errors.New("expected FloatIterator")
	}

	return sitr, nil
}

func extractFields(itr query.FloatIterator) []string {
	var a []string
	for {
		p, err := itr.Next()
		if err != nil {
			return nil
		} else if p == nil {
			break
		} else if f, ok := p.Aux[0].(string); ok {
			a = append(a, f)
		}
	}

	sort.Strings(a)
	i := 1
	// TODO(sgc): skip whilst a[i] != a[i-1] to avoid unnecessary copying
	for j := 1; j < len(a); j++ {
		if a[j] != a[j-1] {
			a[i] = a[j]
			i++
		}
	}

	return a[:i]
}
