package hopper

import (
	"fmt"
	"strings"

	"go.etcd.io/bbolt"
)

type comparison func(a, b any) bool

func eq(a, b any) bool {
	return a == b
}

func neq(a, b any) bool {
	return a != b
}

func gt(a, b any) bool {
	af, err := anyToFloat(a)
	if err != nil {
		return false
	}
	bf, err := anyToFloat(b)
	if err != nil {
		return false
	}
	return af > bf
}

func lt(a, b any) bool {
	af, err := anyToFloat(a)
	if err != nil {
		return false
	}
	bf, err := anyToFloat(b)
	if err != nil {
		return false
	}
	return af < bf
}

func contains(a, b any) bool {
	switch a.(type) {
	case string:
		if _, ok := b.(string); ok {
			return strings.Contains(a.(string), b.(string))
		}
	case []any:
		if _, ok := b.([]any); ok {
			return containsSlice(a.([]any), b.([]any))
		}
	}
	return false
}

func containsSlice(a, b []any) bool {
	if len(b) == 0 {
		return true
	}
	for i := 0; i <= len(a)-len(b); i++ {
		match := true
		for j := 0; j < len(b); j++ {
			if a[i+j] != b[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

type filterAtom struct {
	kvs  Map
	comp comparison
	weak bool // if false, this filter is imperative to the whole chain
}

func newFilterAtom(kvs Map, comp comparison, weak bool) filterAtom {
	return filterAtom{
		kvs,
		comp,
		weak,
	}
}

type Filter struct {
	db      *bbolt.DB
	coll    string
	limit   uint
	filters []filterAtom
}

func NewFilter(db *bbolt.DB, coll string, limit uint) *Filter {
	return &Filter{
		db:      db,
		coll:    coll,
		limit:   limit,
		filters: make([]filterAtom, 0),
	}
}

func (f *Filter) Eq(kvs Map, weak bool) {
	fa := newFilterAtom(kvs, eq, weak)
	f.filters = append(f.filters, fa)
}

func (f *Filter) Neq(kvs Map, weak bool) {
	fa := newFilterAtom(kvs, neq, weak)
	f.filters = append(f.filters, fa)
}

func (f *Filter) Gt(kvs Map, weak bool) {
	fa := newFilterAtom(kvs, gt, weak)
	f.filters = append(f.filters, fa)
}

func (f *Filter) Lt(kvs Map, weak bool) {
	fa := newFilterAtom(kvs, lt, weak)
	f.filters = append(f.filters, fa)
}

func (f *Filter) Contains(kvs Map, weak bool) {
	fa := newFilterAtom(kvs, contains, weak)
	f.filters = append(f.filters, fa)
}

func (f *Filter) Exec() ([]Map, error) {
	tx, err := f.db.Begin(false)
	if err != nil {
		return nil, err
	}
	b := tx.Bucket([]byte(f.coll))
	if b == nil {
		return nil, fmt.Errorf("collection (%s) not found", f.coll)
	}
	results := make([]Map, 0)
	cursor := b.Cursor()
	k, v := cursor.First()
	for len(results) < int(f.limit) && k != nil {
		data, err := UnmarshalKV(k, v)
		if err != nil {
			return nil, err
		}
		if includeMany(data, f.filters) {
			results = append(results, data)
		}
		k, v = cursor.Next()
	}
	tx.Commit()
	return results, nil
}

// TODO: Sort Results

func include(data Map, f filterAtom) bool {
	for fk, fv := range f.kvs {
		if v, ok := data[fk]; ok {
			if !f.comp(v, fv) {
				return false
			}
		} else {
			return false
		}
	}
	return true
}

func includeMany(data Map, fs []filterAtom) bool {
	inc := false
	for _, f := range fs {
		i := include(data, f)
		if i {
			inc = i
		}
		if !i && !f.weak {
			return false
		}
	}
	return inc
}
