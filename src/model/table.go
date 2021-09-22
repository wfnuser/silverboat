package model

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

type Table struct {
	db  fdb.Database
	row, col subspace.Subspace
}

func  NewTable(tableName string, db fdb.Database) *Table {
	return &Table {
		db: db,
		row: subspace.Sub("TBL").Sub(tableName).Sub("row"),
		col: subspace.Sub("TBL").Sub(tableName).Sub("col"),
	}
}

func (tbl Table) TableSetCell(row, column int, value interface{}) {
	tbl.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(tbl.row.Pack(tuple.Tuple{row, column}), _pack(value))
		tr.Set(tbl.col.Pack(tuple.Tuple{column, row}), _pack(value))
		return nil, nil
	})
}

func (tbl Table) TableGetCell(row, column int) interface{} {
	item, _ := tbl.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		i := rtr.Get(tbl.row.Pack(tuple.Tuple{row, column})).MustGet()
		return i, nil
	})
	return _unpack(item.([]byte))[0]
}

func (tbl Table) TableSetRow(row int, cols ...interface{}) {
	tbl.db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kr, err := fdb.PrefixRange(tbl.row.Pack(tuple.Tuple{row}))
		if err != nil {
			return nil, err
		}

		tr.ClearRange(kr)

		for c, v := range cols {
			tbl.TableSetCell(row, c, v)
		}
		return nil, nil
	})
	return
}

func (tbl Table) TableGetRow(row int) ([]interface{}, error) {
	item, err := tbl.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kr, e := fdb.PrefixRange(tbl.row.Pack(tuple.Tuple{row}))
		if e != nil {
			return nil, e
		}

		slice, e := rtr.GetRange(kr, fdb.RangeOptions{Mode: -1}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		ret := make([]interface{}, len(slice))

		for i, v := range slice {
			ret[i] = _unpack(v.Value)[0]
		}

		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return item.([]interface{}), nil
}

func (tbl Table) TableGetCol(col int) ([]interface{}, error) {
	item, err := tbl.db.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kr, e := fdb.PrefixRange(tbl.col.Pack(tuple.Tuple{col}))
		if e != nil {
			return nil, e
		}

		slice, e := rtr.GetRange(kr, fdb.RangeOptions{Mode: -1}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		ret := make([]interface{}, len(slice))

		for i, v := range slice {
			ret[i] = _unpack(v.Value)[0]
		}

		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return item.([]interface{}), nil
}
