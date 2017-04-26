//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @project queryman
// @author 1100282
// @date 2017. 4. 26. PM 8:37
//

package queryman

import (
	"reflect"
	"database/sql"
	"fmt"
)

type QueryResult struct {
	pstmt              *sql.Stmt
	err                error
	rows               *sql.Rows
	fieldNameConverter FieldNameConvertStrategy
}

func newQueryResultError(err error) *QueryResult {
	queryedRow := &QueryResult{}
	queryedRow.err = err
	return queryedRow
}

func newQueryResult(stmt *sql.Stmt, rows *sql.Rows) *QueryResult {
	queryedRow := &QueryResult{}
	queryedRow.pstmt = stmt
	queryedRow.rows = rows
	return queryedRow
}

func (r *QueryResult) Next() bool {
	return r.rows.Next()
}

func (r *QueryResult) GetError() (err error) {
	return r.err
}

func (r *QueryResult) Scan(v ...interface{}) (err error) {
	if r.err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fail to scan : %s", r)
		}
	}()

	atype := reflect.TypeOf(v[0])

	if atype.Kind() != reflect.Ptr {
		return errQueryNeedsPtrParameter
	}

	if reflect.ValueOf(v[0]).IsNil() {
		return errNilPtr
	}

	atype = atype.Elem()
	val := reflect.ValueOf(v[0]).Elem()

	switch atype.Kind() {
	case reflect.Interface :
		return errInterfaceIsNotSupported
	case reflect.Ptr :
		return errPtrIsNotSupported
	case reflect.Struct :
		return r.scanToStruct(&val)
	}

	return r.rows.Scan(v...)
}

func (r *QueryResult) scanToStruct(val *reflect.Value) error {
	if r.rows.Err() != nil {
		return r.rows.Err()
	}

	columns, err := r.rows.Columns()
	if err != nil {
		return err
	}

	ss := newStructureScanner(r.fieldNameConverter, columns, val)

	return r.rows.Scan(ss.cloneScannerList()...)
}

func (r *QueryResult) Close() error {
	defer func() {
		if r.pstmt != nil {
			r.pstmt.Close()
		}
	}()

	if r.rows != nil {
		return r.rows.Close()
	}

	return nil
}

type QueryRowResult struct {
	err                error
	row               *sql.Row
	fieldNameConverter FieldNameConvertStrategy
}

func newQueryRowResultError(err error) *QueryRowResult {
	queryedRow := &QueryRowResult{}
	queryedRow.err = err
	return queryedRow
}

func newQueryRowResult(row *sql.Row) *QueryRowResult {
	queryedRow := &QueryRowResult{}
	queryedRow.row = row
	return queryedRow
}


type ExecMultiResult struct {
	idList			[]int64
	rowAffected		int64
}

func (p *ExecMultiResult) addInsertId(id int64)  {
	if p.idList == nil {
		p.idList = make([]int64, 0)
	}

	p.idList = append(p.idList, id)
}

func (p ExecMultiResult) GetInsertIdList() []int64  {
	return p.idList
}

func (p ExecMultiResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (p ExecMultiResult) RowsAffected() (int64, error) {
	return p.rowAffected, nil
}
