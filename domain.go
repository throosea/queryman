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
// @date 2017. 4. 17. PM 8:03
//

package queryman

import (
	"encoding/xml"
	"database/sql"
	"errors"
	"bytes"
	"unicode"
)

const (
	sqlTypeInsert = iota
	sqlTypeUpdate
	sqlTypeSelect
)

type declareSqlType uint8

const (
	fieldNameConvertToUnderstore = iota
	fieldNameConvertToCamel
)

type fieldNameConvertMethod uint8

var (
	errInterfaceIsNotSupported = errors.New("not supported type : interface")
	errPtrIsNotSupported = errors.New("not supported type : ptr")
	errInvalidMapKeyType = errors.New("map key should be string")
	errInvalidMapType = errors.New("map only accepted [string]interface{} type")
	errExecutionInvalidSqlType = errors.New("invalid execution for sql. only insert or update permitted")
	errQueryInvalidSqlType = errors.New("invalid query for sql. only select permitted")
	errQueryInsufficientParameter = errors.New("insufficient query parameter for select result")
	errQueryNeedsPtrParameter = errors.New("when you select in query, you have to pass parameter as ptr")
	errNilPtr = errors.New("destination pointer is nil")
)

type DBOperator interface {
	exec(query string, args ...interface{}) (sql.Result, error)
	query(query string, args ...interface{}) (*sql.Rows, error)
	prepare(query string) (*sql.Stmt, error)
}

type FieldNameConvertStrategy interface {
	convertFieldName(name string) string
}

type UnderstoreConvertStrategy struct {

}

func (u UnderstoreConvertStrategy) convertFieldName(name string) string {
	// TODO
	return name
}

type CamelConvertStrategy struct {

}

func (u CamelConvertStrategy) convertFieldName(name string) string {
	var buffer bytes.Buffer
	needUpper := true
	for _, c := range name {
		if needUpper {
			buffer.WriteRune(unicode.ToUpper(c))
			needUpper = false
			continue
		}

		if c == '_' {
			needUpper = true
			continue
		}

		buffer.WriteRune(c)
	}

	return buffer.String()
}

// Row is the result of calling QueryRow to select a single row.
type QueryedRow struct {
	pstmt              *sql.Stmt
	err                error
	rows               *sql.Rows
	fieldNameConverter FieldNameConvertStrategy
}

func (r *QueryedRow) Next() bool {
	return r.rows.Next()
}

func newQueryedRowError(err error) *QueryedRow {
	queryedRow := &QueryedRow{}
	queryedRow.err = err
	return queryedRow
}


func newQueryedRow(stmt *sql.Stmt, rows *sql.Rows) *QueryedRow {
	queryedRow := &QueryedRow{}
	queryedRow.pstmt = stmt
	queryedRow.rows = rows
	return queryedRow
}

type QueryStatementFinder interface {
	find(id string)	(QueryStatement, error)
}

type QueryStatement struct {
	sqlTyp			declareSqlType
	Id            	string		`xml:"id,attr"`
	Query         	string		`xml:",cdata"`
	columnMention 	[]string
}

type UserQuery struct {
	XMLName 	xml.Name 			`xml:"query"`
	SqlInsert  []QueryStatement    `xml:"insert"`
	SqlUpdate  []QueryStatement    `xml:"update"`
	SqlSelect  []QueryStatement    `xml:"select"`
}

type PreparedStatementResult struct {
	idList			[]int64
	rowAffected		int64
}

func (p *PreparedStatementResult) addInsertId(id int64)  {
	if p.idList == nil {
		p.idList = make([]int64, 0)
	}

	p.idList = append(p.idList, id)
}

func (p PreparedStatementResult) GetInsertIdList() []int64  {
	return p.idList
}

func (p PreparedStatementResult) LastInsertId() (int64, error) {
	return 0, nil
}

func (p PreparedStatementResult) RowsAffected() (int64, error) {
	return p.rowAffected, nil
}
