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
)

const (
	sqlTypeInsert = iota
	sqlTypeUpdate
	sqlTypeDelete
	sqlTypeSelect
)

type declareSqlType uint8

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
	errNoRows = errors.New("sql: no rows in result set")
	errNoInsertId = errors.New("sql: no insert id")
)

type SqlProxy interface {
	exec(query string, args ...interface{}) (sql.Result, error)
	query(query string, args ...interface{}) (*sql.Rows, error)
	queryRow(query string, args ...interface{}) *sql.Row
	prepare(query string) (*sql.Stmt, error)
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
	SqlDelete  []QueryStatement    `xml:"delete"`
	SqlSelect  []QueryStatement    `xml:"select"`
}
