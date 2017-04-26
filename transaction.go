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
// @date 2017. 4. 21. PM 2:47
//

package queryman

import (
	"database/sql"
)

type DBTransaction struct {
	tx                 *sql.Tx
	queryFinder        QueryStatementFinder
	fieldNameConverter FieldNameConvertStrategy
}

func (t *DBTransaction) Rollback() error {
	return t.tx.Rollback()
}

func (t *DBTransaction) Commit() error {
	return t.tx.Commit()
}


func newTransaction(tx *sql.Tx, queryFinder QueryStatementFinder, fieldNameConverter FieldNameConvertStrategy) *DBTransaction {
	dbTransaction := DBTransaction{}
	dbTransaction.tx = tx
	dbTransaction.queryFinder = queryFinder
	dbTransaction.fieldNameConverter = fieldNameConverter
	return &dbTransaction
}

func (t *DBTransaction) exec(query string, args ...interface{}) (sql.Result, error) {
	return t.tx.Exec(query, args...)
}

func (t *DBTransaction) query(query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.Query(query, args...)
}

func (t *DBTransaction) queryRow(query string, args ...interface{}) *sql.Row {
	return t.tx.QueryRow(query, args...)
}

func (t *DBTransaction) prepare(query string) (*sql.Stmt, error) {
	return t.prepare(query)
}

func (t *DBTransaction) Execute(id string, v ...interface{}) (sql.Result, error) {
	stmt, err := t.queryFinder.find(id)
	if err != nil {
		return nil, err
	}

	if stmt.sqlTyp != sqlTypeInsert && stmt.sqlTyp != sqlTypeUpdate {
		return nil, errExecutionInvalidSqlType
	}

	return execute(t, stmt, v...)
}


func (t *DBTransaction) Query(id string, v ...interface{}) *QueryResult {
	stmt, err := t.queryFinder.find(id)
	if err != nil {
		return newQueryResultError(err)
	}

	if stmt.sqlTyp != sqlTypeSelect {
		return newQueryResultError(errQueryInvalidSqlType)
	}

	queryedRow := queryMultiRow(t, stmt, v...)
	queryedRow.fieldNameConverter = t.fieldNameConverter
	return queryedRow
}