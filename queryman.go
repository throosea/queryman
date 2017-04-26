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
	"strings"
	"fmt"
	"runtime"
)

type QueryNormalizer interface {
	normalize(stmt *QueryStatement) error
}

type QueryMan struct {
	db                 *sql.DB
	preference         QuerymanPreference
	statementMap       map[string]QueryStatement
	normalizer         QueryNormalizer
	fieldNameConverter FieldNameConvertStrategy
}

func (man *QueryMan) GetSqlCount() int {
	return len(man.statementMap)
}

func (man *QueryMan) registStatement(queryStatement QueryStatement) error {
	if man.normalizer == nil {
		man.normalizer = newNormalizer(man.preference.DriverName)
		if man.normalizer == nil {
			return fmt.Errorf("not found normalizer for %s", man.preference.DriverName)
		}
	}

	err := man.normalizer.normalize(&queryStatement)
	if err != nil {
		return err
	}

	id := strings.ToUpper(queryStatement.Id)
	if _, exists := man.statementMap[id]; exists {
		return fmt.Errorf("duplicated user statement Id : %s", id)
	}

	man.statementMap[id] = queryStatement
	return nil
}

func (man *QueryMan) Close() error {
	return man.db.Close()
}

func (man *QueryMan) exec(query string, args ...interface{}) (sql.Result, error) {
	return man.db.Exec(query, args...)
}

func (man *QueryMan) query(query string, args ...interface{}) (*sql.Rows, error) {
	return man.db.Query(query, args...)
}

func (man *QueryMan) queryRow(query string, args ...interface{}) *sql.Row {
	return man.db.QueryRow(query, args...)
}

func (man *QueryMan) prepare(query string) (*sql.Stmt, error) {
	return man.db.Prepare(query)
}

func (man *QueryMan) find(id string)	(QueryStatement, error) {
	stmt, ok := man.statementMap[strings.ToUpper(id)]
	if !ok {
		return stmt, fmt.Errorf("not found Query statement for Id : %s", id)
	}

	return stmt, nil
}

func (man *QueryMan) Execute(id string, v ...interface{}) (sql.Result, error) {
	stmt, err := man.find(id)
	if err != nil {
		return nil, err
	}

	if stmt.sqlTyp != sqlTypeInsert && stmt.sqlTyp != sqlTypeUpdate {
		return nil, errExecutionInvalidSqlType
	}

	return execute(man, stmt, v...)
}


func (man *QueryMan) Query(id string, v ...interface{}) *QueryResult {
	stmt, err := man.find(id)
	if err != nil {
		return newQueryResultError(err)
	}

	if stmt.sqlTyp != sqlTypeSelect {
		return newQueryResultError(errQueryInvalidSqlType)
	}

	queryedRow := queryMultiRow(man, stmt, v...)
	queryedRow.fieldNameConverter = man.fieldNameConverter
	return queryedRow
}

func (man *QueryMan) QueryRow(id string, v ...interface{}) (*sql.Row, error) {
	stmt, err := man.find(id)
	if err != nil {
		return nil, err
	}

	if stmt.sqlTyp != sqlTypeSelect {
		return nil, errQueryInvalidSqlType
	}

	row := man.db.QueryRow(stmt.Query, v...)
	return row, nil
}

func (man *QueryMan) Begin() (*DBTransaction, error) {
	tx, err := man.db.Begin()
	if err != nil {
		return nil, err
	}

	runtime.SetFinalizer(tx, closeTransaction)
	return newTransaction(tx, man, man.fieldNameConverter), nil
}


func closeTransaction(tx *sql.Tx) {
	tx.Commit()
}

