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
// @date 2017. 4. 20. PM 6:24
//

package queryman

import (
	"database/sql"
	"fmt"
	"reflect"
	"database/sql/driver"
)

func execute(sqlProxy SqlProxy, stmt QueryStatement, v ...interface{}) (result sql.Result, err error) {
	if len(v) == 0 {
		return sqlProxy.exec(stmt.Query)
	}

	defer func() {
		if r := recover(); r != nil {
			result = nil
			err = fmt.Errorf("fail to execute : %s", r)
		}
	}()

	atype := reflect.TypeOf(v[0])
	val := v[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
		if reflect.ValueOf(val).IsNil() {
			result = nil
			err = errNilPtr
			return
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return nil, errInterfaceIsNotSupported
	case reflect.Ptr :
		return nil, errPtrIsNotSupported
	case reflect.Slice, reflect.Array :
		return execList(sqlProxy, val, stmt)
	case reflect.Struct :
		return execWithObject(sqlProxy, stmt, val)
	case reflect.Map :
		return execMap(sqlProxy, val, stmt)
	}

	return execWithList(sqlProxy, stmt, v)
}

func execList(sqlProxy SqlProxy, val interface{}, stmt QueryStatement) (sql.Result, error) {
	if slice, ok := val.([]interface{}); ok  {
		return execWithList(sqlProxy, stmt, slice)
	}
	passing := flattenToList(val)
	return execWithList(sqlProxy, stmt, passing)
}

func execMap(sqlProxy SqlProxy, val interface{}, stmt QueryStatement) (sql.Result, error) {
	if m, ok := val.(map[string]interface{}); ok  {
		return execWithMap(sqlProxy, stmt, m)
	}
	passing := flattenToMap(val)
	return execWithMap(sqlProxy, stmt, passing)
}

func execWithObject(sqlProxy SqlProxy, stmt QueryStatement, parameter interface{}) (sql.Result, error) {
	m := flattenStructToMap(parameter)
	return execWithMap(sqlProxy, stmt, m)
}

func execWithMap(sqlProxy SqlProxy, stmt QueryStatement, m map[string]interface{}) (sql.Result, error) {
	param := make([]interface{}, 0)

	for _,v := range stmt.columnMention {
		found, ok := m[v]
		if !ok {
			return nil, fmt.Errorf("not found \"%s\" from parameter values", v)
		}
		param = append(param, found)
	}

	return sqlProxy.exec(stmt.Query, param...)
}


func execWithList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	atype := reflect.TypeOf(args[0])

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()

		if reflect.ValueOf(args[0]).IsNil() {
			return nil, errNilPtr
		}
	}

	// check nested list
	switch atype.Kind() {
	case reflect.Slice :
		return execWithNestedList(sqlProxy, stmt, args)
	case reflect.Struct :
		return execWithStructList(sqlProxy, stmt, args)
	case reflect.Map :
		return execWithNestedMap(sqlProxy, stmt, args)
	}

	if len(stmt.columnMention) > len(args) {
		return nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(stmt.columnMention), len(args))
	}

	return sqlProxy.exec(stmt.Query, args...)
}


func execWithNestedList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithNestedList(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		_, result, err = doExecWithNestedList(sqlProxy, stmt, args[executed:])
	}
	return result, err
}

func doExecWithNestedList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, sql.Result, error) {
	// all data in the list should be 'slice' or 'array'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Slice && reflect.TypeOf(v).Kind() != reflect.Array {
			return 0, nil, fmt.Errorf("nested listing structure should have slice type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return 0, nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, nil, err
	}
	defer pstmt.Close()

	result := ExecMultiResult{}
	for i, v := range args {
		passing := flattenToList(v)
		res, err := pstmt.Exec(passing...)
		if err != nil {
			return i, nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return len(args), result, nil
}

func execWithNestedMap(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithNestedMap(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		_, result, err = doExecWithNestedMap(sqlProxy, stmt, args[executed:])
	}
	return result, err
}

func doExecWithNestedMap(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, sql.Result, error) {
	// all data in the list should be 'map'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Map {
			return 0, nil, fmt.Errorf("nested listing structure should have map type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return 0, nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, nil, err
	}
	defer pstmt.Close()

	result := ExecMultiResult{}
	for i, v := range args {
		m, ok := v.(map[string]interface{})
		if !ok {
			return i, nil, errInvalidMapType
		}

		param := make([]interface{}, 0)
		for _,v2 := range stmt.columnMention {
			found, ok := m[v2]
			if !ok {
				return i, nil, fmt.Errorf("not found \"%s\" from map", v)
			}
			param = append(param, found)
		}

		res, err := pstmt.Exec(param...)
		if err != nil {
			return i, nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return len(args), result, nil
}


func execWithStructList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithStructList(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		_, result, err = doExecWithStructList(sqlProxy, stmt, args[executed:])
	}
	return result, err
}

func doExecWithStructList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, sql.Result, error) {
	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, nil, err
	}
	defer pstmt.Close()

	result := ExecMultiResult{}
	for i, v := range args {
		atype := reflect.TypeOf(v)
		val := v

		// reform ptr
		if atype.Kind() == reflect.Ptr {
			atype = atype.Elem()
			if reflect.ValueOf(v).IsNil() {
				return i, nil, errNilPtr
			}
			val = reflect.ValueOf(v).Elem().Interface()
		}

		m := flattenStructToMap(val)
		param := make([]interface{}, 0)

		for _,v := range stmt.columnMention {
			found, ok := m[v]
			if !ok {
				return i, nil, fmt.Errorf("not found \"%s\" from parameter values", v)
			}
			param = append(param, found)
		}

		res, err := pstmt.Exec(param...)
		if err != nil {
			return i, nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return len(args), result, nil
}

func flattenToList(v interface{}) []interface{} {
	s := reflect.ValueOf(v)
	passing := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		passing[i] = s.Index(i).Interface()
	}
	return passing
}

func flattenToMap(v interface{}) map[string]interface{} {
	s := reflect.ValueOf(v)
	passing := make(map[string]interface{})
	for _, k := range s.MapKeys() {
		if k.Kind() != reflect.String {
			panic(errInvalidMapKeyType.Error())
		}
		v := s.MapIndex(k)
		passing[k.String()] = v.Interface()
	}
	return passing
}


func flattenStructToMap(s interface{}) map[string]interface{} {
	m := make(map[string]interface{})

	t := reflect.TypeOf(s)
	v := reflect.ValueOf(s)
	for i:=0; i<t.NumField(); i++ {
		f := t.Field(i)
		fv := v.FieldByName(f.Name)
		if fv.CanInterface() {
			m[f.Name] = fv.Interface()
		}
	}

	return m
}


func queryMultiRow(sqlProxy SqlProxy, stmt QueryStatement, v ...interface{}) (queryedRow *QueryResult) {
	if len(v) == 0 {
		pstmt, err := sqlProxy.prepare(stmt.Query)
		if err != nil {
			return newQueryResultError(err)
		}

		rows, err := pstmt.Query()
		if err != nil {
			return newQueryResultError(err)
		}
		return newQueryResult(pstmt, rows)
	}

	defer func() {
		if r := recover(); r != nil {
			queryedRow = newQueryResultError(fmt.Errorf("fail to queryMultiRow : %s", r))
		}
	}()

	atype := reflect.TypeOf(v[0])
	val := v[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
		if reflect.ValueOf(val).IsNil() {
			return newQueryResultError(errNilPtr)
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return newQueryResultError(errInterfaceIsNotSupported)
	case reflect.Ptr :
		return newQueryResultError(errPtrIsNotSupported)
	case reflect.Slice, reflect.Array :
		return queryList(sqlProxy, val, stmt)
	case reflect.Struct :
		return queryWithObject(sqlProxy, stmt, val)
	case reflect.Map :
		return queryMap(sqlProxy, val, stmt)
	}

	return queryWithList(sqlProxy, stmt, v)
}


func queryList(sqlProxy SqlProxy, val interface{}, stmt QueryStatement) *QueryResult {
	if slice, ok := val.([]interface{}); ok  {
		return queryWithList(sqlProxy, stmt, slice)
	}
	passing := flattenToList(val)
	return queryWithList(sqlProxy, stmt, passing)
}

func queryWithList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) *QueryResult {
	atype := reflect.TypeOf(args[0])

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
	}

	// check nested list
	switch atype.Kind() {
	case reflect.Slice, reflect.Struct, reflect.Map :
		return newQueryResultError(fmt.Errorf("unacceptable parameter type in list. kind=%s", atype.Kind().String()))
	}

	if len(stmt.columnMention) > len(args) {
		return newQueryResultError(fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(stmt.columnMention), len(args)))
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return newQueryResultError(err)
	}

	rows, err := pstmt.Query(args...)
	if err != nil {
		pstmt.Close()
		return newQueryResultError(err)
	}
	return newQueryResult(pstmt, rows)
}


func queryWithObject(sqlProxy SqlProxy, stmt QueryStatement, parameter interface{}) *QueryResult {
	m := flattenStructToMap(parameter)
	return queryWithMap(sqlProxy, stmt, m)
}

func queryWithMap(sqlProxy SqlProxy, stmt QueryStatement, m map[string]interface{}) *QueryResult {
	param := make([]interface{}, 0)

	for _,v := range stmt.columnMention {
		found, ok := m[v]
		if !ok {
			return newQueryResultError(fmt.Errorf("not found \"%s\" from parameter values", v))
		}
		param = append(param, found)
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return newQueryResultError(err)
	}

	rows, err := pstmt.Query(param...)
	if err != nil {
		pstmt.Close()
		return newQueryResultError(err)
	}
	return newQueryResult(pstmt, rows)
}

func queryMap(sqlProxy SqlProxy, val interface{}, stmt QueryStatement) *QueryResult {
	if m, ok := val.(map[string]interface{}); ok  {
		return queryWithMap(sqlProxy, stmt, m)
	}
	passing := flattenToMap(val)
	return queryWithMap(sqlProxy, stmt, passing)
}
