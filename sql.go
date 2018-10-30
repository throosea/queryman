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
	"bytes"
	"time"
	)

func execute(sqlProxy SqlProxy, stmt QueryStatement, v ...interface{}) (result sql.Result, err error) {
	execStmt, err := refineConditional(stmt, v...)
	if err != nil {
		err = fmt.Errorf("fail to buld conditional query : %s", err.Error())
		return
	}

	if len(v) == 0 {
		if sqlProxy.debugEnabled() {
			sqlProxy.debugPrint("%s", stmt.Debug())
		}
		return sqlProxy.exec(execStmt.Query)
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
			err = ErrNilPtr
			return
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return nil, ErrInterfaceIsNotSupported
	case reflect.Ptr :
		return nil, ErrPtrIsNotSupported
	case reflect.Slice, reflect.Array :
		return execList(sqlProxy, val, execStmt)
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return execWithObject(sqlProxy, execStmt, val)
		}
	case reflect.Map :
		return execMap(sqlProxy, val, execStmt)
	}

	return execWithList(sqlProxy, execStmt, v)
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
			return nil, fmt.Errorf("execWithMap : not found \"%s\" from parameter values", v)
		}
		param = append(param, found)
	}

	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(param...))
	}

	return sqlProxy.exec(stmt.Query, param...)
}

func execWithList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	atype := reflect.TypeOf(args[0])
	val := args[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()

		if reflect.ValueOf(args[0]).IsNil() {
			return nil, ErrNilPtr
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	// check nested list
	switch atype.Kind() {
	case reflect.Slice :
		return execWithNestedList(sqlProxy, stmt, args)
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return execWithStructList(sqlProxy, stmt, args)
		}
	case reflect.Map :
		return execWithNestedMap(sqlProxy, stmt, args)
	}

	if len(stmt.columnMention) > len(args) {
		return nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(stmt.columnMention), len(args))
	}

	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(args...))
	}

	start := time.Now()
	defer func() {
		sqlProxy.recordExcution(stmt.Id, start)
	} ()
	return sqlProxy.exec(stmt.Query, args...)
}


func execWithNestedList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithNestedList(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		var nextResult ExecMultiResult
		_, nextResult, err = doExecWithNestedList(sqlProxy, stmt, args[executed:])
		if err == nil {
			result.idList = append(result.idList, nextResult.idList...)
			result.rowAffected += nextResult.rowAffected
		}
	}
	return result, err
}

func doExecWithNestedList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, ExecMultiResult, error) {
	// all data in the list should be 'slice' or 'array'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Slice && reflect.TypeOf(v).Kind() != reflect.Array {
			return 0, ExecMultiResult{}, fmt.Errorf("nested listing structure should have slice type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return 0, ExecMultiResult{}, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, ExecMultiResult{}, err
	}
	defer pstmt.Close()

	sqlProxy.debugPrint("[%s] %s", stmt.Id, stmt.Query)
	result := ExecMultiResult{}
	for i, v := range args {
		passing := flattenToList(v)

		if sqlProxy.debugEnabled() {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf("[%s] params : ", stmt.Id))
			for _, v := range passing {
				buffer.WriteString(fmt.Sprintf("[%v] ", v))
			}
			sqlProxy.debugPrint("%s", buffer.String())
		}

		start := time.Now()
		res, err := pstmt.Exec(passing...)
		if err != nil {
			return i, result, err
		}
		sqlProxy.recordExcution(stmt.Id, start)
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.eleType == eleTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, result, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return len(args), result, nil
}

func execWithNestedMap(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithNestedMap(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		var nextResult ExecMultiResult
		_, nextResult, err = doExecWithNestedMap(sqlProxy, stmt, args[executed:])
		if err == nil {
			result.idList = append(result.idList, nextResult.idList...)
			result.rowAffected += nextResult.rowAffected
		}
	}
	return result, err
}

func doExecWithNestedMap(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, ExecMultiResult, error) {
	// all data in the list should be 'map'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Map {
			return 0, ExecMultiResult{}, fmt.Errorf("nested listing structure should have map type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return 0, ExecMultiResult{}, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, ExecMultiResult{}, err
	}
	defer pstmt.Close()

	sqlProxy.debugPrint("[%s] %s", stmt.Id, stmt.Query)

	result := ExecMultiResult{}
	for i, v := range args {
		m, ok := v.(map[string]interface{})
		if !ok {
			return i, result, ErrInvalidMapType
		}

		param := make([]interface{}, 0)
		for _,v2 := range stmt.columnMention {
			found, ok := m[v2]
			if !ok {
				return i, result, fmt.Errorf("not found \"%s\" from map", v)
			}
			param = append(param, found)
		}

		if sqlProxy.debugEnabled() {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf("[%s] params : ", stmt.Id))
			for _, v := range param {
				buffer.WriteString(fmt.Sprintf("[%v] ", v))
			}
			sqlProxy.debugPrint("%s", buffer.String())
		}

		start := time.Now()
		res, err := pstmt.Exec(param...)
		if err != nil {
			return i, result, err
		}
		sqlProxy.recordExcution(stmt.Id, start)
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.eleType == eleTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, result, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return len(args), result, nil
}


func execWithStructList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	executed, result, err := doExecWithStructList(sqlProxy, stmt, args)
	if err != nil && err == driver.ErrBadConn {
		var nextResult ExecMultiResult
		_, nextResult, err = doExecWithStructList(sqlProxy, stmt, args[executed:])
		if err == nil {
			result.idList = append(result.idList, nextResult.idList...)
			result.rowAffected += nextResult.rowAffected
		}
	}
	return result, err
}

func doExecWithStructList(sqlProxy SqlProxy, stmt QueryStatement, args []interface{}) (int, ExecMultiResult, error) {
	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return 0, ExecMultiResult{}, err
	}
	defer pstmt.Close()

	sqlProxy.debugPrint("[%s] %s", stmt.Id, stmt.Query)
	result := ExecMultiResult{}
	for i, v := range args {
		atype := reflect.TypeOf(v)
		val := v

		// reform ptr
		if atype.Kind() == reflect.Ptr {
			atype = atype.Elem()
			if reflect.ValueOf(v).IsNil() {
				return i, result, ErrNilPtr
			}
			val = reflect.ValueOf(v).Elem().Interface()
		}

		m := flattenStructToMap(val)
		param := make([]interface{}, 0)

		for _,v := range stmt.columnMention {
			found, ok := m[v]
			if !ok {
				return i, result, fmt.Errorf("doExecWithStructList : not found \"%s\" from parameter values", v)
			}
			param = append(param, found)
		}

		if sqlProxy.debugEnabled() {
			var buffer bytes.Buffer
			buffer.WriteString(fmt.Sprintf("[%s] params : ", stmt.Id))
			for _, v := range param {
				buffer.WriteString(fmt.Sprintf("[%v] ", v))
			}
			sqlProxy.debugPrint("%s", buffer.String())
		}

		start := time.Now()
		res, err := pstmt.Exec(param...)
		if err != nil {
			return i, result, err
		}
		sqlProxy.recordExcution(stmt.Id, start)
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.eleType == eleTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return i, result, fmt.Errorf("fail to get last inserted id : %s", err.Error())
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
			panic(ErrInvalidMapKeyType.Error())
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
	execStmt, err := refineConditional(stmt, v...)
	if err != nil {
		return newQueryResultError(fmt.Errorf("fail to buld conditional query : %s", err.Error()))
	}

	if len(v) == 0 {
		rows, err := sqlProxy.query(execStmt.Query)
		if sqlProxy.debugEnabled() {
			sqlProxy.debugPrint("%s", stmt.Debug())
		}
		if err != nil {
			return newQueryResultError(err)
		}
		return newQueryResult(nil, rows)
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
			return newQueryResultError(ErrNilPtr)
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return newQueryResultError(ErrInterfaceIsNotSupported)
	case reflect.Ptr :
		return newQueryResultError(ErrPtrIsNotSupported)
	case reflect.Slice, reflect.Array :
		return queryList(sqlProxy, val, execStmt)
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return queryWithObject(sqlProxy, stmt, val)
		}
	case reflect.Map :
		return queryMap(sqlProxy, val, execStmt)
	}

	return queryWithList(sqlProxy, execStmt, v)
}

func refineConditional(stmt QueryStatement, v ...interface{}) (QueryStatement, error)		{
	if !stmt.HasCondition() {
		return stmt, nil
	}

	if len(v) == 0 {
		return stmt.RefineStatement(nil)
	}

	atype := reflect.TypeOf(v[0])
	val := v[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
		if reflect.ValueOf(val).IsNil() {
			return stmt, ErrNilPtr
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Map :
		if m, ok := val.(map[string]interface{}); ok  {
			return stmt.RefineStatement(m)
		}
		passing := flattenToMap(val)
		return stmt.RefineStatement(passing)
	default :
		return stmt.RefineStatement(nil)
	}
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

	start := time.Now()
	defer func() {
		sqlProxy.recordExcution(stmt.Id, start)
	} ()

	rows, err := sqlProxy.query(stmt.Query, args...)
	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(args...))
	}
	if err != nil {
		return newQueryResultError(err)
	}
	return newQueryResult(nil, rows)

	/*
	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return newQueryResultError(err)
	}

	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(args...))
	}
	start := time.Now()
	defer func() {
		sqlProxy.recordExcution(stmt.Id, start)
	} ()
	rows, err := pstmt.Query(args...)
	if err != nil {
		if !sqlProxy.isTransaction() {
			pstmt.Close()
		}
		return newQueryResultError(err)
	}
	return newQueryResult(pstmt, rows)
	*/
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
			return newQueryResultError(fmt.Errorf("queryWithMap : not found \"%s\" from parameter values", v))
		}
		param = append(param, found)
	}

	start := time.Now()
	defer func() {
		sqlProxy.recordExcution(stmt.Id, start)
	} ()

	rows, err := sqlProxy.query(stmt.Query, param...)
	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(param...))
	}
	if err != nil {
		return newQueryResultError(err)
	}
	return newQueryResult(nil, rows)

	/*
	pstmt, err := sqlProxy.prepare(stmt.Query)
	if err != nil {
		return newQueryResultError(err)
	}
	if sqlProxy.debugEnabled() {
		sqlProxy.debugPrint("%s", stmt.Debug(param...))
	}
	start := time.Now()
	defer func() {
		sqlProxy.recordExcution(stmt.Id, start)
	} ()
	rows, err := pstmt.Query(param...)
	if err != nil {
		pstmt.Close()
		return newQueryResultError(err)
	}
	return newQueryResult(pstmt, rows)
	*/
}

func queryMap(sqlProxy SqlProxy, val interface{}, stmt QueryStatement) *QueryResult {
	if m, ok := val.(map[string]interface{}); ok  {
		return queryWithMap(sqlProxy, stmt, m)
	}
	passing := flattenToMap(val)
	return queryWithMap(sqlProxy, stmt, passing)
}
