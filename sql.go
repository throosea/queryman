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
)

func execute(operator DBOperator, stmt QueryStatement, v ...interface{}) (result sql.Result, err error) {
	if len(v) == 0 {
		return operator.exec(stmt.Query)
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
		return execList(operator, val, stmt)
	case reflect.Struct :
		return execWithObject(operator, stmt, val)
	case reflect.Map :
		return execMap(operator, val, stmt)
	}

	return execWithList(operator, stmt, v)
}

func execList(operator DBOperator, val interface{}, stmt QueryStatement) (sql.Result, error) {
	if slice, ok := val.([]interface{}); ok  {
		return execWithList(operator, stmt, slice)
	}
	passing := flattenToList(val)
	return execWithList(operator, stmt, passing)
}

func execMap(operator DBOperator, val interface{}, stmt QueryStatement) (sql.Result, error) {
	if m, ok := val.(map[string]interface{}); ok  {
		return execWithMap(operator, stmt, m)
	}
	passing := flattenToMap(val)
	return execWithMap(operator, stmt, passing)
}

func execWithObject(operator DBOperator, stmt QueryStatement, parameter interface{}) (sql.Result, error) {
	m := flattenStructToMap(parameter)
	return execWithMap(operator, stmt, m)
}

func execWithMap(operator DBOperator, stmt QueryStatement, m map[string]interface{}) (sql.Result, error) {
	param := make([]interface{}, 0)

	for _,v := range stmt.columnMention {
		found, ok := m[v]
		if !ok {
			return nil, fmt.Errorf("not found \"%s\" from parameter values", v)
		}
		param = append(param, found)
	}

	return operator.exec(stmt.Query, param...)
}


func execWithList(operator DBOperator, stmt QueryStatement, args []interface{}) (sql.Result, error) {
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
		return execWithNestedList(operator, stmt, args)
	case reflect.Struct :
		return execWithStructList(operator, stmt, args)
	case reflect.Map :
		return execWithNestedMap(operator, stmt, args)
	}

	if len(stmt.columnMention) > len(args) {
		return nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(stmt.columnMention), len(args))
	}

	return operator.exec(stmt.Query, args...)
}


func execWithNestedList(operator DBOperator, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	// all data in the list should be 'slice' or 'array'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Slice && reflect.TypeOf(v).Kind() != reflect.Array {
			return nil, fmt.Errorf("nested listing structure should have slice type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := operator.prepare(stmt.Query)
	if err != nil {
		return nil, err
	}
	defer pstmt.Close()

	result := PreparedStatementResult{}
	for _, v := range args {
		passing := flattenToList(v)
		res, err := pstmt.Exec(passing...)
		if err != nil {
			return nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return result, nil
}

func execWithNestedMap(operator DBOperator, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	// all data in the list should be 'map'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Map {
			return nil, fmt.Errorf("nested listing structure should have map type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(stmt.columnMention) > reflect.ValueOf(v).Len() {
			return nil, fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	pstmt, err := operator.prepare(stmt.Query)
	if err != nil {
		return nil, err
	}
	defer pstmt.Close()

	result := PreparedStatementResult{}
	for _, v := range args {
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, errInvalidMapType
		}

		param := make([]interface{}, 0)
		for _,v2 := range stmt.columnMention {
			found, ok := m[v2]
			if !ok {
				return nil, fmt.Errorf("not found \"%s\" from map", v)
			}
			param = append(param, found)
		}

		res, err := pstmt.Exec(param...)
		if err != nil {
			return nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return result, nil
}


func execWithStructList(operator DBOperator, stmt QueryStatement, args []interface{}) (sql.Result, error) {
	pstmt, err := operator.prepare(stmt.Query)
	if err != nil {
		return nil, err
	}
	defer pstmt.Close()

	result := PreparedStatementResult{}
	for _, v := range args {
		atype := reflect.TypeOf(v)
		val := v

		// reform ptr
		if atype.Kind() == reflect.Ptr {
			atype = atype.Elem()
			if reflect.ValueOf(v).IsNil() {
				return nil, errNilPtr
			}
			val = reflect.ValueOf(v).Elem().Interface()
		}

		m := flattenStructToMap(val)
		param := make([]interface{}, 0)

		for _,v := range stmt.columnMention {
			found, ok := m[v]
			if !ok {
				return nil, fmt.Errorf("not found \"%s\" from parameter values", v)
			}
			param = append(param, found)
		}

		res, err := pstmt.Exec(param...)
		if err != nil {
			return nil, err
		}
		affectedCount, _ := res.RowsAffected()
		result.rowAffected += affectedCount

		if stmt.sqlTyp == sqlTypeInsert {
			id, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("fail to get last inserted id : %s", err.Error())
			}
			(&result).addInsertId(id)
		}
	}

	return result, nil
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


func queryRow(operator DBOperator, stmt QueryStatement, v ...interface{}) (queryedRow *QueryedRow) {
	if len(v) == 0 {
		pstmt, err := operator.prepare(stmt.Query)
		if err != nil {
			return newQueryedRowError(err)
		}

		rows, err := pstmt.Query()
		if err != nil {
			return newQueryedRowError(err)
		}
		return newQueryedRow(pstmt, rows)
	}

	defer func() {
		if r := recover(); r != nil {
			queryedRow = newQueryedRowError(fmt.Errorf("fail to queryRow : %s", r))
		}
	}()

	atype := reflect.TypeOf(v[0])
	val := v[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
		if reflect.ValueOf(val).IsNil() {
			return newQueryedRowError(errNilPtr)
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return newQueryedRowError(errInterfaceIsNotSupported)
	case reflect.Ptr :
		return newQueryedRowError(errPtrIsNotSupported)
	case reflect.Slice, reflect.Array :
		return queryList(operator, val, stmt)
	case reflect.Struct :
		return queryWithObject(operator, stmt, val)
	case reflect.Map :
		return queryMap(operator, val, stmt)
	}

	return queryWithList(operator, stmt, v)
}


func queryList(operator DBOperator, val interface{}, stmt QueryStatement) *QueryedRow {
	if slice, ok := val.([]interface{}); ok  {
		return queryWithList(operator, stmt, slice)
	}
	passing := flattenToList(val)
	return queryWithList(operator, stmt, passing)
}

func queryWithList(operator DBOperator, stmt QueryStatement, args []interface{}) *QueryedRow {
	atype := reflect.TypeOf(args[0])

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
	}

	// check nested list
	switch atype.Kind() {
	case reflect.Slice, reflect.Struct, reflect.Map :
		return newQueryedRowError(fmt.Errorf("unacceptable parameter type in list. kind=%s", atype.Kind().String()))
	}

	if len(stmt.columnMention) > len(args) {
		return newQueryedRowError(fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(stmt.columnMention), len(args)))
	}

	pstmt, err := operator.prepare(stmt.Query)
	if err != nil {
		return newQueryedRowError(err)
	}

	rows, err := pstmt.Query(args...)
	if err != nil {
		pstmt.Close()
		return newQueryedRowError(err)
	}
	return newQueryedRow(pstmt, rows)
}


func queryWithObject(operator DBOperator, stmt QueryStatement, parameter interface{}) *QueryedRow {
	m := flattenStructToMap(parameter)
	return queryWithMap(operator, stmt, m)
}

func queryWithMap(operator DBOperator, stmt QueryStatement, m map[string]interface{}) *QueryedRow {
	param := make([]interface{}, 0)

	for _,v := range stmt.columnMention {
		found, ok := m[v]
		if !ok {
			return newQueryedRowError(fmt.Errorf("not found \"%s\" from parameter values", v))
		}
		param = append(param, found)
	}

	pstmt, err := operator.prepare(stmt.Query)
	if err != nil {
		return newQueryedRowError(err)
	}

	rows, err := pstmt.Query(param...)
	if err != nil {
		pstmt.Close()
		return newQueryedRowError(err)
	}
	return newQueryedRow(pstmt, rows)
}

func queryMap(operator DBOperator, val interface{}, stmt QueryStatement) *QueryedRow {
	if m, ok := val.(map[string]interface{}); ok  {
		return queryWithMap(operator, stmt, m)
	}
	passing := flattenToMap(val)
	return queryWithMap(operator, stmt, passing)
}

func (r *QueryedRow) GetError() (err error) {
	return r.err
}

func (r *QueryedRow) Scan(v ...interface{}) (err error) {
	if r.err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fail to scan : %s", r)
		}
	}()

	atype := reflect.TypeOf(v[0])
	//val := v[0]

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

func (r *QueryedRow) scanToStruct(val *reflect.Value) error {
	if r.rows.Err() != nil {
		return r.rows.Err()
	}

	columns, err := r.rows.Columns()
	if err != nil {
		return err
	}

	ss := newStructureScanner(r.fieldNameConverter, columns, val)

	return r.rows.Scan(ss.cloneScannerList()...)
	/*
	anonymous := make([]interface{}, len(columns))
	for i, c := range columns {
		fieldName := r.fieldNameConverter.convertFieldName(c)
		targetField := val.FieldByName(fieldName)
		if !targetField.IsValid() || !targetField.CanInterface() {
			return fmt.Errorf("field %s is not exist or settable", fieldName)
		}

		//fmt.Printf("[%s] : [%s]-[%s]\n", fieldName, targetField.Type().String(), targetField.Kind().String())
		//if fieldName != "UpdateTime" {
		//	anonymous[i] = targetField.Addr().Interface()
		//} else {
		//	b := make([]byte, 0)
		//	anonymous[i] = &b
		//}
		anonymous[i] = targetField.Addr().Interface()
	}

	return r.rows.Scan(anonymous...)
	*/
}

type StructureScanner struct {
	scanIndex		int
	fieldNameList	[]string
	source			*reflect.Value
}

func newStructureScanner(converter FieldNameConvertStrategy, columns []string, val *reflect.Value) *StructureScanner {
	ss := &StructureScanner{}
	ss.scanIndex = 0
	ss.fieldNameList = make([]string, len(columns))
	for i:=0; i<len(columns); i++ {
		ss.fieldNameList[i] = converter.convertFieldName(columns[i])
	}
	ss.source = val
	return ss
}

func (ss *StructureScanner) cloneScannerList() []interface{} {
	scanners := make([]interface{}, len(ss.fieldNameList))
	for i:=0; i<len(ss.fieldNameList); i++ {
		scanners[i] = ss
	}
	return scanners
}

// Scan implements the Scanner interface.
func (ss *StructureScanner) Scan(value interface{}) error {
	fieldName := ss.fieldNameList[ss.scanIndex]
	ss.scanIndex++


	targetField := ss.source.FieldByName(fieldName)
	if !targetField.IsValid() || !targetField.CanInterface() {
		return fmt.Errorf("field %s is not exist or settable", fieldName)
	}

	dest := targetField.Addr().Interface()
	if scanner, ok := dest.(sql.Scanner); ok {
		return scanner.Scan(value)
	}

	switch value.(type) {
	case nil:
		return nil		// do nothing...
	}

	return convertAssign(dest, value)
}

func (r *QueryedRow) Close() error {
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
