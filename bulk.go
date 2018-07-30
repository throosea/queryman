//
// Copyright (c) 2018 SK TECHX.
// All right reserved.
//
// This software is the confidential and proprietary information of SK TECHX.
// You shall not disclose such Confidential Information and
// shall use it only in accordance with the terms of the license agreement
// you entered into with SK TECHX.
//
//
// @project queryman
// @author 1100282
// @date 2018. 7. 30. AM 8:55
//

package queryman

import (
	"reflect"
	"fmt"
	"database/sql/driver"
			)

type BulkInsert interface {
	AddInsert(params ...interface{})
	Execute()
}

type querymanBulkInsert struct {
	stmt 		QueryStatement
	sqlProxy 	SqlProxy
	params		[]interface{}
	execCount 	int
}

func newQuerymanBulkInsert(sqlProxy SqlProxy, stmt QueryStatement)	*querymanBulkInsert {
	b := &querymanBulkInsert{}
	b.sqlProxy = sqlProxy
	b.stmt = stmt
	b.params = make([]interface{}, 0)
	stmt.HasCondition()
	return b
}

func (b *querymanBulkInsert) AddInsert(params ...interface{}) (err error)	{
	if len(params) == 0 {
		return nil
	}

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("fail to execute : %s", r)
		}
	}()

	atype := reflect.TypeOf(params[0])
	val := params[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()
		if reflect.ValueOf(val).IsNil() {
			err = ErrNilPtr
			return
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Interface :
		return ErrInterfaceIsNotSupported
	case reflect.Ptr :
		return ErrPtrIsNotSupported
	case reflect.Slice, reflect.Array :
		return b.addList(val)
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return b.addWithObject(val)
		}
	case reflect.Map :
		return b.addMap(val)
	}

	return b.addWithList(params)
}

func (b *querymanBulkInsert) Execute()	{

}


func (b *querymanBulkInsert) addParams(param ...interface{})	{
	for _, p := range param {
		b.params = append(b.params, p)
	}
	b.execCount = b.execCount + 1
}

func (b *querymanBulkInsert) addList(val interface{}) error {
	var passing []interface{}
	if slice, ok := val.([]interface{}); ok  {
		passing = slice
	} else {
		passing = flattenToList(val)
	}

	b.addParams(passing)

	return nil
}

func (b *querymanBulkInsert) addMap(val interface{}) error {
	if m, ok := val.(map[string]interface{}); ok  {
		return b.addWithMap(m)
	}
	passing := flattenToMap(val)
	return b.addWithMap(passing)
}

func (b *querymanBulkInsert) addWithObject(parameter interface{}) error {
	m := flattenStructToMap(parameter)
	return b.addWithMap(m)
}

func (b *querymanBulkInsert) addWithMap(m map[string]interface{}) error {
	passing := make([]interface{}, 0)
	for _,v := range b.stmt.columnMention {
		found, ok := m[v]
		if !ok {
			return fmt.Errorf("addWithMap : not found \"%s\" from parameter values", v)
		}
		passing = append(passing, found)
	}

	b.addParams(passing)

	return nil
}

func (b *querymanBulkInsert) addWithList(args []interface{}) error {
	atype := reflect.TypeOf(args[0])
	val := args[0]

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()

		if reflect.ValueOf(args[0]).IsNil() {
			return ErrNilPtr
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	// check nested list
	switch atype.Kind() {
	case reflect.Slice :
		return b.addWithNestedList(args)
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return b.addWithStructList(args)
		}
	case reflect.Map :
		return b.addWithNestedMap(args)
	}

	if len(b.stmt.columnMention) > len(args) {
		return fmt.Errorf("binding parameter count mismatch. defined=%d, args=%d", len(b.stmt.columnMention), len(args))
	}

	return nil
}

func isStruct(arg interface{}) bool {
	atype := reflect.TypeOf(arg)
	val := arg

	// reform ptr
	if atype.Kind() == reflect.Ptr {
		atype = atype.Elem()

		if reflect.ValueOf(arg).IsNil() {
			return false
		}
		val = reflect.ValueOf(val).Elem().Interface()
	}

	switch atype.Kind() {
	case reflect.Struct :
		if _, is := val.(driver.Valuer); !is {
			return true
		}
	}

	return false
}

func (b *querymanBulkInsert) addWithNestedList(args []interface{}) error {
	// all data in the list should be 'slice' or 'array'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Slice && reflect.TypeOf(v).Kind() != reflect.Array {
			return fmt.Errorf("nested listing structure should have slice type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(b.stmt.columnMention) > reflect.ValueOf(v).Len() {
			return fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(b.stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	for _, v := range args {
		passing := flattenToList(v)
		b.addParams(passing)
	}

	return nil
}

func (b *querymanBulkInsert) addWithStructList(args []interface{}) error {
	for _, v := range args {
		atype := reflect.TypeOf(v)
		val := v

		// reform ptr
		if atype.Kind() == reflect.Ptr {
			atype = atype.Elem()
			if reflect.ValueOf(v).IsNil() {
				return ErrNilPtr
			}
			val = reflect.ValueOf(v).Elem().Interface()
		}

		m := flattenStructToMap(val)
		passing := make([]interface{}, 0)

		for _,v := range b.stmt.columnMention {
			found, ok := m[v]
			if !ok {
				return fmt.Errorf("addWithStructList : not found \"%s\" from parameter values", v)
			}
			passing = append(passing, found)
		}
		b.addParams(passing)
	}

	return nil
}

func (b *querymanBulkInsert) addWithNestedMap(args []interface{}) error {
	// all data in the list should be 'map'
	for i, v := range args {
		if reflect.TypeOf(v).Kind() != reflect.Map {
			return fmt.Errorf("nested listing structure should have map type data only. %d=%s", i, reflect.TypeOf(v).String())
		}
		if len(b.stmt.columnMention) > reflect.ValueOf(v).Len() {
			return fmt.Errorf("binding parameter count mismatch. defined=%d, args[%d]=%d", len(b.stmt.columnMention), i, reflect.ValueOf(v).Len())
		}
	}

	for _, v := range args {
		m, ok := v.(map[string]interface{})
		if !ok {
			return ErrInvalidMapType
		}

		passing := make([]interface{}, 0)
		for _,v2 := range b.stmt.columnMention {
			found, ok := m[v2]
			if !ok {
				return fmt.Errorf("not found \"%s\" from map", v)
			}
			passing = append(passing, found)
		}

		b.addParams(passing)
	}

	return nil
}
