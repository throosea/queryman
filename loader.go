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
// @date 2017. 4. 17. PM 8:04
//

package queryman

import (
	"path/filepath"
	"bytes"
	"fmt"
	"strings"
	"io/ioutil"
	"encoding/xml"
	"runtime"
	"database/sql"
	"time"
)

type QuerymanPreference struct {
	queryFilePath		string
	Fileset				string
	DriverName			string
	dataSourceName		string
	ConnMaxLifetime		time.Duration
	MaxIdleConns		int
	MaxOpenConns		int
	fieldNameConvert	fieldNameConvertMethod
}

func NewQuerymanPreference(filepath string, dataSourceName string) QuerymanPreference {
	pref := QuerymanPreference{}
	pref.queryFilePath = filepath
	pref.Fileset = "*.xml"
	pref.DriverName = "mysql"
	pref.dataSourceName = dataSourceName
	pref.ConnMaxLifetime = time.Duration(time.Second * 60)
	pref.MaxIdleConns = 1
	pref.MaxOpenConns = 10
	pref.fieldNameConvert = fieldNameConvertToCamel

	return pref
}

func NewQueryman(pref QuerymanPreference) (*QueryMan, error) {
	manager := &QueryMan{}
	manager.preference = pref
	manager.statementMap = make(map[string]QueryStatement)

	db, err := sql.Open(pref.DriverName, pref.dataSourceName)
	if err != nil {
		return nil, fmt.Errorf("fail to open sql : %s", err.Error())
	}

	manager.db = db
	manager.db.SetConnMaxLifetime(pref.ConnMaxLifetime)
	manager.db.SetMaxOpenConns(pref.MaxOpenConns)
	manager.db.SetMaxIdleConns(pref.MaxIdleConns)
	manager.fieldNameConverter = newFieldNameConverter(pref.fieldNameConvert)

	err = loadXmlFile(manager, pref.queryFilePath, pref.Fileset)
	if err != nil {
		return nil, fmt.Errorf("fail to load xml file : %s [path=%s,fileset=%s]", err.Error(), pref.queryFilePath, pref.Fileset)
	}

	runtime.SetFinalizer(manager, close)
	return manager, nil
}

func newFieldNameConverter(fieldNameConvert	fieldNameConvertMethod) FieldNameConvertStrategy {
	switch fieldNameConvert {
	case fieldNameConvertToUnderstore :
		return UnderstoreConvertStrategy{}
	}

	return CamelConvertStrategy{}
}

func loadXmlFile(manager *QueryMan, filePath string, fileSet string) error {
	var buffer bytes.Buffer
	buffer.WriteString(filePath)
	buffer.WriteRune(filepath.Separator)
	buffer.WriteString(fileSet)
	matches, err := filepath.Glob(buffer.String())
	if err != nil {
		return fmt.Errorf("fail to search xml file : %s [glob=%s]", err.Error(), buffer.String())
	}

	for _, file := range matches {
		if !strings.HasSuffix(file, "xml") {
			continue
		}

		data, err := ioutil.ReadFile(file)
		if err != nil {
			return fmt.Errorf("fail to read file[%s] : %s", file, err.Error())
		}

		userQuery := UserQuery{}
		err = xml.Unmarshal(data, &userQuery)
		if err != nil {
			fmt.Printf("%s\n", string(data))
			return fmt.Errorf("xml unmarshal fail : %s (file:%s)", err.Error(), file)
		}

		for _, v := range userQuery.SqlInsert {
			v.sqlTyp = sqlTypeInsert
			err = manager.registStatement(v)
			if err != nil {
				return err
			}
		}

		for _, v := range userQuery.SqlUpdate {
			v.sqlTyp = sqlTypeUpdate
			err = manager.registStatement(v)
			if err != nil {
				return err
			}
		}

		for _, v := range userQuery.SqlSelect {
			v.sqlTyp = sqlTypeSelect
			err = manager.registStatement(v)
			if err != nil {
				return err
			}
		}
	}

	return nil
}


func close(manager *QueryMan) {
	manager.Close()
}
