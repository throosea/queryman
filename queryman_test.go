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
// @date 2017. 4. 25. PM 7:38
//

package queryman

import (
	"testing"
	"os"
	"fmt"
	"flag"
	"io/ioutil"
	"path/filepath"
	mysql "github.com/go-sql-driver/mysql"
	"time"
	"bytes"
	"errors"
	"database/sql"
)

const (
	xmlFilePrefix = "query."
)

const (
	sqlDropCityTable = "DropCityTable"
	sqlCreateCityTable = "CreateCityTable"
	sqlInsertCity = "InsertCity"
	sqlUpdateCityWithName = "UpdateCityWithName"
	sqlSelectCityWithName = "SelectCityWithName"
	sqlCountCity = "CountCity"
)

const (
	statusDisconnected = iota
	statusNoTable
	statusReady
)

var (
	errNoMoreData = errors.New("no more data")
)

var querymanStatus uint8 = statusDisconnected

var sourceName string
var xmlFile string
var queryManager *QueryMan


type City struct {
	Id		int
	Name	string
	Age		int
	IsMan	bool
	Percentage float32
	CreateTime time.Time
	UpdateTime time.Time
}


// go test -v -db=local -user=local -password=angel -host=10.211.55.7:3306
func TestMain(m *testing.M) {
	prepareSourceName()

	var err error
	xmlFile, err = prepareXmlFile()
	if err != nil {
		fmt.Printf("fail to prepare sample xml file : %s", err.Error())
		return
	}

	code := m.Run()
	os.Remove(xmlFile)
	os.Exit(code)
}

func prepareSourceName() {
	dbName := flag.String("db", "mmate", "database name")
	userName := flag.String("user", "mmate", "Username")
	password := flag.String("password", "angel", "passsword")
	host := flag.String("host", "10.211.55.8:3306", "ip and port")

	flag.Parse()

	sourceName = fmt.Sprintf("%s:%s@tcp(%s)/%s?autocommit=true&timeout=10s&readTimeout=10s&loc=Asia%%2Fseoul&writeTimeout=1s&parseTime=true&charset=utf8mb4,utf8",
		*userName, *password, *host, *dbName)
}

func prepareXmlFile() (string, error) {
	tempDir := os.TempDir()
	clearPreviousXmlFiles(tempDir, "*.xml")

	file, _ := ioutil.TempFile(tempDir, xmlFilePrefix)
	xmlFile := file.Name() + ".xml"
	os.Rename(file.Name(), xmlFile)

	xmlSample := "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n" +
		"<query>\n" +
		"\t<update id=\"DropCityTable\">\n" +
		"\t\tdrop table if exists city\n" +
		"\t</update>\n" +
		"\t<update id=\"CreateCityTable\">\n" +
		"create table city (\n" +
		"\tid  bigint NOT NULL AUTO_INCREMENT,\n" +
		"\tname varchar(64) default null,\n" +
		"\tage  int  default 0,\n" +
		"\tis_man  bool default true,\n" +
		"\tpercentage float default 0.0,\n" +
		"\tcreate_time datetime default CURRENT_TIMESTAMP,\n" +
		"\tupdate_time datetime,\n" +
		"\tprimary key (id)\n" +
		")\n" +
		"\t</update>\n" +
		"\t<insert id=\"InsertCity\">\n" +
		"\t\tINSERT INTO CITY(NAME,AGE,IS_MAN,PERCENTAGE,CREATE_TIME,UPDATE_TIME) VALUES({Name},{Age},{IsMan},{Percentage},{CreateTime},{UpdateTime})\n" +
		"\t</insert>\n" +
		"\t<update id=\"UpdateCityWithName\">\n" +
		"\t\tUPDATE CITY SET AGE={Age} WHERE NAME={Name}\n" +
		"\t</update>\n" +
		"\t<select id=\"SelectCityWithName\">\n" +
		"\t\tSELECT * FROM CITY WHERE NAME like {Name}\n" +
		"\t</select>\n" +
		"\t<select id=\"CountCity\">\n" +
		"\t\tSELECT Count(*) FROM CITY\n" +
		"\t</select>\n" +
		"</query>\n"

	err := ioutil.WriteFile(xmlFile, []byte(xmlSample), 0644)
	if err != nil {
		return xmlFile, err
	}

	return xmlFile, nil
}

func clearPreviousXmlFiles(path string, fileset string) {
	var buffer bytes.Buffer
	buffer.WriteString(path)
	buffer.WriteRune(filepath.Separator)
	buffer.WriteString(fileset)
	matches, err := filepath.Glob(buffer.String())
	if err != nil {
		return
	}

	for _, v := range matches {
		os.Remove(v)
	}
}

func TestConnection(t *testing.T) {
	path := filepath.Dir(xmlFile)
	querymanPref := NewQuerymanPreference(path, sourceName)
	querymanPref.Fileset = xmlFilePrefix + "*.xml"
	man, err := NewQueryman(querymanPref)
	if err != nil {
		t.Errorf("fail to create queryman : %s\n", err.Error())
		return
	}
	querymanStatus = statusNoTable
	queryManager = man
}

func TestDDL(t *testing.T) {
	if querymanStatus == statusDisconnected {
		t.Error("querymanager is not ready")
		return
	}

	err := dropAndCreateTable()
	if err != nil {
		t.Errorf(err.Error())
		return
	}

	querymanStatus = statusReady
}

func dropAndCreateTable() error {
	_, err := queryManager.Execute(sqlDropCityTable)
	if err != nil {
		return fmt.Errorf("fail to execute(%s) : %s\n", sqlDropCityTable, err.Error())
	}

	_, err = queryManager.Execute(sqlCreateCityTable)
	if err != nil {
		return fmt.Errorf("fail to execute(%s) : %s\n", sqlCreateCityTable, err.Error())
	}

	return nil
}

func setup()	{
	if querymanStatus < statusReady {
		panic("querymanager is not ready")
		return
	}

	err := dropAndCreateTable()
	if err != nil {
		panic(fmt.Sprintf("%s", err.Error()))
		return
	}
}

func TestQueryUnknownStatementId(t *testing.T) {
	if querymanStatus < statusReady {
		t.Error("querymanager is not ready")
		return
	}

	_, err := queryManager.Execute("UnknownSomethingStatement")
	if err == nil {
		t.Error("queryManager report statement found")
	}
}


func TestInsertBareParams(t *testing.T) {
	setup()

	result, err := queryManager.Execute(sqlInsertCity, "bare param", 42, true, 40.0, time.Now(), nil)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}


func TestInsertSlice(t *testing.T) {
	setup()

	args := make([]interface{}, 0)
	args = append(args, "slice name")
	args = append(args, 42)
	args = append(args, true)
	args = append(args, 40.0)
	args = append(args, time.Now())
	args = append(args, nil)
	result, err := queryManager.Execute(sqlInsertCity, args)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}

func TestInsertSlicePtr(t *testing.T) {
	setup()

	args := make([]interface{}, 0)
	args = append(args, "slice ptr")
	args = append(args, 42)
	args = append(args, true)
	args = append(args, 40.0)
	args = append(args, time.Now())
	args = append(args, nil)
	result, err := queryManager.Execute(sqlInsertCity, &args)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}

func TestInsertObject(t *testing.T) {
	setup()

	city := createCity()

	result, err := queryManager.Execute(sqlInsertCity, city)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}

func TestInsertObjectPtr(t *testing.T) {
	setup()

	city := createCity()
	city.Name = "ptr test"
	result, err := queryManager.Execute(sqlInsertCity, &city)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}

func TestInsertMap(t *testing.T) {
	setup()

	args := make(map[string]interface{})
	args["Name"] = "map name"
	args["Age"] = nil
	args["IsMan"] = true
	args["Percentage"] = 19.21
	args["CreateTime"] = time.Now()
	args["UpdateTime"] = time.Now()

	result, err := queryManager.Execute(sqlInsertCity, args)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}


func TestInsertNullableSlice(t *testing.T) {
	setup()

	args := make([]interface{}, 0)
	args = append(args, sql.NullString{String:"test_city"})
	args = append(args, sql.NullInt64{})
	args = append(args, sql.NullBool{})
	args = append(args, sql.NullFloat64{})
	args = append(args, time.Now())
	args = append(args, nil)

	result, err := queryManager.Execute(sqlInsertCity, args)
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 {
		t.Errorf("invalid last insert id : %d", id)
		return
	}
}

func TestInsertNestedSlice(t *testing.T) {
	setup()

	params := make([][]interface{}, 0)

	insertingCount := 5

	for i:=0; i<insertingCount; i++ {
		args := make([]interface{}, 0)
		args = append(args, "slice name")
		args = append(args, 42)
		args = append(args, true)
		args = append(args, 40.0)
		args = append(args, time.Now())
		args = append(args, nil)
		params = append(params, args)
	}

	result, err := queryManager.Execute(sqlInsertCity, params)
	if err != nil {
		t.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if int(affected) != insertingCount {
		t.Errorf("invalid affected count : %d", insertingCount)
		return
	}

	if pstmtResult, ok := result.(ExecMultiResult); ok  {
		if len(pstmtResult.GetInsertIdList()) != insertingCount {
			t.Errorf("inserted id count is not valid. %d", len(pstmtResult.GetInsertIdList()))
		}
	} else {
		t.Error("result type is not ExecMultiResult")
	}
}


func TestInsertNestedMap(t *testing.T) {
	setup()

	params := make([]map[string]interface{}, 0)
	insertingCount := 5
	for i:=0; i<insertingCount; i++ {
		args := make(map[string]interface{})
		args["Name"] = "nested map"
		args["Age"] = nil
		args["IsMan"] = true
		args["Percentage"] = 19.21
		args["CreateTime"] = time.Now()
		args["UpdateTime"] = time.Now()
		params = append(params, args)
	}

	result, err := queryManager.Execute(sqlInsertCity, params)
	if err != nil {
		t.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if int(affected) != insertingCount {
		t.Errorf("invalid affected count : %d", insertingCount)
		return
	}

	if pstmtResult, ok := result.(ExecMultiResult); ok  {
		if len(pstmtResult.GetInsertIdList()) != insertingCount {
			t.Errorf("inserted id count is not valid. %d", len(pstmtResult.GetInsertIdList()))
		}
	} else {
		t.Error("result type is not ExecMultiResult")
	}
}

func TestInsertNestedObject(t *testing.T) {
	setup()

	params := make([]interface{}, 0)
	insertingCount := 5
	for i:=0; i<insertingCount; i++ {
		params = append(params, createCity())
	}

	result, err := queryManager.Execute(sqlInsertCity, params)
	if err != nil {
		t.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if int(affected) != insertingCount {
		t.Errorf("invalid affected count : %d", insertingCount)
		return
	}

	if pstmtResult, ok := result.(ExecMultiResult); ok  {
		if len(pstmtResult.GetInsertIdList()) != insertingCount {
			t.Errorf("inserted id count is not valid. %d", len(pstmtResult.GetInsertIdList()))
		}
	} else {
		t.Error("result type is not ExecMultiResult")
	}
}

func TestInsertNestedObjectPtr(t *testing.T) {
	setup()

	params := make([]interface{}, 0)
	insertingCount := 5
	for i:=0; i<5; i++ {
		city := createCity()
		params = append(params, &city)
	}

	result, err := queryManager.Execute(sqlInsertCity, params)
	if err != nil {
		t.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if int(affected) != insertingCount {
		t.Errorf("invalid affected count : %d", insertingCount)
		return
	}

	if pstmtResult, ok := result.(ExecMultiResult); ok  {
		if len(pstmtResult.GetInsertIdList()) != insertingCount {
			t.Errorf("inserted id count is not valid. %d", len(pstmtResult.GetInsertIdList()))
		}
	} else {
		t.Error("result type is not ExecMultiResult")
	}
}


func TestTransactionInsert(t *testing.T) {
	setup()

	city := createCity()
	tx, err := queryManager.Begin()
	defer tx.Rollback()
	result, err := tx.Execute(sqlInsertCity, city)
	if err != nil {
		t.Error(err.Error())
		return
	}

	err = tx.Commit()
	if err != nil {
		t.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Error(err.Error())
		return
	}

	if id != 1 || affected != 1 {
		t.Errorf("invalid result : id=%d, affected=%d", id, affected)
		return
	}
}


func TestQueryButNoMoreData(t *testing.T) {
	setup()

	result := queryManager.Query(sqlSelectCityWithName, "slice name") // time is null
	if result.GetError() != nil {
		t.Error(result.GetError())
		return
	}

	defer result.Close()

	if !result.Next() {
		return
	}

	t.Error("should be no more data")
}

func TestQueryOneObject(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "bare param", 42, true, 40.0, time.Now(), nil)
	if err != nil {
		t.Error(err.Error())
		return
	}

	city := &City{}
	result := queryManager.Query(sqlSelectCityWithName, "bare param") // time is null
	if result.GetError() != nil {
		t.Error(result.GetError())
		return
	}

	defer result.Close()

	if !result.Next() {
		t.Error(errNoMoreData)
		return
	}

	err = result.Scan(city)
	if err != nil {
		t.Errorf("fail to scan : %s", err.Error())
		return
	}
}



func TestQueryRowBare(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "sample_city", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		t.Error(err.Error())
		return
	}

	count := 0
	err = queryManager.QueryRow(sqlCountCity).Scan(&count)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if count != 1 {
		t.Errorf("invalid city count %d", count)
		return
	}
}

func TestQueryRowStruct(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "unexported_field", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		t.Error(err.Error())
		return
	}

	type NullableCity struct {
		Id		sql.NullInt64
		Name	sql.NullString
		Age		sql.NullInt64
		IsMan	sql.NullBool
		Percentage sql.NullFloat64
		CreateTime mysql.NullTime
		UpdateTime mysql.NullTime
	}

	city := NullableCity{}

	err = queryManager.QueryRow(sqlSelectCityWithName, "unexported_field").Scan(&city)
	if err != nil {
		t.Error(err.Error())
		return
	}

	if city.Age.Int64 != 42 {
		t.Errorf("selecting mismatch")
	}
}

func TestQueryBare(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "unexported_field", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		t.Error(err.Error())
		return
	}

	type HasUnexportedFieldCity struct {
		Name	string
		help	string
	}

	city := &City{}
	city.Name = "initial city name"
	sample := HasUnexportedFieldCity{Name:"unexported_field"}
	result := queryManager.Query(sqlSelectCityWithName, sample)
	if result.GetError() != nil {
		t.Errorf(result.GetError().Error())
		return
	}

	defer result.Close()
	var id int
	var name string
	var age int
	var isMan bool
	var percentage float32
	var createTime time.Time
	var updateTime time.Time

	if !result.Next() {
		t.Error(errNoMoreData)
		return
	}
	err = result.Scan(&id, &name, &age, &isMan, &percentage, &createTime, &updateTime)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestQueryWithMap(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "map_name", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		t.Error(err.Error())
		return
	}

	m := make(map[string]string)
	m["Name"] = "map_name"
	result := queryManager.Query(sqlSelectCityWithName, m)
	if result.GetError() != nil {
		t.Errorf(result.GetError().Error())
		return
	}

	defer result.Close()
	var id int
	var name string
	var age int
	var isMan bool
	var percentage float32
	var createTime time.Time
	var updateTime time.Time

	if !result.Next() {
		t.Error(errNoMoreData)
		return
	}
	err = result.Scan(&id, &name, &age, &isMan, &percentage, &createTime, &updateTime)
	if err != nil {
		t.Error(err.Error())
		return
	}
}

func TestQueryNullAndSkipSetting(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "nullable", nil, nil, nil, nil, nil)
	if err != nil {
		t.Error(err.Error())
		return
	}

	type NullableCity struct {
		Id		int
		Name	string
		Age		int
		IsMan	bool
		Percentage float32
		CreateTime time.Time
		UpdateTime time.Time
	}

	city := NullableCity{}
	result := queryManager.Query(sqlSelectCityWithName, "nullable")
	if result.GetError() != nil {
		t.Errorf(result.GetError().Error())
		return
	}

	defer result.Close()

	if !result.Next() {
		t.Error(errNoMoreData)
		return
	}

	err = result.Scan(&city)
	if err != nil {
		t.Error(err.Error())
		return
	}
}


func TestQueryNullScanning(t *testing.T) {
	setup()

	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "nullable", nil, nil, nil, nil, nil)
	if err != nil {
		t.Error(err.Error())
		return
	}

	type NullableCity struct {
		Id		sql.NullInt64
		Name	sql.NullString
		Age		sql.NullInt64
		IsMan	sql.NullBool
		Percentage sql.NullFloat64
		CreateTime mysql.NullTime
		UpdateTime mysql.NullTime
	}

	city := NullableCity{}
	result := queryManager.Query(sqlSelectCityWithName, "%null%")
	if result.GetError() != nil {
		t.Errorf(result.GetError().Error())
		return
	}

	defer result.Close()

	if !result.Next() {
		t.Error(errNoMoreData)
		return
	}

	err = result.Scan(&city)
	if err != nil {
		t.Error(err.Error())
		return
	}
}


func createCity() City {
	city := City{}
	city.Name = "jin.freestyle@gmail.com"
	city.Age = 142
	city.IsMan = true
	city.Percentage = 43.4
	city.CreateTime = time.Now()
	city.UpdateTime = time.Now()
	return city
}
