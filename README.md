# Golang xml base DB Query Manager #

Package throosea.com/queryman implements a xml base query infrastructure for Go. 

```
<?xml version="1.0" encoding="UTF-8" ?>
<query>
	<update id="DropCityTable">
		drop table if exists city
	</update>
	<update id="CreateCityTable">
		create table city (
			id  bigint NOT NULL AUTO_INCREMENT,
			name varchar(64) default null,
			age  int  default 0,
			is_man  bool default true,
			percentage float default 0.0,
			create_time datetime default CURRENT_TIMESTAMP,
			update_time datetime,
			primary key (id)
		)
	</update>
	<insert id="InsertCity">
		INSERT INTO CITY(NAME,AGE,IS_MAN,PERCENTAGE,CREATE_TIME,UPDATE_TIME) VALUES({Name},{Age},{IsMan},{Percentage},{CreateTime},{UpdateTime})
	</insert>
	<update id="UpdateCity">
		UPDATE CITY SET AGE=? WHERE IS_MAN=?
	</update>
	<update id="UpdateCityWithName">
		UPDATE CITY SET AGE={Age} WHERE NAME={Name}
	</update>
	<select id="SelectCityWithName">
		SELECT * FROM CITY WHERE NAME like {Name}
	</select>
	<select id="CountCity">
		SELECT Count(*) FROM CITY
	</select>
</query>
```

# Install #
```
go get throosea.com/queryman

or

govendor fetch throosea.com/queryman

or

any other vendoring tools...
```


# Example #

```
#!go

package main

import (
	"throosea.com/log"
	"errors"
)


const (
	sqlDropCityTable = "DropCityTable"
	sqlCreateCityTable = "CreateCityTable"
	sqlInsertCity = "InsertCity"
	sqlUpdateCityWithName = "UpdateCityWithName"
	sqlSelectCityWithName = "SelectCityWithName"
	sqlCountCity = "CountCity"
)

var queryManager *QueryMan

func main() {
	// prepare db source uri
	sourceName := "user:pwd@tcp(127.0.0.1:3306)/mydb?timeout=10s"

	// xml file dir
	xmlFileDir := "/my/somewhere"

	// create preference
	querymanPref := NewQuerymanPreference(xmlFileDir, sourceName)
	querymanPref.Fileset = "myquery*.xml"

	// create queryman
	man, err := NewQueryman(querymanPref)
	if err != nil {
		log.Info("fail to open queryman : %s", err.Error())
		return
	}
	queryManager = man

	// do work
	dropAndCreateTable()
	insertBareParams()
	insertSlice()
	insertObject()
	insertMap()
	insertNestedSlice()
	transactionInsert()
	queryOneObject()
	queryBare()
	queryWithMap()
	queryRowStruct()
	queryRowBare()
	queryButNoMoreData()
	// more samples in queryman_test.go file
}

func dropAndCreateTable() error {
	_, err := queryManager.Execute(sqlDropCityTable)
	if err != nil {
		return fmlog.Errorf("fail to execute(%s) : %s\n", sqlDropCityTable, err.Error())
	}

	_, err = queryManager.Execute(sqlCreateCityTable)
	if err != nil {
		return fmlog.Errorf("fail to execute(%s) : %s\n", sqlCreateCityTable, err.Error())
	}

	return nil
}


func insertBareParams() {
	setup()

	result, err := queryManager.Execute(sqlInsertCity, "bare param", 42, true, 40.0, time.Now(), nil)
	if err != nil {
		log.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		log.Error(err.Error())
		return
	}

	if id != 1 {
		log.Error("invalid last insert id : %d", id)
		return
	}
}

func insertSlice() {
	args := make([]interface{}, 0)
	args = append(args, "sample city")
	args = append(args, 42)
	args = append(args, true)
	args = append(args, 40.0)
	args = append(args, time.Now())
	args = append(args, nil)
	result, err := queryManager.Execute(sqlInsertCity, args)
	if err != nil {
		log.Error(err.Error())
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

func insertObject() {
	city := createCity()

	result, err := queryManager.Execute(sqlInsertCity, city)
	if err != nil {
		log.Error(err.Error())
		return
	}
}

func insertMap() {
	args := make(map[string]interface{})
	args["Name"] = "map name"
	args["Age"] = nil
	args["IsMan"] = true
	args["Percentage"] = 19.21
	args["CreateTime"] = time.Now()
	args["UpdateTime"] = time.Now()

	result, err := queryManager.Execute(sqlInsertCity, args)
	if err != nil {
		log.Error(err.Error())
		return
	}
}

func insertNestedSlice() {
	params := make([][]interface{}, 0)

	insertingCount := 5
	for i:=0; i<insertingCount; i++ {
		args := make([]interface{}, 0)
		args = append(args, "sample city")
		args = append(args, 42)
		args = append(args, true)
		args = append(args, 40.0)
		args = append(args, time.Now())
		args = append(args, nil)
		params = append(params, args)
	}

	result, err := queryManager.Execute(sqlInsertCity, params)
	if err != nil {
		log.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		log.Error(err.Error())
		return
	}

	if int(affected) != insertingCount {
		log.Error("invalid affected count : %d", insertingCount)
		return
	}

	if pstmtResult, ok := result.(ExecMultiResult); ok  {
		if len(pstmtResult.GetInsertIdList()) != insertingCount {
			log.Error("inserted id count is not valid. %d", len(pstmtResult.GetInsertIdList()))
		}
	} else {
		log.Error("result type is not ExecMultiResult")
	}
}

func transactionInsert() {
	city := createCity()
	tx, err := queryManager.Begin()
	defer tx.Rollback()
	result, err := tx.Execute(sqlInsertCity, city)
	if err != nil {
		log.Error(err.Error())
		return
	}

	err = tx.Commit()
	if err != nil {
		log.Error(err.Error())
		return
	}

	affected, err := result.RowsAffected()
	if err != nil {
		log.Error(err.Error())
		return
	}

	id, err := result.LastInsertId()
	if err != nil {
		log.Error(err.Error())
		return
	}

	if id != 1 || affected != 1 {
		log.Error("invalid result : id=%d, affected=%d", id, affected)
		return
	}
}


func queryButNoMoreData() {
	result := queryManager.Query(sqlSelectCityWithName, "sample city") // time is null
	if result.GetError() != nil {
		log.Error(result.GetError())
		return
	}

	defer result.Close()

	if !result.Next() {
		return
	}

	log.Error("should be no more data")
}

func queryOneObject() {
	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "bare param", 42, true, 40.0, time.Now(), nil)
	if err != nil {
		log.Error(err.Error())
		return
	}

	city := &City{}
	result := queryManager.Query(sqlSelectCityWithName, "bare param") // time is null
	if result.GetError() != nil {
		log.Error(result.GetError())
		return
	}

	defer result.Close()

	if !result.Next() {
		log.Error(errNoMoreData)
		return
	}

	err = result.Scan(city)
	if err != nil {
		log.Errorf("fail to scan : %s", err.Error())
		return
	}
}



func queryRowBare() {
	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "sample_city", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		log.Error(err.Error())
		return
	}

	count := 0
	err = queryManager.QueryRow(sqlCountCity).Scan(&count)
	if err != nil {
		log.Error(err.Error())
		return
	}

	if count != 1 {
		log.Errorf("invalid city count %d", count)
		return
	}
}

func queryRowStruct() {
	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "unexported_field", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		log.Error(err.Error())
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
		log.Error(err.Error())
		return
	}

	if city.Age.Int64 != 42 {
		log.Errorf("selecting mismatch")
	}
}

func queryBare() {
	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "unexported_field", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		log.Error(err.Error())
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
		log.Errorf(result.GetError().Error())
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
		log.Error(errNoMoreData)
		return
	}
	err = result.Scan(&id, &name, &age, &isMan, &percentage, &createTime, &updateTime)
	if err != nil {
		log.Error(err.Error())
		return
	}
}

func queryWithMap() {
	// insert sample
	_, err := queryManager.Execute(sqlInsertCity, "map_name", 42, true, 40.0, time.Now(), time.Now())
	if err != nil {
		log.Error(err.Error())
		return
	}

	m := make(map[string]string)
	m["Name"] = "map_name"
	result := queryManager.Query(sqlSelectCityWithName, m)
	if result.GetError() != nil {
		log.Errorf(result.GetError().Error())
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
		log.Error(errNoMoreData)
		return
	}
	err = result.Scan(&id, &name, &age, &isMan, &percentage, &createTime, &updateTime)
	if err != nil {
		log.Error(err.Error())
		return
	}
}

```

# Queryman Preference Properties #

You can set logging preference. below is preference properties

name     | type   | default | remark
---------:| :----- | :----- | :-----
Fileset  |  string | "*.xml" | file set
DriverName | string | "mysql" | database driver name
ConnMaxLifetime | time.Duration | 60s | max connection life time while idling
MaxIdleConns | int | 1 | max idle db connections
MaxOpenConns | int | 10 | max open db connections