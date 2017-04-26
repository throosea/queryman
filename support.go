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
// @date 2017. 4. 21. AM 9:11
//

package queryman

import (
	"strings"
	"fmt"
	"bytes"
)

const (
	DELIM_START_CH = '{'
	DELIM_START = "{"
	DELIM_STOP = "}"
)


/*
MySQL               PostgreSQL            Oracle
=====               ==========            ======
WHERE col = ?       WHERE col = $1        WHERE col = :col
VALUES(?, ?, ?)     VALUES($1, $2, $3)    VALUES(:val1, :val2, :val3)
*/

func newNormalizer(driverName string) QueryNormalizer {
	normalizer := &UserQueryNormalizer{}

	switch(strings.ToLower(driverName)) {
	case "postgresql" :
		normalizer.strategy = &PostgreSQLPlaceholderStrategy{}
	case "oci8" :
		normalizer.strategy = &OraclePlaceholderStrategy{}
	default :
		normalizer.strategy = &MysqlPlaceholderStrategy{}
	}

	return normalizer
}

type SqlVariablePlaceholderStrategy interface {
	getNextMark() string
}

type MysqlPlaceholderStrategy struct {
}

func (m *MysqlPlaceholderStrategy) getNextMark() string {
	return "?"
}

type PostgreSQLPlaceholderStrategy struct {
	paramIndex		int
}

func (p *PostgreSQLPlaceholderStrategy) getNextMark() string {
	val := fmt.Sprintf("$%d", p.paramIndex)
	p.paramIndex++
	return val
}

type OraclePlaceholderStrategy struct {
	paramIndex		int
}

func (o *OraclePlaceholderStrategy) getNextMark() string {
	val := fmt.Sprintf(":val%d", o.paramIndex)
	o.paramIndex++
	return val
}

type UserQueryNormalizer struct {
	strategy SqlVariablePlaceholderStrategy
}

func (n *UserQueryNormalizer) normalize(stmt *QueryStatement) error {
	stmt.Query = strings.Trim(stmt.Query, " \r\n\t")
	stmt.columnMention = make([]string, 0)
	if len(stmt.Query) < 3 {
		return fmt.Errorf("invalid Query : %s", stmt.Query)
	}

	var buffer bytes.Buffer

	queryLen := len(stmt.Query)
	for i:=0; i<queryLen; i++ {
		ch := stmt.Query[i]
		if ch != DELIM_START_CH {
			buffer.WriteByte(ch)
			continue
		}

		if i >= queryLen - 2 {
			return fmt.Errorf("incompleted variable closer : %s", stmt.Query)
		}
		stopIndex := strings.Index(stmt.Query[i+1:], DELIM_STOP)
		if stopIndex < 1 {
			return fmt.Errorf("incompleted variable closer : %s", stmt.Query)
		}

		v := stmt.Query[i+1:i+1+stopIndex]
		if strings.Index(v, DELIM_START) >= 0 {
			return fmt.Errorf("invalid variable declare format : %s", stmt.Query)
		}
		stmt.columnMention = append(stmt.columnMention, v)
		i = i + stopIndex + 1
		buffer.WriteString(n.strategy.getNextMark())
	}

	stmt.Query = buffer.String()
	return nil
}

