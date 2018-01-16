package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/text/encoding/korean"
)

func main() {
	config := readConfig()
	if config == nil {
		fmt.Println("load config fail !!!")
		return
	}
	//db 연결 dsn id:pw@protocol(ip:port)/database
	//db, err := sql.Open("mysql", "doz2:test1324@tcp(52.79.55.103:3306)/doz2")

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/%s", config.User, config.Pw, config.Host, config.Database))
	defer db.Close()
	if err != nil {
		fmt.Println("Mysql connection fail !!!")
		return
	}

	log, err := os.Create(fmt.Sprintf("csv_import_log_%s.txt", time.Now().Format("2006_01_02_15_04_05")))
	if err != nil {
		return
	}

	fmt.Println("Start Blueberry Csv Importer !!!")

	for i, file := range config.Files {
		filename := file
		// separate file name and extension
		name, extension := getFileNameExtension(file)
		//_, _ = getFileNameExtension(file)

		// check included utf8 string
		if checkUtf8(file) == false {
			var errLog = fmt.Sprintf("file utf8 string fail:%s \r\n", file)
			log.WriteString("\r\n")
			log.WriteString(errLog)

			// change file string encoding -> utf8
			temp := strings.Split(filename, ".")
			var changedfilename = fmt.Sprintf("%s_utf8.%s", temp[0], temp[1])
			encodingfileUtf8(filename, changedfilename)
			filename = changedfilename
		}

		// load csv file format
		//datas, header, err := readCSV(file)
		datas, _, err := readCSV(filename)
		if err != nil {
			var errLog = fmt.Sprintf("load csv file fail:%s \r\n", file)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		// check table exist
		err = checkDBTable(db, name)
		if err != nil {
			var errLog = fmt.Sprintf("not exist table %s err:%s \r\n", name, err)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		// get columns info
		columnInfos, err := getDBTableColumnsInfo(db, name)
		if err != nil {
			var errLog = fmt.Sprintf("columns info fail:%s \r\n", file)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		//		for i := 0; i < len(columnInfos); i++ {
		//			column := columnInfos[i]
		//			fmt.Printf("(%d)%s,%s,%s,%s,%s,%s,%s,%s,%s\r\n", i, column.Field, column.Type,
		//				column.Collation.String, column.Null, column.Key, column.Default.String, column.Extra,
		//				column.Privileges, column.Comment)
		//		}

		// manipulate column by string, datetime
		err = manipulateCSVData(&datas, columnInfos)
		if err != nil {
			var errLog = fmt.Sprintf("manipulate csv data fail:%s \r\n", file)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}

		// generate backup table query
		backupQuery, err := generateBackupTableQuery(name)
		//fmt.Printf("backup query: %s\r\n", backupQuery)

		// generate create table query
		createQuery, err := generateCreateTableQuery(db, name)
		//fmt.Printf("create query: %s\r\n", createQuery)

		// generate insert query
		insertQuery, err := generateInsertQuery(datas, name)
		//fmt.Printf("insert query: %s\r\n", insertQuery)

		tr, _ := db.Begin()

		_, err = db.Exec(backupQuery)
		if err != nil {
			var errLog = fmt.Sprintf("%s fail err:%s \r\n", backupQuery, err)
			_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s_%s`;", name, time.Now().Format("2006_01_02")))
			_, _ = db.Exec(backupQuery)
			log.WriteString("\r\n")
			log.WriteString(errLog)
		}
		log.WriteString("\r\n")
		log.WriteString(backupQuery)

		_, err = db.Exec(createQuery)
		if err != nil {
			tr.Rollback()
			var errLog = fmt.Sprintf("%s fail err:%s \r\n", createQuery, err)
			log.WriteString("\r\n")
			log.WriteString(errLog)
			continue
		}
		log.WriteString("\r\n")
		log.WriteString(createQuery)

		for _, query := range insertQuery {
			_, err = db.Exec(query)
			if err != nil {
				tr.Rollback()
				var errLog = fmt.Sprintf("insert fail err:%s \r\n", err)
				log.WriteString("\r\n")
				log.WriteString(errLog)
				continue
			}

			log.WriteString("\r\n")
			log.WriteString(query)
		}

		tr.Commit()

		// execute query
		var comment = fmt.Sprintf("import file name: %s.%s complete count: %d\r\n", name, extension, i+1)
		fmt.Println(comment)

		log.WriteString("\r\n")
		log.WriteString(comment)
		log.WriteString("\r\n")

	}
	log.WriteString("\r\n")
	log.Close()

	/*
		// 파일 이름 확장자 구분
		filename := "cw_product_refresh.csv"
		file_name, file_extension := getFileNameExtension(filename)
		fmt.Printf("%s.%s\r\n", file_name, file_extension)

		//check utf8
		if false == checkUtf8(filename) {
			fmt.Println("file utf8 string fail")
			return
		}

		// CSV 파일 리딩
		lines, _, err := readCSV(filename)
		if err != nil {
			fmt.Printf("read csv file fail err:%s \r\n", err)
			return
		}

		// 테이블 존재여부 체크
		res, err := db.Query(fmt.Sprintf("SHOW TABLES LIKE '%s'", file_name))
		if err != nil {
			fmt.Printf("show table query fail %s", err)
			return
		}
		if res.Next() == false {
			fmt.Println("show table query fail")
		}

		// 테이블 컬럽 정보 조회
		res2, err := db.Query(fmt.Sprintf("SHOW FULL COLUMNS FROM %s", file_name))
		if err != nil {
			fmt.Println("show columns query fail !!!")
			return
		}

		// 컬럼 정보 스캔
		var Field, Type, Collation, Null, Key, Default, Extra, Privileges, Comment [100]string
		colum_count := 0
		for i := 0; res2.Next(); i++ {

			res2.Scan(&Field[i], &Type[i], &Collation[i], &Null[i], &Key[i], &Default[i], &Extra[i], &Privileges[i], &Comment[i])
			colum_count++
		}

		var varchar [100]int
		varcharindex := 0

		var datetime [100]int
		datetimeindex := 0
		for i := 0; i < colum_count; i++ {
			//fmt.Printf("%s,%s,%s,%s,%s,%s,%s,%s,%s\r\n", Field[i], Type[i], Collation[i], Null[i], Key[i], Default[i], Extra[i], Privileges[i], Comment[i])

			// set datetime column nomber
			if Type[i] == "datetime" {
				datetime[datetimeindex] = i
				datetimeindex++
			}

			if strings.Contains(Type[i], "varchar") {
				varchar[varcharindex] = i
				varcharindex++
			}
		}

		//get location
		loc, _ := time.LoadLocation("Asia/Seoul")

		//get time format
		curformat, _ := getTimeFormatStr(lines[1][datetime[0]], loc)

		// check invalid timedate string
		for row := 1; row < len(lines); row++ {
			if lines[row][0] == "" {
				break
			}

			for i := 0; i < datetimeindex; i++ {
				column_index := datetime[i]
				datetime_value := lines[row][column_index]

				t, err := time.ParseInLocation(curformat, datetime_value, loc)
				if err != nil {
					fmt.Printf("time format string err:%s\r\n", err)
					return
				}
				lines[row][column_index] = "'" + t.Format("2006-01-02 15:04:05") + "'"
				//fmt.Printf("%s %s (%s)\r\n", Field[column_index], datetime_value, lines[row][column_index])
			}

			for i := 0; i < varcharindex; i++ {
				column_index := varchar[i]
				value := lines[row][column_index]

				lines[row][column_index] = "'" + value + "'"
				//fmt.Printf("%s %s (%s)\r\n", Field[column_index], value, lines[row][column_index])
			}
		}

		// 테이블 생성및 삭제 쿼리 생성
		var tableDropQuery string
		tableDropQuery = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", file_name)
		fmt.Println(tableDropQuery)

		// 테이블 백업 쿼리 생성
		var tableBackupQuery string
		tableBackupQuery = fmt.Sprintf("RENAME TABLE `%s` TO `%s_%s`;", file_name, file_name, time.Now().Format("2006_01_02"))
		fmt.Println(tableBackupQuery)

		// 테이블 생성 쿼리 추출
		res3, err := db.Query(fmt.Sprintf("SHOW CREATE TABLE %s", file_name))
		if err != nil {
			fmt.Printf("show create table query fail %s", err)
			return
		}
		if res3.Next() == false {
			fmt.Println("show create table query next fail")
			return
		}

		var tableName, createTableQuery string
		res3.Scan(&tableName, &createTableQuery)
		fmt.Println(createTableQuery)

		// 입력 쿼리 생성
		var insertQuery string
		insertQuery = fmt.Sprintf("INSERT INTO `%s` VALUES ", file_name)
		for i := 1; i < len(lines); i++ {
			if lines[i][0] == "" {
				continue
			}

			insertQuery = insertQuery + "("

			for j := 0; j < len(lines[i]); j++ {

				insertQuery = insertQuery + lines[i][j]

				if j+1 < len(lines[i]) {
					insertQuery = insertQuery + ","
				}
			}

			insertQuery = insertQuery + ")"

			if i+1 >= len(lines) {
				insertQuery = insertQuery + ";\r\n"
				break
			}
			insertQuery = insertQuery + ",\r\n"
		}
		//fmt.Println(insertQuery)
		/**/
	/*
		// 트랜젝션 시작
		tr, _ := db.Begin()
		// 백업 쿼리 실행
		_, err = db.Exec(tableBackupQuery)
		if err != nil {
			//tr.Rollback()
			fmt.Printf("%s fail err:%s \r\n", tableBackupQuery, err)
			_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s_%s`;", file_name, time.Now().Format("2006_01_02")))
			_, _ = db.Exec(tableBackupQuery)
		}
		// 테이블 삭제및 생성 쿼리 실행
		//	_, err = db.Exec(tableDropQuery)
		//	if err != nil {
		//		tr.Rollback()
		//		fmt.Printf("%s fail err:%s \r\n", tableDropQuery, err)
		//		return
		//	}
		_, err = db.Exec(createTableQuery)
		if err != nil {
			tr.Rollback()
			fmt.Printf("%s fail err:%s \r\n", createTableQuery, err)
			return
		}
		var disableKeyQuery string
		disableKeyQuery = fmt.Sprintf("ALTER TABLE `%s` DISABLE KEYS", file_name)

		_, err = db.Exec(disableKeyQuery)

		// 데이터 입력 쿼리 실행
		_, err = db.Exec(insertQuery)
		if err != nil {
			tr.Rollback()
			fmt.Printf("insert fail err:%s \r\n", err)
			return
		}

		var enableKeyQuery string
		enableKeyQuery = fmt.Sprintf("ALTER TABLE `%s` ENABLE KEYS", file_name)

		_, err = db.Exec(enableKeyQuery)
		// 트랜젝션 완료
		tr.Commit()
		/**/
}

type Config struct {
	Host     string   `json:"host"`
	User     string   `json:"user"`
	Pw       string   `json:"pw"`
	Database string   `json:"database"`
	Files    []string `json:"files"`
	Ready    []string `json:"ready"`
}

func readConfig() *Config {
	file, err := os.Open("conf.json")
	if err != nil {
		return nil
	}

	var config Config

	jsonParser := json.NewDecoder(file)
	if err := jsonParser.Decode(&config); err != nil {
		return nil
	}

	//fmt.Println(config)
	return &config
}

// 파일 이름 확장자 구분 함수
func getFileNameExtension(filename string) (string, string) {
	const (
		F_NAME      = 0
		F_EXTENSION = 1
	)

	temp := strings.Split(filename, ".")
	return temp[F_NAME], temp[F_EXTENSION]
}

// 날짜 시간 문자열의 타임 포멧 함수
func getTimeFormatStr(sampleTime string, loc *time.Location) (string, error) {
	// 4가지 포멧 준비( 추가 가능 )
	format := [...]string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006/01/02 15:04:05", "2006-01-02 PM 03:04:05"}
	for _, t_format := range format {
		_, t_err := time.ParseInLocation(t_format, sampleTime, loc)

		if t_err == nil {
			return t_format, nil
		}
	}
	return "", errors.New("not found time format")
}

// UTF8 파일 스트링 확인
func checkUtf8(filename string) bool {
	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		fmt.Println("file open errer")
		return false
	}

	//var EOF = errors.New("EOF")
	reader := bufio.NewReader(f)
	for {
		r, err := reader.ReadString('\n')
		if false == utf8.ValidString(r) {
			fmt.Printf("invalid utf8 file %s \r\n", r)
			return false
		}

		if err == io.EOF {
			break
		}
	}
	return true
}

//
func encodingfileUtf8(filename string, resultfilename string) {
	d, err := os.Create(resultfilename)
	defer d.Close()
	if err != nil {
		return
	}

	f, err := os.Open(filename)
	defer f.Close()

	if err != nil {
		fmt.Println("file open errer")
		return
	}

	r := korean.EUCKR.NewDecoder().Reader(f)
	io.Copy(d, r)
}

// CSV 파일 읽기
func readCSV(filename string) ([][]string, []string, error) {

	f, err := os.Open(filename)
	defer f.Close()
	if err != nil {
		fmt.Println("file open errer")
		return nil, nil, err
	}
	// CSV 파일 리딩
	lines, err := csv.NewReader(f).ReadAll()

	if err != nil {
		fmt.Println("csv read errer")
		return nil, nil, err
	}
	return lines, lines[0], nil
}

// DB 테이블 확인
func checkDBTable(db *sql.DB, tablename string) error {
	res, err := db.Query(fmt.Sprintf("SHOW TABLES LIKE '%s'", tablename))
	if err != nil {
		fmt.Printf("show table query fail %s \r\n", err)
		return err
	}
	if res.Next() == false {
		fmt.Println("show table query fail")
		return errors.New("Not find table name")
	}
	return nil
}

type ColumnInfo struct {
	Field      string
	Type       string
	Collation  sql.NullString
	Null       string
	Key        string
	Default    sql.NullString
	Extra      string
	Privileges string
	Comment    string
}

func getDBTableColumnsInfo(db *sql.DB, tablename string) ([]ColumnInfo, error) {
	column := make([]ColumnInfo, 100)

	res2, err := db.Query(fmt.Sprintf("SHOW FULL COLUMNS FROM %s", tablename))
	if err != nil {
		fmt.Println("show columns query fail !!!")
		return nil, err
	}

	colum_count := 0
	for i := 0; res2.Next(); i++ {

		res2.Scan(&column[i].Field, &column[i].Type, &column[i].Collation, &column[i].Null,
			&column[i].Key, &column[i].Default, &column[i].Extra, &column[i].Privileges, &column[i].Comment)

		//		fmt.Printf("%s, %s, %s, %s, %s, %s, %s, %s, %s\r\n", column[i].Field, column[i].Type,
		//			column[i].Collation.String, column[i].Null, column[i].Key, column[i].Default.String, column[i].Extra,
		//			column[i].Privileges, column[i].Comment)
		colum_count++
	}

	return column[:colum_count], nil
}

func manipulateCSVData(datas *[][]string, columnInfo []ColumnInfo) error {

	var varchar [100]int
	varcharindex := 0

	var datetime [100]int
	datetimeindex := 0

	var nullable [100]int
	nullableindex := 0
	for i := 0; i < len(columnInfo); i++ {

		// set datetime column nomber
		if columnInfo[i].Type == "datetime" {
			datetime[datetimeindex] = i
			datetimeindex++
		}

		if strings.Contains(columnInfo[i].Type, "varchar") {
			varchar[varcharindex] = i
			varcharindex++
		}

		if columnInfo[i].Null == "YES" {
			nullable[nullableindex] = i
			nullableindex++
		}
	}

	//get location
	loc, err := time.LoadLocation(time.Now().Location().String())
	if err != nil {
		fmt.Printf("load location err:%s\r\n", err)
		return err
	}

	//get time format
	curformat, _ := getTimeFormatStr((*datas)[1][datetime[0]], loc)

	// check invalid timedate string
	for row := 1; row < len(*datas); row++ {
		if (*datas)[row][0] == "" {
			break
		}

		for i := 0; i < datetimeindex; i++ {
			column_index := datetime[i]
			datetime_value := (*datas)[row][column_index]

			t, err := time.ParseInLocation(curformat, datetime_value, loc)
			if err != nil {
				fmt.Printf("time format string err:%s\r\n", err)
				return err
			}
			(*datas)[row][column_index] = "'" + t.Format("2006-01-02 15:04:05") + "'"

			//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, datetime_value, (*datas)[row][column_index])
		}

		for i := 0; i < nullableindex; i++ {
			column_index := nullable[i]
			value := (*datas)[row][column_index]

			if value == "" || value == "''" {
				(*datas)[row][column_index] = "NULL"
				//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, value, (*datas)[row][column_index])
			}
		}

		for i := 0; i < varcharindex; i++ {
			column_index := varchar[i]
			value := (*datas)[row][column_index]
			if value != "NULL" {
				(*datas)[row][column_index] = "'" + value + "'"
				//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, value, (*datas)[row][column_index])
			}
		}
	}

	return nil
}

// generate backup table query
func generateBackupTableQuery(tablename string) (string, error) {
	var query string
	query = fmt.Sprintf("RENAME TABLE `%s` TO `%s_%s`;", tablename, tablename, time.Now().Format("2006_01_02"))
	return query, nil
}

// generate create table query
func generateCreateTableQuery(db *sql.DB, tablename string) (string, error) {
	var query string
	res, err := db.Query(fmt.Sprintf("SHOW CREATE TABLE %s", tablename))
	if err != nil {
		return query, err
	}
	if res.Next() == false {
		return query, errors.New("Not find create table query")
	}

	var tableName string
	res.Scan(&tableName, &query)
	return query, nil
}

// generate insert query
func generateInsertQuery(datas [][]string, tablename string) ([]string, error) {
	var query []string

	// 입력 쿼리 생성(100 line 기준 새쿼리 생성)
	startIndex := 1
	endIndex := len(datas)
	for {
		if startIndex == len(datas) {
			break
		}

		if endIndex-startIndex > 500 {
			endIndex = startIndex + 500
		}

		var insertQuery string
		insertQuery = fmt.Sprintf("INSERT INTO `%s` VALUES ", tablename)

		for i := startIndex; i < endIndex; i++ {
			if datas[i][0] == "" {
				continue
			}

			insertQuery = insertQuery + "("

			for j := 0; j < len(datas[i]); j++ {

				insertQuery = insertQuery + datas[i][j]

				if j+1 < len(datas[i]) {
					insertQuery = insertQuery + ","
				}
			}

			insertQuery = insertQuery + ")"

			if i+1 >= endIndex {
				insertQuery = insertQuery + ";\r\n"
				break
			}
			insertQuery = insertQuery + ",\r\n"
		}
		//fmt.Println(insertQuery)

		query = append(query, insertQuery)

		startIndex = endIndex
		endIndex = len(datas)
	}
	return query, nil
}
