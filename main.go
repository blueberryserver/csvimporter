package main

import (
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/blueberryserver/bluecore"
	_ "github.com/go-sql-driver/mysql"
	"github.com/julienschmidt/httprouter"
)

// Config ...
type Config struct {
	Host     string   `json:"host"`
	User     string   `json:"user"`
	Pw       string   `json:"pw"`
	Database string   `json:"database"`
	Files    []string `json:"files"`
	Ready    []string `json:"ready"`
}

var gconf = &Config{}
var gpath = "files/"

func main() {
	port := flag.String("p", "8080", "p=8080")
	flag.Parse() // 명령줄 옵션의 내용을 각 자료형별

	err := bluecore.ReadYAML(gconf, "conf/conf.yaml")
	if err != nil {
		log.Println(err)
		return
	}

	//db 연결 dsn id:pw@protocol(ip:port)/database
	//db, err := sql.Open("mysql", "doz2:test1324@tcp(52.79.55.103:3306)/~~~")

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", gconf.User, gconf.Pw, gconf.Host, gconf.Database))
	defer db.Close()
	if err != nil {
		fmt.Println("Mysql connection fail !!!")
		return
	}

	// create log file
	logfile := "log/log_" + time.Now().Format("2006_01_02_15") + ".txt"
	fileLog, err := os.OpenFile(logfile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	defer fileLog.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	mutiWriter := io.MultiWriter(fileLog, os.Stdout)
	log.SetOutput(mutiWriter)

	log.Printf("Start Csv Importer Port:%s !!!\r\n", *port)

	// start routing
	router := httprouter.New()
	router.POST("/import", ImportCSV)
	router.POST("/upload", UploadCSV)

	log.Fatal(http.ListenAndServe(":"+*port, router))

	//importCSV(gconf.Files, db)
}

// ImportCSV ...
/* json sample
{	"ver":"22.0.1",
	"database":"doz3_global_ub22",
	"files": [
		"iu_global_settings.csv",
		"iu_achiv_table.csv" ]}
*/
func ImportCSV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	decoder := json.NewDecoder(r.Body)
	var importInfo struct {
		Ver      string   `json:"ver"`      // 22.0.0
		Database string   `json:"database"` // doz3_global_ub22
		Files    []string `json:"files"`    // iu_achiv_table.csv
	}
	err := decoder.Decode(&importInfo)
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), 400)
		return
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s", gconf.User, gconf.Pw, gconf.Host, importInfo.Database))
	defer db.Close()
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), 400)
		return
	}

	importCSV(importInfo.Files, db, importInfo.Ver)
}

// UploadCSV ...
// upload curl sample
// curl -i -X POST -H "Content-Type: multipart/form-data" -F "uploadfile=@utf8.csv" http://localhost:8080/upload
func UploadCSV(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
	file, header, err := r.FormFile("uploadfile")
	if err != nil {
		log.Println(err)
		http.Error(w, err.Error(), 400)
	}
	defer file.Close()
	fmt.Fprintf(w, "%v", header.Header)

	f, err := os.OpenFile("./files/"+header.Filename, os.O_WRONLY|os.O_CREATE, 0666)
	defer f.Close()
	io.Copy(f, file)
}

// db import csv
func importCSV(files []string, db *sql.DB, ver string) error {

	messageChan := make(chan string)
	var wg sync.WaitGroup
	wg.Add(len(files))

	for _, file := range files {
		filename := file

		// separate file name and extension
		name, _ := getFileNameExtension(file)

		go func(filename string, name string, db *sql.DB) {
			defer wg.Done()

			// check included utf8 string
			result, err := bluecore.CheckUtf8(gpath + filename)
			if result == false {
				var errLog = fmt.Sprintf("file utf8 string fail:%s \r\n", filename)
				log.Printf(errLog)

				// change file string encoding -> utf8
				temp := strings.Split(filename, ".")
				var changedfilename = fmt.Sprintf("%s_utf8.%s", temp[0], temp[1])
				bluecore.EncodingfileUtf8(gpath+filename, gpath+changedfilename)
				filename = changedfilename
			}

			// load csv file format
			//datas, header, err := readCSV(file)
			datas, _, err := readCSV(gpath + filename)
			if err != nil {
				var errLog = fmt.Sprintf("load csv file fail:%s \r\n", filename)
				log.Printf(errLog)
				messageChan <- name + " fail" + err.Error()
				return
			}

			// check table exist
			err = checkDBTable(db, name)
			if err != nil {
				var errLog = fmt.Sprintf("not exist table %s err:%s \r\n", name, err)
				log.Printf(errLog)
				messageChan <- name + " fail" + err.Error()
				return
			}

			// get columns info
			columnInfos, err := getDBTableColumnsInfo(db, name)
			if err != nil {
				var errLog = fmt.Sprintf("columns info fail:%s \r\n", filename)
				log.Printf(errLog)
				messageChan <- name + " fail" + err.Error()
				return
			}

			// manipulate column by string, datetime
			err = manipulateCSVData(&datas, columnInfos)
			if err != nil {
				var errLog = fmt.Sprintf("manipulate csv data fail:%s \r\n", filename)
				log.Printf(errLog)
				messageChan <- name + " fail" + err.Error()
				return
			}

			// generate backup table query
			backupQuery, err := generateBackupTableQuery(name, ver)
			log.Printf("%s\r\n", backupQuery)

			// generate create table query
			createQuery, err := generateCreateTableQuery(db, name, ver)
			log.Printf("%s\r\n", createQuery)

			// generate insert query
			insertQuery, err := generateInsertQuery(datas, name, ver)
			log.Printf("%s\r\n", insertQuery)

			// execution
			tr, _ := db.Begin()

			_, err = db.Exec(backupQuery)
			if err != nil {
				var errLog = fmt.Sprintf("%s fail err:%s \r\n", backupQuery, err)
				_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s_%s`;", name, time.Now().Format("2006_01_02_1504")))
				_, _ = db.Exec(backupQuery)
				log.Printf(errLog)
			}

			_, err = db.Exec(createQuery)
			if err != nil {
				tr.Rollback()
				var errLog = fmt.Sprintf("%s fail err:%s \r\n", createQuery, err)
				log.Printf(errLog)
				messageChan <- name + " fail" + err.Error()
				return
			}

			for _, query := range insertQuery {
				_, err = db.Exec(query)
				if err != nil {
					tr.Rollback()
					var errLog = fmt.Sprintf("insert fail err:%s \r\n", err)
					log.Printf(errLog)
					messageChan <- name + " fail" + err.Error()
					return
				}
			}
			tr.Commit()

			messageChan <- name + " success"
		}(filename, name, db)
	}

	go func() {
		for msg := range messageChan {
			log.Printf("%s\r\n", msg)
		}
	}()

	wg.Wait()
	return nil
}

// 파일 이름 확장자 구분 함수
func getFileNameExtension(filename string) (string, string) {
	const (
		FileName      = 0
		FileExtention = 1
	)

	temp := strings.Split(filename, ".")
	return temp[FileName], temp[FileExtention]
}

// 날짜 시간 문자열의 타임 포멧 함수
func getTimeFormatStr(sampleTime string, loc *time.Location) (string, error) {
	// 4가지 포멧 준비( 추가 가능 )
	format := [...]string{"2006-01-02 15:04:05", "2006-01-02 15:04", "2006/01/02 15:04:05", "2006-01-02 PM 03:04:05"}
	for _, tFormat := range format {
		_, tErr := time.ParseInLocation(tFormat, sampleTime, loc)

		if tErr == nil {
			return tFormat, nil
		}
	}
	return "", errors.New("not found time format")
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

// ColumnInfo ...
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

	columCount := 0
	for i := 0; res2.Next(); i++ {

		res2.Scan(&column[i].Field, &column[i].Type, &column[i].Collation, &column[i].Null,
			&column[i].Key, &column[i].Default, &column[i].Extra, &column[i].Privileges, &column[i].Comment)

		//		fmt.Printf("%s, %s, %s, %s, %s, %s, %s, %s, %s\r\n", column[i].Field, column[i].Type,
		//			column[i].Collation.String, column[i].Null, column[i].Key, column[i].Default.String, column[i].Extra,
		//			column[i].Privileges, column[i].Comment)
		columCount++
	}

	return column[:columCount], nil
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

		if strings.Contains(columnInfo[i].Type, "varchar") || strings.Contains(columnInfo[i].Type, "char") {
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
			columnIndex := datetime[i]
			datetimeValue := (*datas)[row][columnIndex]

			t, err := time.ParseInLocation(curformat, datetimeValue, loc)
			if err != nil {
				fmt.Printf("time format string err:%s\r\n", err)
				return err
			}
			(*datas)[row][columnIndex] = "'" + t.Format("2006-01-02 15:04:05") + "'"

			//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, datetime_value, (*datas)[row][column_index])
		}

		for i := 0; i < nullableindex; i++ {
			columnIndex := nullable[i]
			value := (*datas)[row][columnIndex]

			if value == "" || value == "''" {
				(*datas)[row][columnIndex] = "NULL"
				//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, value, (*datas)[row][column_index])
			}
		}

		for i := 0; i < varcharindex; i++ {
			columnIndex := varchar[i]
			value := (*datas)[row][columnIndex]
			if value != "NULL" {
				if strings.Contains(value, "'") {
					value = strings.Replace(value, "'", "\"", -1)
				}

				(*datas)[row][columnIndex] = "'" + value + "'"
				//fmt.Printf("%s %s (%s)\r\n", columnInfo[column_index].Field, value, (*datas)[row][column_index])
			}
		}
	}

	return nil
}

// generate backup table query
func generateBackupTableQuery(tablename string, ver string) (string, error) {
	var query string
	query = fmt.Sprintf("RENAME TABLE `%s` TO `%s_%s`;", tablename, tablename, time.Now().Format("2006_01_02_1504"))
	return query, nil
}

// generate create table query
func generateCreateTableQuery(db *sql.DB, tablename string, ver string) (string, error) {
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
func generateInsertQuery(datas [][]string, tablename string, ver string) ([]string, error) {
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
