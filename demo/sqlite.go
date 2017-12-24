package main

import (
	"database/sql"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func mustOK(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func main() {
	db, err := sql.Open("sqlite3", "e:/temp.db")
	mustOK(err)
	_, err = db.Exec(`
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  md5 varchar(40) DEFAULT '',
  name varchar(256) DEFAULT '',
  size int(11) DEFAULT '0',
  modtime varchar(20) DEFAULT '',
  folder varchar(256) DEFAULT '',
  fullpath varchar(512) DEFAULT '',
  folderid bigint(20) DEFAULT '0'
)`)
	mustOK(err)
}
