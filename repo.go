package main

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	//	"strings"
	//	"time"

	_ "github.com/go-sql-driver/mysql"
)

/*
CREATE DATABASE repo;
DROP TABLE IF EXISTS files;
CREATE TABLE `files` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `md5` varchar(40) DEFAULT '',
  `name` varchar(256) DEFAULT '',
  `size` int(11) DEFAULT '0',
  `modtime` varchar(20) DEFAULT '',
  `folder` varchar(256) DEFAULT '',
  `fullpath` varchar(512) DEFAULT '',
  `folderid` bigint(20) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4
;
DROP TABLE IF EXISTS repo;
CREATE TABLE `repo` (
  `id` varchar(40) NOT NULL DEFAULT '',
  `refcount` int(11) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
;
DROP TABLE IF EXISTS folders;
CREATE TABLE `folders` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `md5` varchar(40) DEFAULT '',
  `name` varchar(256) DEFAULT '',
  `path` varchar(512) DEFAULT '',
  `parent` bigint(20) DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8mb4

*/

func MustOK(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
func (o *Repo) hash(file string, size int64) string {
	var sum string
	//t := time.Now()
	md5hash := md5.New()

	if size > 2*1024*1024*1024 {
		md5hash.Write([]byte(file))
		sum = fmt.Sprintf("x%d", size)
	}
	if size <= 0 {
		md5hash.Write([]byte(""))
		sum = fmt.Sprintf("x%x", md5hash.Sum(nil))
	} else {
		f, err := os.Open(file)
		MustOK(err)

		defer f.Close()

		_, err = io.Copy(md5hash, f)
		MustOK(err)

		sum = fmt.Sprintf("%x", md5hash.Sum(nil))

	}
	//tt := time.Since(t)

	return sum

}

func (o *Repo) getFolderID(path string) int64 {
	path = strings.TrimRight(path, "\\")
	parentPath, name := filepath.Split(path)
	log.Printf("path:%s,parentPath:%s,name:%s", path, parentPath, name)
	var folderID int64 = 0
	if parentPath != "" {
		folderID = o.getFolderID(parentPath)
	}

	o.connectDB()

	digest := o.hashStr(path)

	var n int64
	err := o.db.QueryRow("SELECT id FROM folders WHERE md5=?", digest).Scan(&n)
	if err == sql.ErrNoRows {
		stmt, err := o.db.Prepare("INSERT INTO folders(md5,path,name,parent) VALUES(?,?,?,?)")
		MustOK(err)

		_, err = stmt.Exec(digest, path, name, folderID)
		MustOK(err)
	}
	err = o.db.QueryRow("SELECT id FROM folders WHERE md5=?", digest).Scan(&n)
	MustOK(err)

	return n

}

func (o *Repo) copyFile(path string, repo string) {
	o.connectDB()
	var refcount int64
	err := o.db.QueryRow("SELECT refcount FROM repo WHERE id=?", repo).Scan(&refcount)
	if err == sql.ErrNoRows {
		repoBase, _ := filepath.Abs("e:/repo")
		distDir := filepath.Join(repoBase, repo[:1], repo[1:2])
		_, err := os.Stat(distDir)
		if os.IsNotExist(err) {
			os.MkdirAll(distDir, 0666)
		}
		srcFile := path
		distFile := filepath.Join(distDir, repo)
		src, err := os.Open(srcFile)
		if err != nil {
			log.Fatal(err)
		}
		defer src.Close()
		dest, err := os.OpenFile(distFile, os.O_CREATE|os.O_RDWR, 0666)
		if err != nil {
			log.Fatal(err)
		}
		defer dest.Close()
		_, err = io.Copy(dest, src)

		stmt, err := o.db.Prepare("INSERT INTO repo(id,refcount) VALUES(?,?)")
		MustOK(err)
		_, err = stmt.Exec(repo, 1)
		MustOK(err)

	} else {
		MustOK(err)
		stmt, err := o.db.Prepare("UPDATE repo SET refcount=? WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(refcount+1, repo)
		MustOK(err)
	}
}

func (o *Repo) connectDB() {
	var once sync.Once
	once.Do(func() {
		var err error
		o.db, err = sql.Open("mysql", "root@tcp(127.0.0.1:3306)/test")
		if err != nil {
			log.Fatal(err)
		}
	})
}
func (o *Repo) Exit() {
	if o.db != nil {
		o.db.Close()
	}
}
func (o *Repo) addFile(fullpath string, hash string, f os.FileInfo, relPath string, parentID int64) {
	o.connectDB()
	var n int64
	err := o.db.QueryRow("SELECT id from files WHERE fullpath=?", fullpath).Scan(&n)
	if err == sql.ErrNoRows {
		stmt, err := o.db.Prepare("INSERT INTO files(md5,size,fullpath,name,modtime,folder,folderid) VALUES(?,?,?,?,?,?,?)")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(hash, f.Size(), fullpath, f.Name(), f.ModTime().Format("20060102150405"), relPath, parentID)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		if err != nil {
			log.Fatal(err)
		}
		stmt, err := o.db.Prepare("UPDATE files SET md5=?,size=?,modtime=? WHERE id=?")
		if err != nil {
			log.Fatal(err)
		}
		_, err = stmt.Exec(hash, f.Size(), f.ModTime(), n)
		if err != nil {
			log.Fatal(err)
		}

	}
}

//遍历srcDir,将所有文件加入数据库
func (o *Repo) AddDir() {
	err := filepath.Walk(o.WorkDir, func(fullpath string, f os.FileInfo, err error) error {
		MustOK(err)
		relPath, _ := filepath.Rel(o.BaseDir, fullpath)

		if f.IsDir() {
			_ = o.getFolderID(relPath)

		} else {
			size := f.Size()
			hash := o.hash(fullpath, size)
			parentPath := filepath.Dir(relPath)
			parentID := o.getFolderID(parentPath)

			o.addFile(fullpath, hash, f, parentPath, parentID)

			o.copyFile(fullpath, hash)

			fmt.Println(hash, ",", parentPath, ",", f.Name(), ",", size)
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}

}
func (o *Repo) Search(key string) {
	o.connectDB()
	key = strings.Replace(key, "\\", "%\\", -1)
	rows, err := o.db.Query("select id,md5,name,size,folder,fullpath from files where fullpath like ?", "%"+key+"%")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var (
		id, size                    int64
		name, fullpath, md5, folder string
	)

	for rows.Next() {
		err := rows.Scan(&id, &md5, &name, &size, &folder, &fullpath)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(folder, name)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}
}
func (o *Repo) hashStr(s string) string {
	md5Ctx := md5.New()
	md5Ctx.Write([]byte(s))
	digest := fmt.Sprintf("%x", md5Ctx.Sum(nil))
	return digest
}
func (o *Repo) removeFile(md5 string) {
	o.connectDB()
	var refcount int64

	MustOK(o.db.QueryRow("SELECT refcount FROM repo WHERE id=?", md5).Scan(&refcount))
	log.Printf("to delete md5=%s, refcount=%d", md5, refcount)
	if refcount > 1 {
		log.Printf("refcount dec, md5=%s", md5)
		stmt, err := o.db.Prepare("UPDATE repo SET refcount=? WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(refcount-1, md5)
		MustOK(err)
	} else {
		f := filepath.Join("E:\\repo", md5[:1], md5[1:2], md5)
		log.Printf("remove file %s", f)
		MustOK(os.Remove(f))
		stmt, err := o.db.Prepare("DELETE FROM repo WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(md5)
		MustOK(err)
	}

}
func (o *Repo) RmDir() {
	relPath, _ := filepath.Rel(o.BaseDir, o.WorkDir)
	pattern := strings.Replace(relPath, "\\", "%\\", -1) + "%"
	log.Printf("Delete dir '%s' from repo:", relPath)
	log.Print(pattern)

	o.connectDB()

	{ //因为文件可能有多重引用,不能直接删除
		rows, err := o.db.Query("select md5,fullpath from files where folder like ?", pattern)
		MustOK(err)
		defer rows.Close()
		var md5, fullpath string

		for rows.Next() {
			MustOK(rows.Scan(&md5, &fullpath))
			log.Printf("delte file md5=%s, path=%s", md5, fullpath)
			o.removeFile(md5)
		}
		MustOK(rows.Err())

	}
	{
		stmt, err := o.db.Prepare("DELETE FROM files WHERE folder like ?")
		MustOK(err)
		res, err := stmt.Exec(pattern)
		MustOK(err)
		rowCnt, err := res.RowsAffected()
		MustOK(err)
		log.Printf("%d records in files are deleted.", rowCnt)
	}

	{
		stmt, err := o.db.Prepare("DELETE FROM folders WHERE path like ?")
		MustOK(err)
		res, err := stmt.Exec(pattern)
		MustOK(err)
		rowCnt, err := res.RowsAffected()
		MustOK(err)
		log.Printf("%d records in folders are deleted.", rowCnt)
	}

}

func (o *Repo) CmpDir() {
	o.connectDB()
	reportFile := path.Join(o.WorkDir, "duplicates-found-report.txt")
	os.Remove(reportFile)
	rpt, err := os.Create(reportFile) //os.OpenFile(reportFile, os.O_CREATE|os.O_RDWR, 0666)
	MustOK(err)

	defer rpt.Close()

	err = filepath.Walk(o.WorkDir, func(fullpath string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if !f.IsDir() {
			size0 := f.Size()
			hash := o.hash(fullpath, size0)
			var (
				name, folder   string
				size, folderID int64
			)
			err := o.db.QueryRow("SELECT size,name,folder,folderid FROM files WHERE md5=?", hash).Scan(&size, &name, &folder, &folderID)
			if err == sql.ErrNoRows {
				return nil
			}
			MustOK(err)
			if size == size0 {
				out := fmt.Sprintf("%s\\%s\n", folder, name)
				fmt.Fprint(rpt, out)
				fmt.Print(out)
			}

		}
		return nil
	})
	MustOK(err)

}

type Repo struct {
	WorkDir string
	BaseDir string
	db      *sql.DB
}

func (o *Repo) ListDir() {
	var parentID int64
	if o.WorkDir == "\\" {
		parentID = 0
	} else {
		parentID, _ = strconv.ParseInt(o.WorkDir, 10, 64)
	}

	fmt.Printf("List %s\n", o.WorkDir)

	o.connectDB()
	rows, err := o.db.Query("SELECT id ,name from folders where parent=?", parentID)
	MustOK(err)
	for rows.Next() {
		var (
			id   int64
			name string
		)
		err = rows.Scan(&id, &name)
		MustOK(err)
		fmt.Printf("[%d]\t%s\n", id, name)
	}
	rows, err = o.db.Query("SELECT md5 ,name from files where folderid=?", parentID)
	MustOK(err)
	for rows.Next() {
		var (
			sum  string
			name string
		)
		rows.Scan(&sum, &name)
		fmt.Printf("[%s]\t%s\n", sum, name)
	}
}
