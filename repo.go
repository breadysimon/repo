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
	"time"

	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/mattn/go-sqlite3"
)

func MustOK(err error) {
	if err != nil {
		fmt.Println(err)
		log.Fatal(err)
	}
}
func (o *Repo) hash(fullpath string) string {
	var sum string
	//t := time.Now()
	md5hash := md5.New()

	finfo, err := os.Stat(fullpath)
	MustOK(err)
	size := finfo.Size()
	if size > 10<<30 { //大于10G不生成MD5,直接用size
		sum = fmt.Sprintf("x%d", size)
	} else {
		f, err := os.Open(fullpath)
		MustOK(err)

		defer f.Close()

		_, err = io.Copy(md5hash, f)
		MustOK(err)

		sum = fmt.Sprintf("%x", md5hash.Sum(nil))

	}
	//tt := time.Since(t)

	return sum

}

func (o *Repo) getConn() *sql.DB {
	var err error
	if o.db1 == nil {
		o.db1, err = sql.Open("sqlite3", "e:/repo/repo.db")
		MustOK(err)
	}
	return o.db1
}
func (o *Repo) releaseConn(db *sql.DB) {
	//db.Close()
	//
}
func (o *Repo) getFolderID(path string) int64 {
	path = strings.TrimRight(path, "\\")
	parentPath, name := filepath.Split(path)
	//log.Printf("path:%s,parentPath:%s,name:%s", path, parentPath, name)
	var folderID int64 = 0
	if parentPath != "" {
		folderID = o.getFolderID(parentPath)
	}

	digest := o.hashStr(path)

	db := o.getConn()
	defer o.releaseConn(db)

	var n int64
	err := db.QueryRow("SELECT id FROM folders WHERE md5=?", digest).Scan(&n)
	if err == sql.ErrNoRows {
		stmt, err := db.Prepare("INSERT INTO folders(md5,path,name,parent) VALUES(?,?,?,?)")
		MustOK(err)
		_, err = stmt.Exec(digest, path, name, folderID)

		MustOK(err)
	}
	err = db.QueryRow("SELECT id FROM folders WHERE md5=?", digest).Scan(&n)
	MustOK(err)

	return n

}
func (o *Repo) getRepoPath(hash string) string {
	repoBase, _ := filepath.Abs("e:/repo")
	distDir := filepath.Join(repoBase, hash[:1], hash[1:2], hash[2:3])
	_, err := os.Stat(distDir)
	if os.IsNotExist(err) {
		os.MkdirAll(distDir, 0666)
	}
	return filepath.Join(distDir, hash)
}
func (o *Repo) getRefPath(path string) string {
	repoBase, _ := filepath.Abs("e:/repo")
	dir, filename := filepath.Split(path)
	relDir, _ := filepath.Rel(o.BaseDir, dir)

	distDir := filepath.Join(repoBase, "ref", relDir)
	_, err := os.Stat(distDir)
	if os.IsNotExist(err) {
		os.MkdirAll(distDir, 0666)
	}
	//fmt.Printf("\n------------------\n%s,%s\n", distDir, filename)
	return filepath.Join(distDir, filename)
}
func (o *Repo) createRefDir(relPath string) {
	repoBase, _ := filepath.Abs("e:/repo")
	//relDir, _ := filepath.Rel(o.BaseDir, path)

	distDir := filepath.Join(repoBase, "ref", relPath)
	_, err := os.Stat(distDir)
	if os.IsNotExist(err) {
		log.Printf("create ref dir: %s", distDir)
		os.MkdirAll(distDir, 0666)
	}
	return
}
func (o *Repo) copyFile(path string, hash string) {
	db := o.getConn()
	defer o.releaseConn(db)

	var refcount int64
	finfo, _ := os.Stat(path)
	err := db.QueryRow("SELECT refcount FROM repo WHERE id=?", hash).Scan(&refcount)
	if err == sql.ErrNoRows {
		log.Printf("copy new file:%s,hash=%s", path, hash)

		if !o.RefOnly {
			srcFile := path
			distFile := o.getRepoPath(hash)
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
		}

		stmt, err := db.Prepare("INSERT INTO repo(id,refcount) VALUES(?,?)")
		MustOK(err)
		_, err = stmt.Exec(hash, 1)

		MustOK(err)

	} else {
		MustOK(err)
		log.Printf("add refcount to %d, hash=%s", refcount+1, hash)

		stmt, err := db.Prepare("UPDATE repo SET refcount=? WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(refcount+1, hash)

		MustOK(err)
	}
	refFile := o.getRefPath(path)
	{
		f, err := os.Create(refFile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		rel, _ := filepath.Rel(o.BaseDir, path)
		fmt.Fprintf(f, "SIMON#REPO:%s,%d,%d,%s", hash, finfo.Size(), finfo.ModTime().Unix(), rel)
	}
	fmt.Printf("added [%s]%s\n", hash, path)
}

/*
sqlite3:
DROP TABLE IF EXISTS files;
CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, md5 varchar (40) DEFAULT '', name varchar (256) DEFAULT '', size int (11) DEFAULT '0', modtime varchar (20) DEFAULT '', folder varchar (256) DEFAULT '', fullpath varchar (512) DEFAULT '', folderid INTEGER DEFAULT '0');
DROP TABLE IF EXISTS folders;
CREATE TABLE folders (id INTEGER PRIMARY KEY AUTOINCREMENT, md5 varchar (40) DEFAULT '', name varchar (256) DEFAULT '', path varchar (512) DEFAULT '', parent INTEGER DEFAULT '0');
DROP TABLE IF EXISTS repo;
CREATE TABLE repo (id varchar (40) NOT NULL DEFAULT '', refcount int (11) DEFAULT '0', PRIMARY KEY (id));
*/
func (o *Repo) connectDB() {
	var once sync.Once
	if o.db1 == nil {
		once.Do(func() {
			var err error
			o.db1, err = sql.Open("sqlite3", "e:/repo/repo.db")
			MustOK(err)
		})
	}
}
func (o *Repo) Init() {
	var err error
	db, err := sql.Open("sqlite3", "e:/repo/repo.db")
	MustOK(err)
	_, err = db.Exec(`CREATE TABLE files (id INTEGER PRIMARY KEY AUTOINCREMENT, md5 varchar (40) DEFAULT '', name varchar (256) DEFAULT '', size int (11) DEFAULT '0', modtime varchar (20) DEFAULT '', folder varchar (256) DEFAULT '', fullpath varchar (512) DEFAULT '', folderid INTEGER DEFAULT '0');`)
	MustOK(err)
	_, err = db.Exec(`CREATE INDEX filemd5_idx ON files(md5);`)
	//MustOK(err)
	_, err = db.Exec(`CREATE TABLE folders (id INTEGER PRIMARY KEY AUTOINCREMENT, md5 varchar (40) DEFAULT '', name varchar (256) DEFAULT '', path varchar (512) DEFAULT '', parent INTEGER DEFAULT '0');`)
	MustOK(err)
	_, err = db.Exec(`CREATE INDEX foldermd5_idx ON folders(md5);`)
	MustOK(err)
	_, err = db.Exec(`CREATE TABLE repo (id varchar (40) NOT NULL DEFAULT '', refcount int (11) DEFAULT '0', PRIMARY KEY (id));`)
	MustOK(err)
	err = db.Close()
	MustOK(err)

}
func (o *Repo) Exit() {
	if o.db1 != nil {
		o.db1.Close()
	}
}
func (o *Repo) decRef(hash string) {
	db := o.getConn()
	defer o.releaseConn(db)

	var n int64
	MustOK(db.QueryRow("SELECT refcount from repo WHERE id=?", hash).Scan(&n))
	n--
	if n > 0 {
		log.Printf("decrease refcount to %d, id=%s", n, hash)

		stmt, err := db.Prepare("UPDATE repo SET refcount=? WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(n, hash)

		MustOK(err)
	} else {
		log.Printf("remove old copy(ref=0): %s", hash)

		stmt, err := db.Prepare("DELETE FROM repo WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(hash)

		MustOK(err)
		err = os.Remove(o.getRepoPath(hash))
		if err != nil {
			log.Print(err)
			fmt.Printf("fail to remove:%s\n", o.getRepoPath(hash))
		}
	}
}
func (o *Repo) addFile(fullpath string, hash string, f os.FileInfo, relPath string, parentID int64) {
	db := o.getConn()
	defer o.releaseConn(db)

	var n int64
	var oldhash string
	err := db.QueryRow("SELECT id,md5 from files WHERE name=? AND folder=?", f.Name(), relPath).Scan(&n, &oldhash)
	if err == sql.ErrNoRows {

		stmt, err := db.Prepare("INSERT INTO files(md5,size,fullpath,name,modtime,folder,folderid) VALUES(?,?,?,?,?,?,?)")
		MustOK(err)
		_, err = stmt.Exec(hash, f.Size(), fullpath, f.Name(), f.ModTime().Format("20060102150405"), relPath, parentID)

		MustOK(err)
		o.copyFile(fullpath, hash)
	} else {
		MustOK(err)
		if hash == oldhash {
			log.Printf("Ingore same path and hash:%s,%s,%s", relPath, f.Name(), hash)
		} else {

			stmt, err := db.Prepare("UPDATE files SET md5=?,size=?,modtime=? WHERE id=?")
			MustOK(err)
			_, err = stmt.Exec(hash, f.Size(), f.ModTime(), n)

			MustOK(err)
			o.decRef(oldhash)
			o.copyFile(fullpath, hash)
		}

	}
}

//遍历srcDir,将所有文件加入数据库
func (o *Repo) AddDirNew() {
	var wg sync.WaitGroup

	t := time.Now()
	var count, data int64 = 0, 0
	err := filepath.Walk(o.WorkDir, func(fullpath string, f os.FileInfo, err error) error {
		MustOK(err)
		relPath, _ := filepath.Rel(o.BaseDir, fullpath)

		if f.IsDir() {
			_ = o.getFolderID(relPath)

		} else {
			go func(fullpath string) {
				wg.Add(1)

				finfo, err := os.Stat(fullpath)
				MustOK(err)
				count++
				data = data + finfo.Size()

				hash := o.hash(fullpath)

				parentPath := filepath.Dir(relPath)
				parentID := o.getFolderID(parentPath)

				o.addFile(fullpath, hash, f, parentPath, parentID)

				wg.Done()
			}(fullpath)
		}
		return nil
	})
	wg.Wait()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("\nadd %d files with %d M in %0.0f seconds.\n", count, data/1000000, time.Since(t).Seconds())
}

//遍历srcDir,将所有文件加入数据库
func (o *Repo) AddDir() {
	err := filepath.Walk(o.WorkDir, func(fullpath string, f os.FileInfo, err error) error {
		MustOK(err)
		relPath, _ := filepath.Rel(o.BaseDir, fullpath)

		if f.IsDir() {
			_ = o.getFolderID(relPath)
			o.createRefDir(relPath)

		} else {
			hash := o.hash(fullpath)
			parentPath := filepath.Dir(relPath)
			parentID := o.getFolderID(parentPath)

			o.addFile(fullpath, hash, f, parentPath, parentID)
		}
		return nil
	})
	if err != nil {
		fmt.Println(err)
	}

}
func (o *Repo) Search(key string) {
	db := o.getConn()
	defer o.releaseConn(db)

	key = strings.Replace(key, "\\", "%\\", -1)
	rows, err := db.Query("select id,md5,name,size,folder,fullpath,folderid from files where fullpath like ?  order by name", "%"+key+"%")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	var (
		id, size, folderid          int64
		name, fullpath, md5, folder string
	)

	for rows.Next() {
		err := rows.Scan(&id, &md5, &name, &size, &folder, &fullpath, &folderid)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%-8d %s: %s\n", folderid, folder, name)
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
	db := o.getConn()
	defer o.releaseConn(db)

	var refcount int64

	MustOK(db.QueryRow("SELECT refcount FROM repo WHERE id=?", md5).Scan(&refcount))
	log.Printf("to delete md5=%s, refcount=%d", md5, refcount)
	if refcount > 1 {
		log.Printf("refcount dec, md5=%s", md5)

		stmt, err := db.Prepare("UPDATE repo SET refcount=? WHERE id=?")
		MustOK(err)
		_, err = stmt.Exec(refcount-1, md5)

		MustOK(err)
	} else {
		f := filepath.Join("E:\\repo", md5[:1], md5[1:2], md5)
		log.Printf("remove file %s", f)
		MustOK(os.Remove(f))

		stmt, err := db.Prepare("DELETE FROM repo WHERE id=?")
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

	db := o.getConn()
	defer o.releaseConn(db)

	{
		rows, err := db.Query("select md5,fullpath from files where folder like ?", pattern)
		MustOK(err)
		defer rows.Close()
		var md5, fullpath string

		for rows.Next() {
			MustOK(rows.Scan(&md5, &fullpath))
			log.Printf("delete file md5=%s, path=%s", md5, fullpath)
			o.removeFile(md5) //因为文件可能有多重引用,不能直接删除
		}
		MustOK(rows.Err())

	}
	{

		stmt, err := db.Prepare("DELETE FROM files WHERE folder like ?")
		MustOK(err)
		res, err := stmt.Exec(pattern)

		MustOK(err)
		rowCnt, err := res.RowsAffected()
		MustOK(err)
		log.Printf("%d records in files are deleted.", rowCnt)
	}

	{

		stmt, err := db.Prepare("DELETE FROM folders WHERE path like ?")
		MustOK(err)
		res, err := stmt.Exec(pattern)

		MustOK(err)
		rowCnt, err := res.RowsAffected()
		MustOK(err)
		log.Printf("%d records in folders are deleted.", rowCnt)
	}

}

func (o *Repo) CmpDir() {
	db := o.getConn()
	defer o.releaseConn(db)

	reportFile := path.Join(o.WorkDir, "duplicates-found-report.txt")
	os.Remove(reportFile)
	rpt, err := os.Create(reportFile)
	MustOK(err)

	defer rpt.Close()

	err = filepath.Walk(o.WorkDir, func(fullpath string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if !f.IsDir() {
			hash := o.hash(fullpath)
			var (
				name, folder   string
				size, folderID int64
			)
			err := db.QueryRow("SELECT size,name,folder,folderid FROM files WHERE md5=?", hash).Scan(&size, &name, &folder, &folderID)
			if err == sql.ErrNoRows {
				return nil
			}
			MustOK(err)
			msg := fmt.Sprintf("%s\\%s\n", folder, name)
			fmt.Fprint(rpt, msg)
			fmt.Print(msg)

		}
		return nil
	})
	MustOK(err)

}

type Repo struct {
	WorkDir string
	BaseDir string
	db1     *sql.DB
	RefOnly bool
}

func (o *Repo) ListDir(folderid string) {
	var parentID int64
	parentID, _ = strconv.ParseInt(folderid, 10, 64)

	db := o.getConn()
	defer o.releaseConn(db)

	rows, err := db.Query("SELECT name from folders where id=?", parentID)
	MustOK(err)
	for rows.Next() {
		var (
			name string
		)
		err = rows.Scan(&name)
		MustOK(err)
		fmt.Printf("List %s:\n", name)
	}
	rows, err = db.Query("SELECT id ,name from folders where parent=? order by name", parentID)
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
	rows, err = db.Query("SELECT md5 ,name from files where folderid=? order by name", parentID)
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
