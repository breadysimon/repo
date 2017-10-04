package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
)

func main() {
	logfile, err := os.Create("e:/repo/cli.log")
	if err != nil {
		log.Fatal(err)
	}
	defer logfile.Close()
	log.SetOutput(logfile)
	log.SetFlags(log.Lshortfile | log.LstdFlags)

	add := flag.Bool("a", false, "generate md5 checksum and index files into db.")
	rmdir := flag.Bool("d", false, "remove dir records before index.")
	key := flag.String("s", "", "search file, keyword required.")
	cmp := flag.Bool("c", false, "compare dir to find duplicated files")
	loc := flag.String("l", "\\", "local dir baidu disk root")
	lst := flag.Bool("r", false, "list folders and files")
	flag.Parse()

	wd := flag.Arg(0)
	if wd == "" {
		wd, _ = os.Getwd()
	}
	wd, _ = filepath.Abs(wd)
	basedir, _ := filepath.Abs(*loc)
	x := &Repo{
		WorkDir: wd,
		BaseDir: basedir,
	}
	defer x.Exit()

	switch {
	case *add:
		fmt.Printf("Add local dir '%s' to the repository.\n", x.WorkDir)
		x.AddDir()
	case *key != "":
		fmt.Printf("Search files with keyword '%s'\n", *key)
		x.Search(*key)
	case *rmdir:
		fmt.Printf("Delete repo dir '%s'\n", x.WorkDir)
		x.RmDir()
	case *cmp:
		fmt.Printf("List of duplicates:\n")
		x.CmpDir()
	case *lst:
		fmt.Printf("List files in %s:\n", x.WorkDir)
		x.ListDir()
	}
	return
}
