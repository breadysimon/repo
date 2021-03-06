package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	//"runtime/pprof"
)

func main() {
	//	f, _ := os.Create("d:/perf")
	//	pprof.StartCPUProfile(f)
	//	defer pprof.StopCPUProfile()
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
	loc := flag.String("b", "\\", "local dir baidu disk root")
	lst := flag.String("l", "", "list folders and files, 0 for root dir")
	ref := flag.Bool("r", false, "create reference files only")
	init := flag.Bool("i", false, "init database")
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
		RefOnly: *ref,
	}
	defer x.Exit()

	switch {
	case *init:
		fmt.Println("Init db")
		x.Init()
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
	case *lst != "":
		x.ListDir(*lst)
	}
	return
}
func main1() {
	fmt.Print(2 << 30)
}
