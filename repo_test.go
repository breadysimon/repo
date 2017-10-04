package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTest(t *testing.T) {
	fmt.Print(time.Now().Format("20060102150405"))
}

func ATestRepoAdd(t *testing.T) {
	workDir, _ := os.Getwd()
	baseDir, _ := filepath.Abs("\\")
	repo := &Repo{
		WorkDir: workDir,
		BaseDir: baseDir,
	}
	repo.WorkDir = "E:\\KuGou"
	repo.BaseDir = "E:\\"
	repo.RmDir()

	repo.AddDir()

	repo.WorkDir = "\\"
	repo.ListDir()
	repo.WorkDir = "14"
	repo.ListDir()
	repo.WorkDir = "17"
	repo.ListDir()
	repo.WorkDir = "E:\\KuGou"
	repo.CmpDir()
	repo.Search("7239")
	repo.Search("8701324709")

}
