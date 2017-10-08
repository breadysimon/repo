package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepoAdd(t *testing.T) {
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

	repo.ListDir("0")
	repo.ListDir("14")
	repo.ListDir("17")
	repo.CmpDir()
	repo.Search("7239")
	repo.Search("8701324709")

}
