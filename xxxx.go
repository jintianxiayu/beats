package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

func dir(path string) string {
	return filepath.Dir(path)
}

func dirMode(permissions fs.FileMode) os.FileMode {
	mode := 0700
	if permissions&0070 > 0 {
		mode |= 0050
	}
	if permissions&0007 > 0 {
		mode |= 0005
	}
	return os.FileMode(mode)
}

func main() {
	// fileName := "/home/bichangshuo/aaa/bbb/ccc/xxxx.txt"
	fileName := "/root/aaa/bbb/ccc/xxxx.txt"
	var permissions fs.FileMode = 0666

	dir := dir(fileName)
	mode := dirMode(permissions)
	fmt.Printf("dir=%s mode=%o \n", dir, mode)

	err := os.MkdirAll(dir, mode)
	if err != nil {
		fmt.Printf("%+v \n", err)
	}

	f, _ := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, permissions)
	fileinfo, _ := f.Stat()
	fmt.Printf("specific permissions=%o , file=%s  mode=%o \n", permissions, fileinfo.Name(), fileinfo.Mode())
	f.Close()
}
