package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"mysqltools/sql"
)

func usage(flagset *flag.FlagSet) {
	fmt.Println("Usage: mysqltools [OPTIONS] <CMD>")
	fmt.Print("OPTIONS ")
	flagset.Usage()
	fmt.Println("CMD:")
	fmt.Println("  sql-trans: trans sql file for import speed.")
}

func main() {
	var CMD string
	var InputDir, OutputDir, FileExt string
	var ShowHelp bool
	var MaxValuesCount int

	argsflag := flag.NewFlagSet("mysqltools", flag.PanicOnError)

	argsflag.StringVar(&InputDir, "i", "./", "input dir")
	argsflag.StringVar(&OutputDir, "o", "", "output dir")
	argsflag.StringVar(&FileExt, "e", ".sql", "file ext of sql")
	argsflag.BoolVar(&ShowHelp, "h", false, "show help")
	argsflag.IntVar(&MaxValuesCount, "m", 100, "show help")
	argsflag.Parse(os.Args[2:])

	if len(os.Args) < 2 {
		usage(argsflag)
		return
	}
	CMD = os.Args[1]
	if CMD != "sql-trans" {
		usage(argsflag)
		return
	}

	if ShowHelp {
		usage(argsflag)
		return
	}
	if OutputDir == "" {
		log.Panic("Please set output dir with -o")
	}

	// 实际目录
	InputDir, _ = filepath.Abs(InputDir)
	OutputDir, _ = filepath.Abs(OutputDir)
	if InputDir == OutputDir {
		log.Panic("can not be input dir == output dir")
	}
	sql.MAX_VALUES_COUNT = MaxValuesCount

	listDir(InputDir, OutputDir, FileExt)
}

func listDir(inputDir, outputDir, fileExt string) {
	fmt.Println("####", inputDir)

	// 创建输出目录
	_, err := os.Stat(outputDir)
	if err != nil {
		if os.IsNotExist(err) { // 根据错误类型进行判断
			os.MkdirAll(outputDir, os.ModePerm)
		}
	}

	//获取文件或目录相关信息
	fileInfoList, err := ioutil.ReadDir(inputDir)
	if err != nil {
		log.Fatal(err)
	}
	for i := range fileInfoList {
		newInFile := linkFilePath(inputDir, fileInfoList[i].Name())
		newOutFile := linkFilePath(outputDir, fileInfoList[i].Name())
		if fileInfoList[i].IsDir() {
			listDir(newInFile, newOutFile, fileExt)
		} else {
			if path.Ext(newInFile) == fileExt {
				sql.ChgSqlFile(newInFile, newOutFile)
			}
		}
	}
}

func linkFilePath(dir, file string) string {
	if strings.LastIndex(dir, "/") == len(dir)-1 {
		return dir + file
	}
	if strings.LastIndex(dir, "\\") == len(dir)-1 {
		return dir + file
	}
	return dir + string(os.PathSeparator) + file
}
