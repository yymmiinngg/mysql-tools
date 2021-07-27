package sql

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
)

func ChgSqlFile(inputFile, outputFile string) {
	fmt.Println("    ", inputFile)
	indexHelper := NewIndexHelper(inputFile)
	insertHelper := NewMergeInsertHelper(inputFile)

	// 打开源文件
	fi, err := os.Open(inputFile)
	if err != nil {
		panic(err)
	}
	defer fi.Close()

	// 打开目标文件
	fo, err := os.OpenFile(outputFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModeAppend)
	if err != nil {
		panic(err)
	}
	defer fo.Close()
	_, err = fo.WriteString("-- FOR SPEED SQL --\n")
	if err != nil {
		fmt.Println("Error in write file", outputFile)
		panic(err)
	}

	// 读取文件
	rd := bufio.NewReader(fi)
	for {
		line, err := rd.ReadString('\n') // 以'\n'为结束符读入一行

		if line == "" {
			if err != nil || io.EOF == err {
				break
			}
		}

		// 尝试insert合并
		merged := insertHelper.InputLine(line)
		if merged {
			// 有合并
			mergeInsertBean := insertHelper.TakeFullMergeInsertBean()
			if nil != mergeInsertBean { // 合并缓存满
				fo.WriteString(mergeInsertBean.MakeSQL())
			}
			continue
		}

		// insert段落结束
		if insertHelper.HasData() {
			fo.WriteString(insertHelper.MakeSQL())
			insertHelper.Clear()
		}

		// 当前行原文输出
		fo.WriteString(line)

		// 尝试索引项分析
		indexHelper.InputLine(strings.TrimSpace(line))
	}

	// 写入删除和增加索引的文件
	if len(indexHelper.CreateTalbeList) > 0 {
		{
			dropFile := outputFile + ".drop-index.sql"
			dropSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				dropSql += createTable.MakeDropSQL()
			}
			ioutil.WriteFile(dropFile, []byte(dropSql), os.ModeAppend)
			fmt.Println("    ", inputFile)
		}
		{
			addFile := outputFile + ".add-index.sql"
			addSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				addSql += createTable.MakeAddSQL()
			}
			ioutil.WriteFile(addFile, []byte(addSql), os.ModeAppend)
			fmt.Println("    ", inputFile)
		}
	}
}
