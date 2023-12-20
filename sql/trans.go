package sql

import (
	"bufio"
	"fmt"
	"io"
	"os"
)

var SET_FOREIGN_KEY_CHECKS_OFF = false

func ChgSqlFile(inputFile, outputFile string) {
	ofStruct := outputFile + ".struct.sql"
	ofInsert := outputFile + ".insert.sql"
	fmt.Println("    ", inputFile, "->", outputFile+".*")
	fmt.Println("        ", ofStruct)
	fmt.Println("        ", ofInsert)
	indexHelper := NewIndexHelper(inputFile)
	insertHelper := NewMergeInsertHelper(inputFile)

	// 读取文件
	var r *bufio.Reader
	var wStruct *bufio.Writer
	var wInsert *bufio.Writer

	{
		// 打开源文件
		fi, err := os.Open(inputFile)
		if err != nil {
			panic(err)
		}
		defer fi.Close()

		// 打开目标文件
		fo, err := os.OpenFile(ofStruct, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer fo.Close()

		// 打开目标文件
		foInsert, err := os.OpenFile(ofInsert, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, os.ModePerm)
		if err != nil {
			panic(err)
		}
		defer foInsert.Close()

		// 读取文件
		r = bufio.NewReaderSize(fi, 1024*16)
		wStruct = bufio.NewWriterSize(fo, 1024*16)
		wInsert = bufio.NewWriterSize(foInsert, 1024*16)
		defer func() {
			// 开启外键约束
			if SET_FOREIGN_KEY_CHECKS_OFF {
				wInsert.WriteString("\n-- Open foreign checks\n")
				wInsert.WriteString("SET FOREIGN_KEY_CHECKS=1;\n")
				wInsert.WriteString("-- ----------------------\n")
			}
			wInsert.Flush()
		}()

		// 临时关闭外键约束
		if SET_FOREIGN_KEY_CHECKS_OFF {
			wInsert.WriteString("-- Close foreign checks\n")
			wInsert.WriteString("SET FOREIGN_KEY_CHECKS=0;\n")
			wInsert.WriteString("-- ----------------------\n\n")
		}

		_, err = wStruct.WriteString("-- FOR SPEED SQL --\n")
		if err != nil {
			fmt.Println("Error in write file", outputFile)
			panic(err)
		}
	}

	for {
		line, err := r.ReadString('\n') // 以'\n'为结束符读入一行

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
				wInsert.WriteString(mergeInsertBean.MakeSQL())
			}
			continue
		}
		// insert段落结束
		if insertHelper.HasData() {
			wInsert.WriteString(insertHelper.MakeSQL())
			insertHelper.Clear()
		}

		// 尝试索引项分析
		line, err = indexHelper.InputLine(line)
		if err != nil {
			fmt.Println("Error in handle file", outputFile)
			fmt.Println("      in input line", line)
			panic(err)
		}
		// 当前行原文输出
		wStruct.WriteString(line)
	}

	// 写入删除和增加索引的文件
	if len(indexHelper.CreateTalbeList) > 0 {
		{
			dropFile := outputFile + ".drop-index.sql"
			dropSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				dropSql += createTable.MakeDropIndexSQL()
			}
			os.WriteFile(dropFile, []byte(dropSql), os.ModePerm)
			fmt.Println("        ", dropFile)
		}
		{
			dropFile := outputFile + ".drop-fk.sql"
			dropSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				dropSql += createTable.MakeDropForeignKeySQL()
			}
			os.WriteFile(dropFile, []byte(dropSql), os.ModePerm)
			fmt.Println("        ", dropFile)
		}
		{
			addFile := outputFile + ".add-index.sql"
			addSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				addSql += createTable.MakeAddIndexSQL()
			}
			os.WriteFile(addFile, []byte(addSql), os.ModePerm)
			fmt.Println("        ", addFile)
		}
		{
			addFile := outputFile + ".add-fk.sql"
			addSql := ""
			for _, createTable := range indexHelper.CreateTalbeList {
				addSql += createTable.MakeAddForeignKeySQL()
			}
			os.WriteFile(addFile, []byte(addSql), os.ModePerm)
			fmt.Println("        ", addFile)
		}
	}
}
