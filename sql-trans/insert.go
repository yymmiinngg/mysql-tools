package sql

import (
	"bytes"
	"strings"
)

type InsertHelperBean struct {
	InputFile      string
	mergeInsertMap map[string]*MergeInsertBean
}

type MergeInsertBean struct {
	INSERT string   // INSERT INTO `table_name`
	VALUES []string // `value1`, `value2`, ... , `valuen`
	count  int
}

// 批大小
var BATCH_SIZE = 0

func NewMergeInsertHelper(inputFile string) *InsertHelperBean {
	return &InsertHelperBean{
		InputFile:      inputFile,
		mergeInsertMap: make(map[string]*MergeInsertBean, 10),
	}
}

// return: true 合并成功 | false 无需合并
func (it *InsertHelperBean) InputLine(linesource string) bool {
	line := strings.TrimSpace(linesource)

	valueString := strings.TrimSpace(GetSectionOutter(line, "VALUES (", ")"))
	// 没有 VALUES 的行
	if valueString == "" {
		return false
	}

	insertString := strings.TrimSpace(GetLeft(line, "VALUES"))
	tableName := ""
	// 以 INSERGT INTO 开头
	if strings.Index(insertString, "INSERT INTO") == 0 {
		tableName = strings.TrimSpace(GetSection(line, "INSERT INTO ", " "))
	} else
	// 以 INSERGT IGNORE INTO 开头
	if strings.Index(insertString, "INSERT IGNORE INTO") == 0 {
		tableName = strings.TrimSpace(GetSection(line, "INSERT IGNORE INTO ", " "))
	} else
	// 不是 INSERT 开头
	{
		return false
	}

	// 合并
	mergeInsertBean, ok := it.mergeInsertMap[tableName]
	if !ok {
		mergeInsertBean = &MergeInsertBean{
			INSERT: insertString,
			VALUES: []string{valueString},
			count:  len(valueString),
		}
		it.mergeInsertMap[tableName] = mergeInsertBean
	} else {
		mergeInsertBean.VALUES = append(mergeInsertBean.VALUES, valueString)
		mergeInsertBean.count += len(valueString)
	}
	return true
}

func (it *InsertHelperBean) HasData() bool {
	return len(it.mergeInsertMap) > 0
}

func (it *InsertHelperBean) MakeSQL() string {
	var buffer bytes.Buffer
	for _, value := range it.mergeInsertMap {
		buffer.WriteString(value.MakeSQL())
	}

	return buffer.String()
}

func (it *InsertHelperBean) Clear() {
	it.mergeInsertMap = make(map[string]*MergeInsertBean, 10)
}

func (it *InsertHelperBean) TakeFullMergeInsertBean() *MergeInsertBean {
	for key, value := range it.mergeInsertMap {
		if value.count >= BATCH_SIZE {
			delete(it.mergeInsertMap, key)
			return value
		}
	}
	return nil
}

func (it *MergeInsertBean) MakeSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString(it.INSERT)
	buffer.WriteString(" VALUES ")
	first := true
	for _, values := range it.VALUES {
		if !first {
			buffer.WriteString("\n  ,")
		} else {
			first = false
		}
		buffer.WriteString("(")
		buffer.WriteString(values)
		buffer.WriteString(")")
	}
	buffer.WriteString(";\n")
	return buffer.String()
}
