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
	INSERT     string   // INSERT INTO `table_name`
	TABLE_NAME string   // `table_name`
	VALUES     []string // `value1`, `value2`, ... , `valuen`
	count      int
}

var MAX_VALUES_COUNT = 2

func NewMergeInsertHelper(inputFile string) *InsertHelperBean {
	return &InsertHelperBean{
		InputFile:      inputFile,
		mergeInsertMap: make(map[string]*MergeInsertBean, 10),
	}
}

// return: true 合并成功 | false 无需合并
func (this *InsertHelperBean) InputLine(line string) bool {

	if strings.Index(line, "INSERT INTO") != 0 {
		return false
	}

	tableName := strings.TrimSpace(GetSection(line, "INSERT INTO ", " "))

	mergeInsertBean, ok := this.mergeInsertMap[tableName]
	if !ok {
		mergeInsertBean = &MergeInsertBean{
			INSERT:     strings.TrimSpace(GetLeft(line, "VALUES")),
			TABLE_NAME: tableName,
			VALUES:     make([]string, 0, MAX_VALUES_COUNT),
			count:      0,
		}
		this.mergeInsertMap[tableName] = mergeInsertBean
	}
	mergeInsertBean.VALUES = append(mergeInsertBean.VALUES, strings.TrimSpace(GetSectionOutter(line, "VALUES (", ")")))
	mergeInsertBean.count++

	return true
}

func (this *InsertHelperBean) HasData() bool {
	return len(this.mergeInsertMap) > 0
}

func (this *InsertHelperBean) MakeSQL() string {
	var buffer bytes.Buffer
	for _, value := range this.mergeInsertMap {
		buffer.WriteString(value.MakeSQL())
	}

	return buffer.String()
}

func (this *InsertHelperBean) Clear() {
	this.mergeInsertMap = make(map[string]*MergeInsertBean, 10)
}

func (this *InsertHelperBean) TakeFullMergeInsertBean() *MergeInsertBean {
	for key, value := range this.mergeInsertMap {
		if value.count >= MAX_VALUES_COUNT {
			delete(this.mergeInsertMap, key)
			return value
		}
	}
	return nil
}

func (this *MergeInsertBean) MakeSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString(this.INSERT)
	buffer.WriteString(" VALUES ")
	first := true
	for _, values := range this.VALUES {
		if !first {
			buffer.WriteString(",")
		} else {
			first = false
		}
		buffer.WriteString("\n    (")
		buffer.WriteString(values)
		buffer.WriteString(")")
	}
	buffer.WriteString(";\n")
	return buffer.String()
}
