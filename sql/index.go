package sql

import (
	"bytes"
	"fmt"
	"strings"
)

type IndexHelperBean struct {
	InputFile          string
	currentCreateTable *CreateTableBean
	CreateTalbeList    []CreateTableBean
}

type CreateTableBean struct {
	START       string // CREATE TABLE `table_name` (
	TABLE_NAME  string // `table_name`
	END         string // ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
	PRIMARY_KEY *PrimaryKeyBean
	KEY         []KeyBean
	UNIQUE_KEY  []UniqueKeyBean
}

type PrimaryKeyBean struct {
	LINE  string // PRIMARY KEY (`openid`),
	FIELD string // `openid`
}

type UniqueKeyBean struct {
	LINE  string // UNIQUE KEY `key_name` (`field1`, `field2`, ...)
	NAME  string // `key_name`
	FIELD string // `field1`, `field2`, ...
}

type KeyBean struct {
	LINE  string // KEY `key_name` (`field`)
	NAME  string // `key_name`
	FIELD string // `field`
}

func NewIndexHelper(inputFile string) *IndexHelperBean {
	return &IndexHelperBean{
		InputFile:       inputFile,
		CreateTalbeList: make([]CreateTableBean, 0, 10),
	}
}

func (this *IndexHelperBean) InputLine(line string) error {
	if strings.Index(line, "CREATE TABLE") == 0 {
		if this.currentCreateTable != nil && this.currentCreateTable.END == "" {
			return fmt.Errorf("%s has not closed", this.currentCreateTable.START)
		}
		this.currentCreateTable = &CreateTableBean{
			START:      line,
			TABLE_NAME: strings.TrimSpace(GetSection(line, "CREATE TABLE", "(")),
		}
	}
	if strings.Index(line, "PRIMARY KEY") == 0 {
		this.currentCreateTable.PRIMARY_KEY = &PrimaryKeyBean{
			LINE:  line,
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		}
	}
	if strings.Index(line, "KEY") == 0 {
		this.currentCreateTable.KEY = append(this.currentCreateTable.KEY, KeyBean{
			LINE:  line,
			NAME:  strings.TrimSpace(GetSection(line, "KEY", "(")),
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		})
	}
	if strings.Index(line, "UNIQUE KEY") == 0 {
		this.currentCreateTable.UNIQUE_KEY = append(this.currentCreateTable.UNIQUE_KEY, UniqueKeyBean{
			LINE:  line,
			NAME:  strings.TrimSpace(GetSection(line, "UNIQUE KEY", "(")),
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		})
	}
	if strings.Index(line, ")") == 0 {
		if this.currentCreateTable != nil && this.currentCreateTable.END != "" {
			return fmt.Errorf("%s closed double time", this.currentCreateTable.START)
		}
		this.currentCreateTable.END = line
		this.CreateTalbeList = append(this.CreateTalbeList, *this.currentCreateTable)
		this.currentCreateTable = nil
	}
	return nil
}

func (this *CreateTableBean) MakeDropSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Drop Table: " + this.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	if this.PRIMARY_KEY != nil {
		dropPrimaryKey := fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", this.TABLE_NAME)
		buffer.WriteString(dropPrimaryKey + "\n")
	}
	for _, key := range this.KEY {
		dropKey := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", this.TABLE_NAME, key.NAME)
		buffer.WriteString(dropKey + "\n")
	}
	for _, key := range this.UNIQUE_KEY {
		dropKey := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", this.TABLE_NAME, key.NAME)
		buffer.WriteString(dropKey + "\n")
	}
	return buffer.String()
}

func (this *CreateTableBean) MakeAddSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Create Table: " + this.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	if this.PRIMARY_KEY != nil {
		addPrimaryKey := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);", this.TABLE_NAME, strings.Trim(this.PRIMARY_KEY.FIELD, ","))
		buffer.WriteString(addPrimaryKey + "\n")
	}
	for _, key := range this.KEY {
		addKey := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s);", this.TABLE_NAME, key.NAME, strings.Trim(key.FIELD, ","))
		buffer.WriteString(addKey + "\n")
	}
	for _, key := range this.UNIQUE_KEY {
		addKey := fmt.Sprintf("ALTER TABLE %s ADD UNIQUE INDEX %s (%s);", this.TABLE_NAME, key.NAME, strings.Trim(key.FIELD, ","))
		buffer.WriteString(addKey + "\n")
	}
	return buffer.String()
}

func GetSection(s string, left, right string) string {

	defer func() { // 必须要先声明defer，否则不能捕获到panic异常
		if err := recover(); err != nil {
			fmt.Println("ERROR:")
			fmt.Println("    ", strings.TrimSpace(s))
			fmt.Println("    ", left)
			fmt.Println("    ", right)
			panic(err) // 这里的err其实就是panic传入的内容，55
		}
	}()

	li := strings.Index(s, left)
	if li < 0 {
		return ""
	}
	ss := string([]byte(s)[li+len([]byte(left)):])
	ri := strings.Index(ss, right)
	if ri < 0 {
		return ""
	}
	ri += li + len([]byte(left))
	return string(
		[]byte(s)[li+len([]byte(left)) : ri],
	)
}

func GetLeft(s string, right string) string {
	ri := strings.Index(s, right)
	if ri < 0 {
		return ""
	}
	return string([]byte(s)[:ri])
}

func GetRight(s string, left string) string {
	li := strings.Index(s, left)
	if li < 0 {
		return ""
	}
	return string([]byte(s)[li+len([]byte(left)):])
}
