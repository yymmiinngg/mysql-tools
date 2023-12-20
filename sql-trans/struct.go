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
	START          string // CREATE TABLE `table_name` (
	TABLE_NAME     string // `table_name`
	END            string // ) ENGINE=InnoDB DEFAULT CHARSET=utf8 AUTO_INCREMENT=1;
	AUTO_INCREMENT string // AUTO_INCREMENT 对应的字段定义
	PRIMARY_KEY    *PrimaryKeyBean
	KEY            []KeyBean
	UNIQUE_KEY     []UniqueKeyBean
	FOREIGN_KEY    []ForeignKeyBean
}

type PrimaryKeyBean struct {
	LINE  string // PRIMARY KEY (`openid`),
	FIELD string // `openid`
}

type KeyBean struct {
	LINE  string // KEY `key_name` (`field`)
	NAME  string // `key_name`
	FIELD string // `field`
}

type UniqueKeyBean struct {
	LINE  string // UNIQUE KEY `key_name` (`field1`, `field2`, ...)
	NAME  string // `key_name`
	FIELD string // `field1`, `field2`, ...
}

type ForeignKeyBean struct {
	LINE        string // CONSTRAINT `foreign_key_instance_id` FOREIGN KEY (`process_instance_id`) REFERENCES `t_ds_process_instance` (`id`) ON DELETE CASCADE
	NAME        string // `foreign_key_instance_id`
	FOREIGN_KEY string // `process_instance_id`
	REFERENCES  string // `t_ds_process_instance` (`id`)
	ON          string // ON DELETE CASCADE
}

func NewIndexHelper(inputFile string) *IndexHelperBean {
	return &IndexHelperBean{
		InputFile:       inputFile,
		CreateTalbeList: make([]CreateTableBean, 0, 10),
	}
}

func (it *IndexHelperBean) InputLine(linesource string) (string, error) {
	line := strings.TrimSpace(linesource)
	// CREATE TABLE `name` (
	if strings.Index(line, "CREATE TABLE") == 0 {
		if it.currentCreateTable != nil && it.currentCreateTable.END == "" {
			return "", fmt.Errorf("%s has not closed", it.currentCreateTable.START)
		}
		it.currentCreateTable = &CreateTableBean{
			START:      line,
			TABLE_NAME: strings.TrimSpace(GetSection(line, "CREATE TABLE", "(")),
		}
	}
	// `id` int(11) NOT NULL AUTO_INCREMENT,
	if !strings.Contains(line, "AUTO_INCREMENT=") && strings.Index(line, "AUTO_INCREMENT") > 0 { // 字段中的声明
		autoIncrementFieldDDL := strings.TrimSpace(strings.ReplaceAll(linesource, " AUTO_INCREMENT", ""))
		if strings.Index(autoIncrementFieldDDL, ",") == len(autoIncrementFieldDDL)-1 { // 清除尾部逗号
			autoIncrementFieldDDL = autoIncrementFieldDDL[:len(autoIncrementFieldDDL)-1]
		}
		it.currentCreateTable.AUTO_INCREMENT = autoIncrementFieldDDL // `id` int(11) NOT NULL,
	}
	// PRIMARY KEY (`id`),
	if strings.Index(line, "PRIMARY KEY") == 0 {
		it.currentCreateTable.PRIMARY_KEY = &PrimaryKeyBean{
			LINE:  line,
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		}
	}
	// KEY `process_instance_id` (`process_instance_id`) USING BTREE,
	if strings.Index(line, "KEY") == 0 {
		it.currentCreateTable.KEY = append(it.currentCreateTable.KEY, KeyBean{
			LINE:  line,
			NAME:  strings.TrimSpace(GetSection(line, "KEY", "(")),
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		})
	}
	// UNIQUE KEY `process_instance_id` (`process_instance_id`) USING BTREE,
	if strings.Index(line, "UNIQUE KEY") == 0 {
		it.currentCreateTable.UNIQUE_KEY = append(it.currentCreateTable.UNIQUE_KEY, UniqueKeyBean{
			LINE:  line,
			NAME:  strings.TrimSpace(GetSection(line, "UNIQUE KEY", "(")),
			FIELD: strings.TrimSpace(GetSection(line, "(", ")")),
		})
	}
	// CONSTRAINT `foreign_key_instance_id` FOREIGN KEY (`process_instance_id`) REFERENCES `t_ds_process_instance` (`id`) ON DELETE CASCADE
	if strings.Index(line, "CONSTRAINT") == 0 {
		it.currentCreateTable.FOREIGN_KEY = append(it.currentCreateTable.FOREIGN_KEY, ForeignKeyBean{
			LINE:        line,
			NAME:        strings.TrimSpace(GetSection(line, "CONSTRAINT ", " ")),
			FOREIGN_KEY: strings.TrimSpace(GetSection(line, "FOREIGN KEY (", ")")),
			REFERENCES:  strings.TrimSpace(GetSection(line, "REFERENCES ", ")") + ")"),
			ON:          strings.TrimSpace(GetRight(line, "ON ")),
		})
	}
	// )
	if strings.Index(line, ")") == 0 {
		if it.currentCreateTable != nil && it.currentCreateTable.END != "" {
			return "", fmt.Errorf("%s closed double time", it.currentCreateTable.START)
		}
		it.currentCreateTable.END = line
		it.CreateTalbeList = append(it.CreateTalbeList, *it.currentCreateTable)
		it.currentCreateTable = nil
	}
	return linesource, nil
}

func (it *CreateTableBean) MakeDropIndexSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Drop KEY: " + it.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	if it.AUTO_INCREMENT != "" { // 移除自增长约索
		autoIncrement := fmt.Sprintf("ALTER TABLE %s MODIFY %s;", it.TABLE_NAME, it.AUTO_INCREMENT)
		buffer.WriteString(autoIncrement + "\n")
	}
	if it.PRIMARY_KEY != nil {
		dropPrimaryKey := fmt.Sprintf("ALTER TABLE %s DROP PRIMARY KEY;", it.TABLE_NAME)
		buffer.WriteString(dropPrimaryKey + "\n")
	}
	for _, key := range it.KEY {
		dropKey := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", it.TABLE_NAME, key.NAME)
		buffer.WriteString(dropKey + "\n")
	}
	for _, key := range it.UNIQUE_KEY {
		dropKey := fmt.Sprintf("ALTER TABLE %s DROP INDEX %s;", it.TABLE_NAME, key.NAME)
		buffer.WriteString(dropKey + "\n")
	}
	return buffer.String()
}

func (it *CreateTableBean) MakeDropForeignKeySQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Drop FOREIGN KEY: " + it.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	for _, key := range it.FOREIGN_KEY {
		dropKey := fmt.Sprintf("ALTER TABLE %s DROP FOREIGN KEY %s;", it.TABLE_NAME, key.NAME)
		buffer.WriteString(dropKey + "\n")
	}
	return buffer.String()
}

func (it *CreateTableBean) MakeAddIndexSQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Add KEY: " + it.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	if it.PRIMARY_KEY != nil {
		addPrimaryKey := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s);", it.TABLE_NAME, strings.Trim(it.PRIMARY_KEY.FIELD, ","))
		buffer.WriteString(addPrimaryKey + "\n")
	}
	for _, key := range it.KEY {
		addKey := fmt.Sprintf("ALTER TABLE %s ADD INDEX %s (%s);", it.TABLE_NAME, key.NAME, strings.Trim(key.FIELD, ","))
		buffer.WriteString(addKey + "\n")
	}
	for _, key := range it.UNIQUE_KEY {
		addKey := fmt.Sprintf("ALTER TABLE %s ADD UNIQUE INDEX %s (%s);", it.TABLE_NAME, key.NAME, strings.Trim(key.FIELD, ","))
		buffer.WriteString(addKey + "\n")
	}
	if it.AUTO_INCREMENT != "" { // 增加自增长约索
		autoIncrement := fmt.Sprintf("ALTER TABLE %s MODIFY %s AUTO_INCREMENT;", it.TABLE_NAME, it.AUTO_INCREMENT)
		buffer.WriteString(autoIncrement + "\n")
	}
	return buffer.String()
}

func (it *CreateTableBean) MakeAddForeignKeySQL() string {
	var buffer bytes.Buffer
	buffer.WriteString("\n")
	buffer.WriteString("--\n")
	buffer.WriteString("-- Add FOREIGN KEY: " + it.TABLE_NAME + "\n")
	buffer.WriteString("--\n")
	for _, key := range it.FOREIGN_KEY {
		on := ""
		if key.ON != "" {
			on = " ON " + key.ON
		}
		addKey := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s%s;", it.TABLE_NAME, key.NAME, key.FOREIGN_KEY, key.REFERENCES, on)
		buffer.WriteString(addKey + "\n")
	}
	return buffer.String()
}

func GetSection(s string, left, right string) string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("ERROR:")
			fmt.Println("    ", strings.TrimSpace(s))
			fmt.Println("    ", left)
			fmt.Println("    ", right)
			panic(err)
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

func GetSectionOutter(s string, left, right string) string {
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
	ri := strings.LastIndex(ss, right)
	if ri < 0 {
		return ""
	}
	ri += li + len([]byte(left))
	return string(
		[]byte(s)[li+len([]byte(left)) : ri],
	)
}

func GetLeft(s string, right string) string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("ERROR:")
			fmt.Println("    ", strings.TrimSpace(s))
			fmt.Println("    ", right)
			panic(err)
		}
	}()
	ri := strings.Index(s, right)
	if ri < 0 {
		return ""
	}
	return string([]byte(s)[:ri])
}

func GetRight(s string, left string) string {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("ERROR:")
			fmt.Println("    ", strings.TrimSpace(s))
			fmt.Println("    ", left)
			panic(err)
		}
	}()
	li := strings.Index(s, left)
	if li < 0 {
		return ""
	}
	return string([]byte(s)[li+len([]byte(left)):])
}
