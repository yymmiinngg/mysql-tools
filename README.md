# mysql-tools

## sql-trans 命令（MYSQL导入提速）

能将mysqldump导出的.sql文件进行优化，生成便于快速导入数据库的.sql文件

### 主要特点

- 读取 CREATE TABLE 语句中的主键、唯一键和索引修饰部分，生成 .drop-index 和 .add-index 文件。
    - .drop-index 删除索引
    - .add-index 增加索引

- 合并同一段落下同一表格中的 INSERT INTO 语句，将逐条插入转换成批量插入。

