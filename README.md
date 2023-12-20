# mysql-tools

## sql-trans 命令（MYSQL导入提速）

能将mysqldump导出的.sql文件进行优化，生成便于快速导入数据库的.sql文件

### 主要特点

- 读取 CREATE TABLE 语句中的主键、唯一键和、外键定义和插入语句部分，生成按顺序执行的加速文件。
    - .struct 数据库结构
    - .insert 插入语句
    - .drop-index 删除索引
    - .add-index 增加索引
    - .drop-fk 删除外键
    - .add-fk 增加外键

- 合并同一段落下同一表格中的 INSERT INTO 语句，将逐条插入转换成批量插入。

### 执行顺序

    1. struct
    2. drop-fk
    3. drop-index
    4. insert
    5. add-index
    6. add-fk