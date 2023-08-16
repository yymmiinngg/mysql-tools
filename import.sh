#!/bin/bash
USER="root"
PASSWORD="123456"
DATABASE="$1"

OUTPUT_PATH=./$DATABASE"-out"

# /home/mysqltools sql-trans -i $INPUT_PATH -o $OUTPUT_PATH

TAG="MAIN"

function mysql_source(){
    db=$1
    sqlFile=$2
    status=0
    echo "[$(date +%Y-%m-%d_%H:%M:%S)] [$TAG] import sql file: $sqlFile" >&1
    if [ "$db" == "NULL" ]; then
        mysql -u$USER -p$PASSWORD < "$sqlFile" >&1
        status=$?
    else
        mysql -u$USER -p$PASSWORD -e "USE $db; SET FOREIGN_KEY_CHECKS=0; SOURCE $sqlFile;" $db >&1
        status=$?
    fi
    if [ $status -ne 0 ]; then
        exit $status;
    fi
}

TAG="CREATE DB"
mysql_source "NULL" "$OUTPUT_PATH/structure.sql"

TAG="CREATE TABLE"
find $OUTPUT_PATH -mindepth 1 -maxdepth 1 -type d | while read -r file; do
    mysql_source $DATABASE  "$file/structure.sql"
done

TAG="DROP FOREIGN KEY"
find $OUTPUT_PATH -name "structure.sql.drop-fk.sql" | while read -r file; do
    mysql_source $DATABASE "$file"
done

TAG="DROP INDEX"
find $OUTPUT_PATH -name "structure.sql.drop-index.sql" | while read -r file; do
    mysql_source $DATABASE "$file"
done

TAG="INSERT DATA"
find $OUTPUT_PATH -maxdepth 3 -mindepth 3 -type f -name "*.sql" | while read -r file; do
    mysql_source $DATABASE "$file"
done

TAG="ADD INDEX"
find $OUTPUT_PATH -name "structure.sql.add-index.sql" | while read -r file; do
    mysql_source $DATABASE "$file"
done

TAG="ADD FOREIGN KEY"
find $OUTPUT_PATH -name "structure.sql.add-fk.sql" | while read -r file; do
    mysql_source $DATABASE "$file"
done

echo "import $DATABASE done!!!"