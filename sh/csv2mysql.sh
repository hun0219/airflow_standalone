#!/bin/bash

CSV_PATH=$1
DEL_DT=$2

user="root"
# password="1234"
# database="history_db"

MYSQL_PWD='1234' mysql --local-infile=1 -u"$user" <<EOF
DELETE FROM history_db.tmp_cmd_usage WHERE dt='${DEL_DT}';

-- LOAD DATA LOCAL INFILE '/var/lib/mysql-files/csv.csv'
-- LOAD DATA LOCAL INFILE '/home/hun/data/csv/20240717/csv.csv'
LOAD DATA LOCAL INFILE '${CSV_PATH}'
INTO TABLE history_db.tmp_cmd_usage
-- CHARACTER SET latin1
FIELDS 
	TERMINATED BY ','
	ESCAPED BY '"'
	ESCAPED BY ''
-- ENCLOSED BY '^'
LINES TERMINATED BY '\n';
EOF
