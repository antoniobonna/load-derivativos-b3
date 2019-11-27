# -*- coding: utf-8 -*-
"""
Created on Wed Nov 27 16:22:45 2019

@author: abonna
"""

import os
import csv
from zipfile import ZipFile
from datetime import datetime
import credentials
import psycopg2
from subprocess import call

### Definicao das variaveis
indir = '/home/ubuntu/scripts/load-dados-bmf/'
outdir = '/home/ubuntu/scripts/load-dados-bmf/csv/'
file = 'RESULT.ZIP'
csvfile = 'RESULT.txt '
new_file = 'result.csv'
tablename = 'bmf.result_stg'

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

zip = ZipFile(file)
zip.extractall(indir)
os.remove(file)

with open(indir+csvfile, 'r', encoding='utf16') as ifile:
    reader = csv.reader(ifile, delimiter=';')
    header = next(reader, None)  ### Pula o cabeçalho
    with open(outdir+new_file,'w', newline="\n", encoding='utf8') as ofile:
        writer = csv.writer(ofile, delimiter=';')
        for row in reader:
            if row[1] == 'MERC' and (int(row[4]) != 0 or int(row[5]) != 0):
                row[0] = str(datetime.strptime(row[0], '%m/%d/%Y'))[:10]
                row[3] = row[3].strip()
                row[6] = str(datetime.strptime(row[6], '%m/%d/%Y'))[:10]
                writer.writerow(row[:-1])
os.remove(indir+csvfile)

### conecta no banco de dados
db_conn = psycopg2.connect("dbname='{}' user='{}' host='{}' password='{}'".format(DATABASE, USER, HOST, PASSWORD))
cursor = db_conn.cursor()
print('Connected to the database')
### copy
with open(outdir+new_file, 'r') as ifile:
    SQL_STATEMENT = "COPY %s FROM STDIN WITH CSV DELIMITER AS ';' NULL AS ''"
    print("Executing Copy in "+tablename)
    cursor.copy_expert(sql=SQL_STATEMENT % tablename, file=ifile)
    db_conn.commit()
cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM VERBOSE ANALYZE '+tablename+'";',shell=True)
os.remove(indir+new_file)