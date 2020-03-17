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
columns = ['Data do pregão','Referência do Resumo','Mercado','Mercadoria','Contratos negociados','Contratos em aberto (Final)','Data do vencimento','Dias de saques até vencimento','Cotação do ajuste - dia atual']

DATABASE, HOST, USER, PASSWORD = credentials.setDatabaseLogin()

zip = ZipFile(file)
zip.extractall(indir)
os.remove(file)

with open(indir+csvfile, 'r', encoding='utf16') as ifile:
    reader = csv.DictReader(ifile, delimiter=';')
    #header = next(reader, None)  ### Pula o cabeçalho
    with open(outdir+new_file,'w', newline="\n", encoding='utf8') as ofile:
        writer = csv.DictWriter(ofile, fieldnames=columns, extrasaction='ignore',delimiter=';')
        for row in reader:
            #print(row)
            if row['Referência do Resumo'] == 'MERC' and (int(row['Contratos negociados']) != 0 or int(row['Contratos em aberto (Final)']) != 0):
                row['Data do pregão'] = str(datetime.strptime(row['Data do pregão'], '%m/%d/%Y'))[:10]
                row['Mercadoria'] = row['Mercadoria'].strip()
                row['Data do vencimento'] = str(datetime.strptime(row['Data do vencimento'], '%m/%d/%Y'))[:10]
                writer.writerow(row)
            # if row[1] == 'MERC' and (int(row[4]) != 0 or int(row[5]) != 0):
                # row[0] = str(datetime.strptime(row[0], '%m/%d/%Y'))[:10]
                # row[3] = row[3].strip()
                # row[6] = str(datetime.strptime(row[6], '%m/%d/%Y'))[:10]
                # writer.writerow(row[:-1])
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

SQL_INSERT = '''INSERT INTO bmf.result_hist
TABLE bmf.result_stg
EXCEPT table bmf.result_hist'''

cursor.execute(SQL_INSERT)
db_conn.commit()

SQL_TRUNCATE = 'TRUNCATE table bmf.result_stg'
cursor.execute(SQL_TRUNCATE)
db_conn.commit()

cursor.close()
db_conn.close()

### VACUUM ANALYZE
call('psql -d torkcapital -c "VACUUM ANALYZE bmf.result_hist";',shell=True)
os.remove(outdir+new_file)