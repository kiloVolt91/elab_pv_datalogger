#DOWNLOAD DEI DATI CONTENUTI ALL'INTERNO DEL DATALOGGER VIA FTP E UPLOAD DEGLI STESSI SU DB LOCALE TRAMITE AZIONE PROGRAMMATA OGNI 5 MINUTI

from datetime import datetime
import time
import os
from configurazione.funzioni_dataframe import *
import sys
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
import ftplib
from configurazione.init import *
from configurazione.dati import *
from apscheduler.schedulers.background import BackgroundScheduler


def cron_job():
    print('Orario: ', datetime.now())
    today = datetime.today().strftime('%y%m%d')
    download_ftp(today)
    df = get_data(today)
    cnx = mysql_connection(host, user, password, database)
    last_entry = check_last_db_entry(today, cnx)
    df = select_missing_data_db(df, last_entry)
    if df.empty==False:
        cnx = mysql_connection(host, user, password, database)
        sql_export_df(df, 'fv', cnx)
        print('Ultimo upload db: ', datetime.now())
    print('Fine Operazioni')
    return
    
def download_ftp(today):
    os.chdir(download_data_path)
    ftpc = ftplib.FTP()
    ftpc.connect(HOST, PORT)
    ftpc.login(USER, PASSWD)
    file_originale_inverter = 'int_gefran_' + today + '.txt'
    file_originale_stringhe = 'int_gefran_sb_' + today + '.txt'
    file_scaricato_inverter = os.path.join(today, 'download_int_' + today +'.txt')
    file_scaricato_stringhe = os.path.join(today, 'download_sb_'+  today +'.txt')
    if os.path.isdir(today): 
        print ('cartella esistente') 
    else:
            os.makedirs(today) 
            print ('cartella creata')   
    ftp_path = ftpc.pwd()
    ftpc.cwd('DATA')
    ftpc.cwd(today)
    ftpc.retrlines('LIST')
    ftpc.encoding='utf-8'
    files = [(file_originale_inverter, file_scaricato_inverter), (file_originale_stringhe, file_scaricato_stringhe)]
    for file in files:
        with open(file[1], "wb") as f:
            ftpc.retrbinary("RETR " + file[0], f.write)                    
    ftpc.cwd(ftp_path)
    os.chdir(download_data_path)
    ftpc.quit()
    print('Download FTP completato')
    return()

def get_data(today):
    os.chdir(origin_path+'/download_ftp')
    file_sb = 'download_sb_' + today + '.txt'
    file_int = 'download_int_' + today + '.txt'

#ESTRAZIONE DF STRING_BOX
    os.chdir(today)
    df = pd.read_csv(file_sb, sep=';', header=4, encoding_errors="replace")
    os.chdir('..')
    df.rename(columns={'Unnamed: 0':'0_Time'}, inplace=True) 
    df.drop(df.loc[df['Adresse']==-1].index, inplace=True)
    df.drop(df.loc[df['Adresse']==0.0].index, inplace=True) 
    df.drop(df.loc[df['0_Time']=='[Start]'].index, inplace=True)
    df.drop(df.loc[df['0_Time']=='Info'].index, inplace=True)
    df.rename(columns={'Irr':'0_Irr'}, inplace=True) 
    df.sort_values(by=['Adresse', '0_Time'], inplace=True) 
    for headers in colonne_vuote_string_box:
        df.drop(columns=[headers], inplace=True)
    for old_ind in indirizzi_da_rinominare:
        k = indirizzi_da_rinominare.index(old_ind)
        df['Adresse'].replace({indirizzi_da_rinominare[k]:nuovi_indirizzi[k]}, inplace=True)  
    nuove_colonne=[0,0,0,0,0,0,0,0,0,0,0,0] 
    for new_ind in nuovi_indirizzi:  
        k = nuovi_indirizzi.index(new_ind)
        nuove_colonne[k]=[('Adresse'), (new_ind+'_intervallo misura'), (new_ind+'_tres comp'), (new_ind+'_I_AC_av'), (new_ind+'_1'), (new_ind+'_2'), (new_ind+'_3'), (new_ind+'_4'), (new_ind+'_5'), (new_ind+'_6'), (new_ind+'_7'), (new_ind+'_TCARD')] #MODIFICATO 01_07_22 AGGIUNTO _TPAN
    df.dropna(inplace=True, axis =0)
    stringa_datetime = ' '+today[0:2]+'-'+today[2:4]+'-'+today[4:6]
    df['0_Time'] = df['0_Time'] + stringa_datetime
    df['0_Time'] = pd.to_datetime(df['0_Time'], format = '%H:%M:%S %y-%m-%d')
    df['time_index'] = df['0_Time']
    df.set_index(keys='time_index', inplace=True)
    #RE-DISLOCAZIONE DELLE INFORMAZIONI DI STRINGA SU APPOSITE COLONNE
    df_irraggiamento=df.loc[df['Adresse']==nuovi_indirizzi[6],['0_Time', '0_Irr']]
    df_string_box = df_irraggiamento.copy()
    for i in range(0,len(headers_da_rinominare)):  
        for k in range(0,len(headers_da_rinominare) ):
            df_iterativo = (df.loc[df['Adresse']==nuovi_indirizzi[i], [headers_da_rinominare[k]]])
            df_concatena = pd.DataFrame( {nuove_colonne[i][k]:df_iterativo.iloc[0:len(df_iterativo.index),0]})
            df_string_box= pd.concat([df_string_box, df_concatena], axis=1)
    df_string_box.drop(columns=['Adresse', '1.2_7', '2.2_7', '3.2_7', '4.2_7', '5.2_TCARD'], inplace=True)
    #ESTRAZIONE DF INVERTER
    os.chdir(today)
    df = pd.read_csv(file_int, sep=';', header=4, encoding_errors="replace")
    os.chdir('..')
    df.rename(columns={'Unnamed: 0':'Time'}, inplace=True)
    df.drop(df.loc[df['Time']=='[Start]'].index, inplace=True) 
    df.drop(df.loc[df['Time']=='Info'].index, inplace=True) 
    df.dropna(axis=0, inplace=True)
    df['Time'] = df['Time'] + stringa_datetime
    df['Time'] = pd.to_datetime(df['Time'], format = '%H:%M:%S %y-%m-%d')
    df['time_index'] = df['Time']
    df.set_index(keys='time_index', inplace=True)
    df.drop(df.loc[df['Adresse']!=1].index, inplace=True)
    for headers in colonne_vuote_inverter:
        df.drop(columns=[headers], inplace=True)
    for key in df.keys():
        df.rename(columns={key:'0_'+key}, inplace=True)
    df_inverter = df.copy()    
    #MERGE DF GIORNALIERO
    df_day = pd.merge(df_inverter, df_string_box, on = '0_Time') 
    df_day.fillna(value=0, inplace=True)
    print('DataFrame creato')
    return(df_day)

def mysql_connection(host, user, password, database):
    try:
        cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
    except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
      else:
        print(err)
    return(cnx)

def check_last_db_entry(today, sql_cnx):
    mycursor = sql_cnx.cursor()
    today = str(today)[0:19]
    query="SELECT 0_Time FROM fv WHERE EXISTS(SELECT * FROM fv WHERE 0_Time <= '"+str(today)+"') ORDER BY 0_Time DESC LIMIT 1"
    mycursor.execute(query)
    last_entry=mycursor.fetchall()
    print('ultimo valore su db: ', (last_entry[0])[0])
    sql_cnx.close()
    return(last_entry)

def select_missing_data_db(df, last_entry):
    df = df.loc[df['0_Time']>(last_entry[0])[0]]
    return(df)

def sql_export_df(df, sql_tabella, sql_cnx): 
    columns = df.columns.tolist()
    placeholders = '%s'
    str_nomi = '('+columns[0]+','
    str_vals = '(%s,'
    for i in range(1, len(columns)):
        if i == len(columns)-1:
            str_nomi = str_nomi +'`'+ columns[i] +'`' +')'
            str_vals = str_vals + placeholders + ')'
        else: 
            str_nomi = str_nomi +'`'+ columns[i] +'`'+', '
            str_vals = str_vals + placeholders + ', '
    mysql_str = "INSERT INTO "+ sql_tabella+ " {col_name} VALUES {values}".format(col_name = str_nomi, values = str_vals)
    cursor = sql_cnx.cursor()

    for i in range (0, df.shape[0]):
        print('db uploading..')
        if i%100==True:
            print('.')
        cursor.execute (mysql_str, df.iloc[i].tolist())
    sql_cnx.commit()
    cursor.close()
    sql_cnx.close()
    return

if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(cron_job, 'interval', minutes=5)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
