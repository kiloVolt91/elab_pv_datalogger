### DATALOGGER INVERTER + STRING_BOX GEFRAN. DOWNLOAD DEI DATI VIA FTP E UPLOAD DEL DATABASE
from dati import *
from init import * 
from datetime import datetime, date, timedelta
import time
import os
import sys
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
import ftplib

def datalogger():
    print('Orario esecuzione: ', datetime.now())
    global id_impianto
    id_impianto = sys.argv[1] ## da associare alla foreign_key del database #1
    mysql_connection(sql_host, sql_user, sql_password, sql_database)
    ultimo_valore_db = check_last_db_entry()
    aggiornamento_directory_dataset(ultimo_valore_db)
    global df
    if ultimo_valore_db:
        df = get_data(today_str)
        df = select_missing_data_db(df, ultimo_valore_db)
    else:
        df = pd.DataFrame()
        directory = os.listdir(download_data_path)
        directory.sort()
        for data in directory:
            df_daily = get_data(data)
            df = pd.concat([df, df_daily])
    if not df.empty:
        mysql_connection(sql_host, sql_user, sql_password, sql_database)
        sql_export_df(df, db_table)
        print('Ultimo upload db: ', datetime.now())
    print('Fine Operazioni')
    return

def mysql_connection(host, user, password, database):
    global sql_cnx
    try:
        sql_cnx = mysql.connector.connect(host=host, user=user, password=password, database=database)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Username o Password errate")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Il database non esiste")
        else:
            print(err)
    return

def ftp_connection(host, port, user, password):
    global ftpc_cnx
    try:
            ftpc_cnx = ftplib.FTP()
            ftpc_cnx.connect(host, port)
            ftpc_cnx.login(user, password)
            ftpc.set_pasv(False) # Utilizza la modalità ATTIVA
    except Exception as err:
            print(str(err))
    return

def check_last_db_entry():
    global today 
    global today_str 
    today = datetime.today()
    today_str = str(today.strftime('%y%m%d'))[0:19]
    sql_cursor = sql_cnx.cursor()
    query = "SELECT "+str(db_colonna_temporale_inverter)+" FROM "+str(db_table)+" WHERE EXISTS(SELECT * FROM "+str(db_table)+" WHERE 0_Time <= '"+str(today_str)+"') ORDER BY "+str(db_colonna_temporale_inverter)+" DESC LIMIT 1"
    sql_cursor.execute(query)
    last_entry = sql_cursor.fetchall()
    if last_entry:
        print('ultimo valore su db: ', (last_entry[0])[0])
    else:
        print('db vuoto')
    sql_cnx.close()
    return(last_entry)

def buffer_nuovi_dati(last_entry):
    lista_giorni_mancanti_su_db = lista_giorni_mancanti(last_entry)
    ftp_connection(ftp_host, ftp_port, ftp_user, ftp_password)
    for giorno in lista_giorni_mancanti_su_db:
        download_ftp(giorno)
    return

def aggiornamento_directory_dataset(last_entry):
    if last_entry: 
        buffer_nuovi_dati(last_entry)
    else: 
        if not os.listdir(download_data_path):
            print('Directory vuota - download dei file FTP')
            ftp_connection(ftp_host, ftp_port, ftp_user, ftp_password)
            ftpc_cnx.cwd('DATA')
            ftpc_cnx.retrlines('LIST', lista_file_ftp.append)
            ftpc_cnx.quit()
            lista_file_ftp=[]
            for file in lista_file_ftp:
                if file[-6::].isnumeric():
                    giorno = str(file[-6::])
                    download_ftp(giorno)
    return

def lista_giorni_mancanti(last_entry):
    data_inizio = last_entry[0][0].date()  
    data_fine = today.date()    
    delta = data_fine - data_inizio  
    lista_giorni_mancanti=[]
    for i in range(delta.days + 1):
        day = datetime.strftime(
            data_inizio + timedelta(days=i), 
            '%y%m%d')
        lista_giorni_mancanti.append(day)
    return lista_giorni_mancanti

def select_missing_data_db(df, last_entry):
    df = df.loc[df['0_Time']>(last_entry[0])[0]]
    return(df)

def sql_export_df(df, sql_tabella): 
    print('esportazione dati verso db')
    columns = df.columns.tolist()
    columns.append('fk_id_impianto')
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
        if i%1000==True:
            print('.')
        data =  df.iloc[i].tolist()
        data.append(id_impianto)
        cursor.execute (mysql_str, data)
    sql_cnx.commit()
    cursor.close()
    sql_cnx.close()
    print('upload db completato')
    return

def download_ftp(giorno):
    print('Download FTP', giorno)
    os.chdir(download_data_path)
    ftp_connection(ftp_host, ftp_port, ftp_user, ftp_password)
    file_originale_inverter = 'int_gefran_' + str(giorno) + '.txt'
    file_originale_stringhe = 'int_gefran_sb_' + str(giorno) + '.txt'
    file_scaricato_inverter = os.path.join(giorno, 'download_int_' + str(giorno) +'.txt')
    file_scaricato_stringhe = os.path.join(giorno, 'download_sb_'+  str(giorno) +'.txt')
    if os.path.isdir(giorno): 
        print ('cartella esistente') 
    else:
            os.makedirs(giorno) 
            print ('cartella creata')   
    ftp_path = ftpc_cnx.pwd()
    ftpc_cnx.cwd('DATA')
    ftpc_cnx.cwd(giorno)
    ftpc_cnx.retrlines('LIST')
    ftpc_cnx.encoding='utf-8'
    files = [(file_originale_inverter, file_scaricato_inverter), (file_originale_stringhe, file_scaricato_stringhe)]
    for file in files:
        with open(file[1], "wb") as f:
            ftpc_cnx.retrbinary("RETR " + file[0], f.write)                    
    ftpc_cnx.cwd(ftp_path)
    os.chdir(download_data_path)
    ftpc_cnx.quit()
    print('Download FTP completato')
    return()

def get_data(giorno):
    os.chdir(download_data_path)
    file_sb = 'download_sb_' + str(giorno) + '.txt'
    file_int = 'download_int_' + str(giorno) + '.txt'

    #ESTRAZIONE E MANIPOLAZIONE DF STRING_BOX
    os.chdir(giorno)
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
    nuove_colonne = [0]*len(headers_da_rinominare)
    for new_ind in nuovi_indirizzi:
        lista_colonne =[]
        k = nuovi_indirizzi.index(new_ind)
        if len(suffissi_nuove_colonne) != len(headers_da_rinominare):
            print('Verificare le colonne da rinominare in /configurazione/dati.py') 
        for suffisso in suffissi_nuove_colonne:
            if suffissi_nuove_colonne.index(suffisso) == 0:
                lista_colonne.append(str(suffisso))
            else:
                lista_colonne.append(str(new_ind)+str(suffisso))
        nuove_colonne[k] = lista_colonne
    df.dropna(inplace=True, axis =0)
    stringa_datetime = ' '+giorno[0:2]+'-'+giorno[2:4]+'-'+giorno[4:6]
    df['0_Time'] = df['0_Time'] + stringa_datetime
    df['0_Time'] = pd.to_datetime(df['0_Time'], format = '%H:%M:%S %y-%m-%d')
    df['time_index'] = df['0_Time']
    df.set_index(keys='time_index', inplace=True)
    #RE-DISLOCAZIONE DELLE INFORMAZIONI DI STRINGA SU APPOSITE COLONNE
    df_irraggiamento=df.loc[df['Adresse']==nuovi_indirizzi[6],['0_Time', '0_Irr']]
    df_string_box = df_irraggiamento.copy()
    for i in range(0,len(nuovi_indirizzi)):  
        for k in range(0,len(headers_da_rinominare) ):
            df_iterativo = (df.loc[df['Adresse']==nuovi_indirizzi[i], [headers_da_rinominare[k]]])
            df_concatena = pd.DataFrame( {nuove_colonne[i][k]:df_iterativo.iloc[0:len(df_iterativo.index),0]})
            df_string_box= pd.concat([df_string_box, df_concatena], axis=1)
    df_string_box.drop(columns=['Adresse', '1.2_7', '2.2_7', '3.2_7', '4.2_7', '5.2_TCARD'], inplace=True)
    
    #ESTRAZIONE E MANIPOLAZIONE DF INVERTER
    os.chdir(giorno)
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
    return(df_day)



while True:
    try:
        datalogger()
    except Exception as error:
        sys.exit(str(error))
    break
