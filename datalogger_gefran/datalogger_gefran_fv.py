from configurazione.init import * 
from datetime import datetime, date, timedelta
import os
import sys
import pandas as pd
import numpy as np
from pytz import timezone
import sqlalchemy
import ftplib

#####################################
## BLOCCO FUNZIONI ACCESSORIE DI BASE
#####################################

class mysql_client:
    def __init__(self, user, password, host, db):
        url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
        self.engine = sqlalchemy.create_engine(url)
        
    def db_fetch(self, query):
        with self.engine.connect() as connection:
            df = pd.read_sql(query, connection)
            return(df)
        
    def db_alter_table(self, table, df):
        with self.engine.connect() as connection:
            df.to_sql(table, connection, index = False, if_exists= 'append')

def ftp_connection(host, port, user, password):
    try:
            ftp_engine = ftplib.FTP(encoding='utf-8')
            ftp_engine.connect(host, port)
            ftp_engine.login(user, password)
            ftp_engine.set_pasv(False) # Utilizza la modalità ATTIVA)
    except Exception as err:
            print(str(err))
    return (ftp_engine)


##############################
## BLOCCO FUNZIONI ACCESSORIE
##############################

def download_ftp(day):
## SELEZIONE DELLE DIRECTORY LOCALI
    os.chdir(download_data_path) 
    if os.path.isdir(day): 
        print ('Cartella Esistente') 
    else:
            os.makedirs(day) 
            print ('Nuova cartella creata: ', day)  
            
## IDENTIFICAZIONE DEI FILE
    ftpserver_int_filename = 'int_gefran_' + str(day) + '.txt'
    ftpserver_sb_filename = 'int_gefran_sb_' + str(day) + '.txt'
    local_int_filename = os.path.join(day, 'download_int_' + str(day) +'.txt')
    local_sb_filename = os.path.join(day, 'download_sb_'+  str(day) +'.txt')
 
    ftp_engine = ftp_connection(ftp_host, ftp_port, ftp_user, ftp_password)
    with ftp_engine:
        ftp_engine.cwd('DATA')
        ftp_engine.cwd(day)
        ftp_engine.retrlines('LIST')
        files = [(ftpserver_int_filename, local_int_filename), (ftpserver_sb_filename, local_sb_filename)]
        for file in files:
            with open(file[1], "wb") as f:
                ftp_engine.retrbinary("RETR " + file[0], f.write)
        ftp_engine.quit()

    return

def manipulate_dataframe(df, day):
    ## CORREZIONE COLONNE E VALORI INUTILIZZATI
    for column in lista_colonne_da_eliminare:
        try:
            df.drop(columns=[column], inplace=True)
            df[column]=df[column].astype(float)
        except:
            pass
    df.dropna(inplace=True)
    df['Adresse']=df['Adresse'].astype(int)
    
    ## CORREZIONE NOMI
    df.rename(columns={'Unnamed: 0':'local_time'}, inplace=True)
    df.rename(columns={'Adresse':'fk_address'}, inplace=True)
    
    ## CORREZIONE INDICE
    df.sort_values(by=['fk_address', 'local_time'], inplace=True) 
    df.reset_index(inplace=True)
    df.drop(columns=["index"], inplace=True)
    
    ## CORREZIONE TEMPORALE
    stringa_datetime = ' '+day[0:2]+'-'+day[2:4]+'-'+day[4:6]
    df['local_time'] = df['local_time'] + stringa_datetime
    df['local_time'] = pd.to_datetime(df['local_time'], format = '%H:%M:%S %y-%m-%d')
    df['local_time'] = pd.to_datetime(df['local_time']).dt.tz_localize('Europe/Rome')
    df["time_utc"] = pd.to_datetime(df['local_time']).dt.tz_convert('UTC')
    df["timestamp_utc"] = pd.to_datetime(df['time_utc']).values.astype(np.int64) // 10 ** 9
    return(df)

def get_data_as_dataframe(day):
    file_sb = os.path.join(download_data_path, day,'download_sb_' + str(day) + '.txt')
    file_int = os.path.join(download_data_path, day,'download_int_' + str(day) + '.txt')
    df_sb = pd.read_csv(file_sb, sep=';', header=4, encoding_errors="replace")
    df_int = pd.read_csv(file_int, sep=';', header=4, encoding_errors="replace")
    df_sb = manipulate_dataframe(df_sb, day)
    df_int = manipulate_dataframe(df_int, day)

    
    #MANIPOLAZIONE SPECIFICA
    ## DISLOCAZIONE DELLA COLONNA IRRAGGIAMENTO NEL DF_INVERTER DAL DF_STRINGHE
    df_irr=df_sb.loc[df_sb['fk_address']==107,['local_time', 'Irr']]
    temp_media_tcard = []
    for item in df_irr["local_time"]:
        temp_media_tcard.append(
            pd.to_numeric(
                df_sb["T_CARD"].loc[
                    (df_sb["local_time"]==item) & (df_sb["fk_address"]!=110) 
                ]
            ).mean()
        )
    df_irr['temp_media_string_box'] = temp_media_tcard
    df_int = pd.merge(df_int, df_irr, on = 'local_time') 
    df_sb.drop(columns=["Irr"], inplace=True)
    return(df_sb, df_int)

def update_database(df, tabella, sql_engine):
    try:
        sql_engine.db_alter_table(tabella, df)
    except Exception as err:
        print("impossibile eseguire l'upload")
        print(err)
    return

def get_list_of_missing_days(last_db_entry):
    missing_days=[]
    last_entry_date = datetime.fromtimestamp(
        list(last_db_entry.loc[0])[0],
        tz = timezone('Europe/Amsterdam')
    ).date()
    today_date = datetime.now().date()
    t_delta =today_date-last_entry_date
    for i in range(t_delta.days + 1):
        day = datetime.strftime(
            last_entry_date + timedelta(days=i), 
            '%y%m%d')
        missing_days.append(day)
    return missing_days


################################
## BLOCCO DI FUNZIONI PRINCIPALI
################################

def check_last_db_entry(sql_engine, db_table):
    now = int(datetime.today().timestamp())   
    query = "SELECT timestamp_utc FROM "+db_table+" WHERE timestamp_utc <= "+str(now)+" ORDER BY timestamp_utc DESC LIMIT 1"
    last_db_entry = sql_engine.db_fetch(query)
    return (last_db_entry)

def update_dataset_directory(last_db_entry):
    if not last_db_entry.empty: ##SE VI È ALMENO UN VALORE SUL DB, SCARICA I DATI DEI NUOVI VALORI 
        daylist_to_upload = get_list_of_missing_days(last_db_entry)
        for day in daylist_to_upload:
            download_ftp(day)

    else: ## SE IL DB È VUOTO -->
        if not os.listdir(download_data_path): ## SE NON CI SONO CARTELLE, SCARICA L'INTERO DATASET
            print('Directory vuota - download dei file FTP')
            lista_file_ftp=[]
            ftp_engine = ftp_connection(ftp_host, ftp_port, ftp_user, ftp_password)
            ftp_engine.cwd('DATA')
            ftp_engine.retrlines('LIST', lista_file_ftp.append)
            ftp_engine.quit()
            for file in lista_file_ftp:
                if file[-6::].isnumeric():
                    day = str(file[-6::])
                    download_ftp(day)
            daylist_to_upload = os.listdir(download_data_path)

        else: ##SE VI SONO GIÀ ALTRE CARTELLE --> PROCEDERE CON L'UPLOAD
            print('La cartella contiene cartelle non presenti sul db: ')
            daylist_to_upload = os.listdir(download_data_path)  ## verificare contenuto? 
    return daylist_to_upload

def extract_dataframe_and_upload_to_database(sql_engine, last_db_entry, daylist_to_upload):
    df_sb = pd.DataFrame()
    df_int = pd.DataFrame()    
    for day in daylist_to_upload:
        try:
            df_daily_sb, df_daily_int = get_data_as_dataframe(day)
            df_sb = pd.concat([df_sb, df_daily_sb])
            df_int = pd.concat([df_int, df_daily_int])
        except Exception as err:
            print(err)
    if not last_db_entry.empty:
        df_sb = df_sb.loc[df_sb['timestamp_utc']>last_db_entry.loc[0][0]]
        df_int = df_int.loc[df_int['timestamp_utc']>last_db_entry.loc[0][0]]
    if not df_sb.empty:
        update_database(df_sb, db_table_stringa, sql_engine)
    if not df_int.empty:
        update_database(df_int, db_table_inverter, sql_engine)
    return
    
################################
## PROGRAMMA PRINCIPALE
################################
    
def datalogger():
    now = datetime.now()
    print('Orario esecuzione: ', now)
    sql_engine = mysql_client(user_inverter, password_inverter, host_inverter, db_inverter)
    ultimo_dato_su_db = check_last_db_entry(sql_engine, db_table_inverter)
    lista_giorni_da_caricare = update_dataset_directory(ultimo_dato_su_db)
    extract_dataframe_and_upload_to_database(sql_engine, ultimo_dato_su_db, lista_giorni_da_caricare)
    print('Ultima verifica: ', datetime.now())    
    print('Fine Operazioni')
    return

################################
## FUNZIONE MAIN
################################

while True:
    try:
        datalogger()
    except Exception as error:
        sys.exit(str(error))
    break