# ESTRAZIONE DATI ELETTRICI ED ENERGETICI DAL DATALOGGER GEFRAN RADIUS LOG PRO DISLOCATO PRESSO L'IMPIANTO FOTOVOLTAICO ASI-GPP - exRVR
# I DATI DA ESTRARRE DEVONO ESSERE PREVENTIVAMENTE SCARICATI ALL'INTERNO DI UN APPOSITO PERCORSO (VEDI FILE DI CONFIGURAZIONE)
# VI È LA POSSIBILITÀ DI CARICARE I DATI SUL DATABASE PREDISPOSTO
# VIENE GENERATO UN RIEPILOGO .CSV CONTENENTE IL DATAFRAME SCARICATO 


#INIZIALIZZAZIONE 

from configurazione.dati import *
from configurazione.init import *
from datetime import datetime
import sys
import pandas as pd
import numpy as np
import os
from matplotlib import pyplot as plt 
import mysql.connector
from mysql.connector import errorcode

def seleziona_data_inizio (lista_date):
    while True:
        ans = input("Selezionare una data di inizio per l'import dei dati? (y/n): ")
        if ans.lower() == 'y':
            anno_inizio = input("Inserire anno nel formato AAAA")

            if anno_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(anno_inizio)) != 4) or (str(anno_inizio)[0:2]!='20'):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente anno: ', anno_inizio)
            data_inizio = str(anno_inizio)[2:4]
            mese_inizio = input("inserire mese nel formato mm")
            if mese_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(mese_inizio)) != 2) or (int(mese_inizio)>12):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente mese: ', mese_inizio)
            data_inizio = data_inizio + mese_inizio

            if (mese_inizio == "02"):
                if (int(anno_inizio)%4 == 0 and int(anno_inizio)%100 !=0) or (int(anno_inizio)%400 == 0):
                    mese_inizio="2b"
            giorno_inizio = input("inserire giorno nel formato dd") 
            if giorno_inizio.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(giorno_inizio)) != 2) or (int(giorno_inizio)>(month[mese_inizio])):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente giorno: ', giorno_inizio)
            data_inizio = data_inizio + giorno_inizio

            if lista_date.count(data_inizio) == 0:
                sys.exit('La data scelta non è presente tra quelle disponibili') 
            break
        elif ans.lower() == 'n':
            data_inizio = lista_date[0]
            print('Hai selezionato il seguente giorno: ', data_inizio)
            break
        else:
            print('Il carattere inserito è errato')
    return (data_inizio)


#Selezione di una data di FINE per l'import dei dati
def seleziona_data_fine (lista_date, data_inizio):
    while True:
        ans = input("Selezionare una data di fine per l'import dei dati? (y/n): ")
        if ans.lower() == 'y':
            anno_fine = input("Inserire anno nel formato AAAA")

            if anno_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(anno_fine)) != 4) or (str(anno_fine)[0:2]!='20'):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente anno: ', anno_fine)
            data_fine = str(anno_fine)[2:4]
            mese_fine = input("inserire mese nel formato mm")
            if mese_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(mese_fine)) != 2) or (int(mese_fine)>12):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente mese: ', mese_fine)
            data_fine = data_fine + mese_fine

            if (mese_fine == "02"):
                if (int(anno_fine)%4 == 0 and int(anno_fine)%100 !=0) or (int(anno_fine)%400 == 0):
                    mese_fine="2b"
            giorno_fine = input("inserire giorno nel formato dd") 
            if giorno_fine.isnumeric() == False:
                sys.exit('Il valore inserito non è un numero')
            if (len(str(giorno_fine)) != 2) or (int(giorno_fine)>(month[mese_fine])):
                sys.exit('Il valore inserito è errato o non è presente') 
            print('Hai selezionato il seguente giorno: ', giorno_fine)
            data_fine = data_fine + giorno_fine

            if lista_date.count(data_fine) == 0:
                sys.exit('La data scelta non è presente tra quelle disponibili')
            if lista_date.index(data_inizio) > lista_date.index(data_fine):
                sys.exit('La data scelta è antecedente alla data di inizio')
            break
        elif ans.lower() == 'n':
            data_fine = lista_date[len(lista_date)-1]
            print('Hai selezionato il seguente giorno: ', data_fine)
            break
        else:
            print('Il carattere inserito è errato')
    return (data_fine)

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

#ESTRAZIONE DEI DATI (DA SCRIPT ESTRAZIONE_DATI)
lista_date = []   
os.chdir(origin_path+'/download_ftp')
for name in os.listdir(os.getcwd()):    
    if (name[0:1]=='2'):
        lista_date.append(name[0:18])       
lista_date.sort()
print('Intervallo temporale dati: ', lista_date[0],' - ', lista_date[len(lista_date)-1]) 


#INIZIALIZZAZIONE
df_year=pd.DataFrame()
data_inizio = seleziona_data_inizio(lista_date)
data_fine = seleziona_data_fine(lista_date, data_inizio)
indice = lista_date.index(data_inizio)
while indice <= lista_date.index(data_fine):
    data = lista_date[indice]
    file_sb = 'download_sb_' + data + '.txt'
    file_int = 'download_int_' + data + '.txt'
    
#ESTRAZIONE DF STRING_BOX
    os.chdir(data)
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
    stringa_datetime = ' '+data[0:2]+'-'+data[2:4]+'-'+data[4:6]
    df['0_Time'] = df['0_Time'] + stringa_datetime
    df['0_Time'] = pd.to_datetime(df['0_Time'], format = '%H:%M:%S %y-%m-%d')
    df['time_index'] = df['0_Time']
    df.set_index(keys='time_index', inplace=True)
    
#RE-DISLOCAZIONE DELLE INFORMAZIONI DI STRINGA SU APPOSITE COLONNE
    df_irraggiamento=df.loc[df['Adresse']==nuovi_indirizzi[6],['0_Time', '0_Irr']]
    df_string_box = df_irraggiamento.copy()
    for i in range(0,len(nuovi_indirizzi)):  
        for k in range(0,len(headers_da_rinominare)):
            df_iterativo = (df.loc[df['Adresse']==nuovi_indirizzi[i], [headers_da_rinominare[k]]]) 
            df_concatena = pd.DataFrame( {nuove_colonne[i][k]:df_iterativo.iloc[0:len(df_iterativo.index),0]})
            df_string_box= pd.concat([df_string_box, df_concatena], axis=1)
    df_string_box.drop(columns=['Adresse', '1.2_7', '2.2_7', '3.2_7', '4.2_7', '5.2_TCARD'], inplace=True)

#ESTRAZIONE DF INVERTER
    os.chdir(data)
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
    
#CREAZIONE DF COMPLESSIVI
    df_day = pd.merge(df_inverter, df_string_box, on = '0_Time') 
    df_year = pd.concat([df_year, df_day])

    indice = indice +1
    if indice%10 == True:
        print(indice)

df_year.fillna(value=0, inplace=True)
df_year.to_csv(export_path+'riepilogo_'+data_inizio+'_'+data_fine'.csv', index=False)    

while True:
    ans = input('Esportare i dati verso database mySQL locale? (y/n): ')
    if ans.lower() == 'y':
        cnx = mysql_connection(host, user, password, database)
        sql_export_df(df_year, 'fv', cnx)
        break
    elif ans.lower() == 'n':
        break
    else:
        print('Il carattere inserito è errato')

print('Fine Operazioni')    