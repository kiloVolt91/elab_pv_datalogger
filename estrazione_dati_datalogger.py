#INIZIALIZZAZIONE 
from configurazione.funzioni_dataframe import *
from configurazione.dati import *
from configurazione.init import *
from datetime import datetime
import sys
import pandas as pd
import numpy as np
import os
from matplotlib import pyplot as plt 

#VERIFICA DIRECTORIES DISPONIBILI NEL PERCORSO SELEZIONATO
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
data_fine = seleziona_data_fine(lista_date)
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
    nuove_colonne=[0,0,0,0,0,0,0,0,0,0,0,0] 
    for new_ind in nuovi_indirizzi:  
        k = nuovi_indirizzi.index(new_ind)
        nuove_colonne[k]=[('Adresse'), (new_ind+'_intervallo misura'), (new_ind+'_tres comp'), (new_ind+'_I_AC_av'), (new_ind+'_1'), (new_ind+'_2'), (new_ind+'_3'), (new_ind+'_4'), (new_ind+'_5'), (new_ind+'_6'), (new_ind+'_7'), (new_ind+'_TCARD')] #MODIFICATO 01_07_22 AGGIUNTO _TPAN
    df.dropna(inplace=True, axis =0)
    stringa_datetime = ' '+data[0:2]+'-'+data[2:4]+'-'+data[4:6]
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
test_path = "/home/episciotta/Documenti/SVILUPPO/repo_sviluppo_ctz/elab_pv_datalogger/"
df_year.to_csv(test_path+'riepilogo.csv', index=False)    
print('Fine Operazioni')       
