import pandas as pd
import numpy as np
import os
from matplotlib import pyplot as plt 
from cfg_docu.init import *


def get_df_day(df_int, df_sb):
    df_day = pd.merge(df_int, df_sb, on = '0_Time') 
    df_day.dropna(subset=['0_Inv_State'], inplace=True)
    #df_day = multi_indexing_final_df(df_day)  #RIMOSSO 30/06/2022
    #df_day = df_day['0_P_AC'].astype(float) #AGGIUNTO 30/06/2022
    return(df_day)

def get_df_string_box(file):
    indirizzi_da_rinominare = [101.0, 102.0, 103.0, 104.0, 105.0, 106.0, 107.0, 108.0, 109.0, 110.0, 111.0, 112.0]
    headers_da_rinominare = ['Adresse', 's', 'tres comp', 'I_AC_av', 'I_DC0', 'I_DC1', 'I_DC2', 'I_DC3', 'I_DC4', 'I_DC5', 'I_DC6', 'T_CARD'] #MODIFICATO 01_07_22 AGGIUNTO T_PAN
    nuovi_indirizzi = ['1.1', '1.2', '2.1', '2.2', '3.1', '3.2', '4.1', '4.2', '5.1', '5.2', '6.1', '6.2']
    colonne_vuote = ['Typ', 'S_VERS', 'Thres_dev', 'Time_dev', 'Str_State', 'Box_State', 'Box_Reg', 'I_DC7', 'T_AMB', 'T_PAN', 'v', 'DIG_INPUT','SPARE2', 'SPARE3', 'SPARE4'] #MODIFICATO 01_07_22 RIMOSSO T_PAN DOPO T_AMB

    #df = pd.read_csv(file, sep=';', header=4, encoding="UTF-8", encoding_errors="replace")   #encoding mbcs per sistemi operativi Windows. Per ubuntu UTF-8
    df = pd.read_csv(file, sep=';', header=4, encoding_errors="replace")
    df = get_df_time(df)  #AGGIUNTO 27_04
    df = clear_df_string_box(df)
    df, nuove_colonne = header_reindex_df_string_box(df, colonne_vuote, indirizzi_da_rinominare, nuovi_indirizzi)    
    df = arrange_final_df_string_box(df, headers_da_rinominare, nuovi_indirizzi, nuove_colonne)
    return df

def get_df_time (df): #AGGIUNTO 27_04
    df_time = df.loc[:,['Unnamed: 0']]
    df_time.rename(columns={'Unnamed: 0':'Universal_Time'}, inplace=True)
    df=pd.concat([df_time,df], axis=1)
    df.reset_index(inplace=True)
    df.drop(columns=['index'], inplace=True)
    df.rename(columns={'Unnamed: 0':'Time_index'}, inplace=True)
    df.set_index(keys='Time_index', inplace=True)
    return(df)

def clear_df_string_box(df):
    #aggiungere ciclo for per verificare gli indirizzi esistenti ed eliminare solo quelli differenti - utilizzare vecchi indirizzi"
    df.drop(df.loc[df['Adresse']==-1].index, inplace=True)
    df.drop(df.loc[df['Adresse']==0.0].index, inplace=True) # AGGIUNTO 27_04
    df.rename(columns={'Universal_Time':'0_Time'}, inplace=True) #precedentemente Unnamed: 0 -> 0_Time
    df.rename(columns={'Irr':'0_Irr'}, inplace=True) #SPOSTARE SOPRA
    df.sort_values(by=['Adresse', '0_Time'], inplace=True) 
    df.reset_index(inplace=True)
    df.set_index(keys='Time_index', inplace=True)
    return df

def get_df_irraggiamento(df, indirizzi_da_rinominare):
    df_irr = pd.concat([df.loc[df['Adresse']==indirizzi_da_rinominare[6],'0_Time'], df.loc[df['Adresse']==indirizzi_da_rinominare[6],'0_Irr']], axis=1)
    #df_irr = df_irr.astype(float) #AGGIUNTO 30/06/2022
    return df_irr

def header_reindex_df_string_box(df, colonne_vuote, indirizzi_da_rinominare, nuovi_indirizzi):
    for headers in colonne_vuote:
        df.drop(columns=[headers], inplace=True)
    nuove_colonne=[0,0,0,0,0,0,0,0,0,0,0,0] #inizializzo il vettore - un elemento per indirizzo
    for old_ind in indirizzi_da_rinominare:
        k = indirizzi_da_rinominare.index(old_ind)
        df['Adresse'].replace({indirizzi_da_rinominare[k]:nuovi_indirizzi[k]}, inplace=True)  
    for new_ind in nuovi_indirizzi:  
        k = nuovi_indirizzi.index(new_ind)
        nuove_colonne[k]=[('Adresse'), (new_ind+'_intervallo misura'), (new_ind+'_tres comp'), (new_ind+'_I_AC_av'), (new_ind+'_1'), (new_ind+'_2'), (new_ind+'_3'), (new_ind+'_4'), (new_ind+'_5'), (new_ind+'_6'), (new_ind+'_7'), (new_ind+'_TCARD')] #MODIFICATO 01_07_22 AGGIUNTO _TPAN
    return(df, nuove_colonne)

def arrange_final_df_string_box(df, headers_da_rinominare, nuovi_indirizzi, nuove_colonne):        
    df_s=df.loc[df['Adresse']==nuovi_indirizzi[6],['0_Time', '0_Irr']].reset_index().set_index(keys='Time_index')
    df.to_csv(export_path+'/df_string_box.csv', index=False)
    
    for i in range(0,12):  
        for k in range(0,12): #MODIFICATO DA PRECEDENTE 0:11 AGGIUNTO 01_07_22 _ se si vuole aggiungere altri headers da leggere [headers_da_rinominare] incrementare questo valore
            df_iterativo = (df.loc[df['Adresse']==nuovi_indirizzi[i], [headers_da_rinominare[k]]]).reset_index().set_index(keys='Time_index')
            df_concatena = pd.DataFrame( {nuove_colonne[i][k]:df_iterativo.iloc[0:len(df_iterativo.index),0]}).reset_index().set_index(keys='Time_index')#.drop(columns=['index'])
            df_s= pd.concat([df_s, df_concatena], axis=1)
    df_s.drop(columns=['Adresse', '1.2_7', '2.2_7', '3.2_7', '4.2_7', '5.2_TCARD'], inplace=True)
    return(df_s)       

def multi_indexing_final_df(df): 
    vec=np.array(df.keys())
    for i in range (0, len(vec)):
        a, b = vec[i].split('_',1)
        vec[i]=a
    df.columns = pd.MultiIndex.from_arrays([vec, df.columns],names=['L0','L1'])
    return(df)

def get_df_inverter(file, data): 
    colonne_vuote = ['Adresse', 'Typ', 'Inv_size', 'SPARE1', 'SPARE2', 'ALARM1', 'ALARM2', 'ALARM3', 'SPARE3', 'SPARE5', 'Unnamed: 26']
    #df = pd.read_csv(file, sep=';', header=4, encoding="UTF-8", encoding_errors="replace")
    df = pd.read_csv(file, sep=';', header=4, encoding_errors="replace")
    df = get_df_time(df) # AGGIUNTO 27_04
    df = concat_datatime(df, data)                        
    df = clear_df_inverter(df, colonne_vuote)
    return(df)
                         
def concat_datatime(df, data):  
    size = df.shape[0]    
    a = data[:2]
    m = data[2:4]
    g = data[4:6]    
    vec_a=[]
    vec_m=[]
    vec_g=[]    
    vec_a = [a for i in range (size)]
    vec_m = [m for i in range (size)]
    vec_g = [g for i in range (size)]         
    dati = np.array([vec_a, vec_m, vec_g])    
    df['Anno'] = vec_a
    df['Mese'] = vec_m
    df['Giorno'] = vec_g                      
    return(df)
                           
def clear_df_inverter(df, colonne_vuote):
    df.drop(df.loc[df['Adresse']!=1].index, inplace=True)
    df.rename(columns={'Universal_Time':'Time'}, inplace=True)
    for headers in colonne_vuote:
        df.drop(columns=[headers], inplace=True)
    for key in df.keys():
        df.rename(columns={key:'0_'+key}, inplace=True)
    df.reset_index(inplace=True)
    df.set_index(keys='Time_index', inplace=True)
    return (df)