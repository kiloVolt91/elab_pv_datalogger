##ESTRAZIONE DEL DATAFRAME DAL DATABASE

from datetime import datetime
from datetime import date
import time
import sqlalchemy
import math
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
import matplotlib
from matplotlib import gridspec
from configurazione.init import *
import os
import sys
from sklearn.neural_network import MLPRegressor
import statistics



def get_dataframe_from_database(user, password, host, db, now, data_inizio, fk_id, db_table, db_col, gmt):
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    if fk_id != 'none':
        sec_query = " AND fk_idParameter = '"+str(fk_id)+"'" #energia attiva prodotta
    else:
        sec_query='' 
    query = "SELECT * FROM "+str(db_table)+" WHERE "+str(db_col)+" BETWEEN '" +str(data_inizio)+ "' AND '"+str(now)+"'"+sec_query
    df = pd.read_sql(query, connection)
    return(df)

def convert_data_inizio(origin_path):
    lista_date = []   
    os.chdir(origin_path+'/download_ftp')
    for name in os.listdir(os.getcwd()):    
        if (name[0:1]=='2'):
            lista_date.append(name[0:18])       
    lista_date.sort()
    aa = str(lista_date[0])[0:2]
    mm = str(lista_date[0])[2:4]
    dd = str(lista_date[0])[4:6]
    data_inizio = str(20)+str(aa) +'-'+str(mm)+'-'+str(dd) + ' 00:00:00'
    return(data_inizio)


## ESTRAZIONE DATI DAL DB
data_inizio = convert_data_inizio(origin_path)
t0 = datetime.now()
print('Orario di inizio estrazione dati: ', t0)

df = get_dataframe_from_database(user, password, host_inverter, db_inverter, t0, data_inizio, 'none', db_table_inverter, db_col_inverter,0)
t1 = datetime.now()
print('Orario di fine estrazione dati: ', t1)
print('Durata estrazione dati: ', (t1-t0).total_seconds(), ' secondi')




def estrazione_colonne_temp_string(df):
    temperature = []
    stringhe = []
    colonne = df.columns.tolist()    
    for colonna in colonne:
        if colonna[4::] == 'TCARD':
            temperature.append(colonna)
        if colonna[4:5].isnumeric():
            stringhe.append(colonna)
    return(temperature, stringhe)
    
def calcolo_indicatori_identificazione_outliers(pd_series):
    q1 = pd_series.quantile(0.25)
    q3 = pd_series.quantile(0.75)
    iqr = q3-q1
    lower = q1-1.5*iqr
    upper = q3+1.5*iqr
    return(lower, upper)

def rimozione_outliers_df(df):
    global colonna_temperature
    print('Dimensioni dataset iniziale: ', df.shape[0])
    print('Irraggiamento - Potenza - Dataset non manipolato')
    irr = df['0_Irr'].astype(float).tolist()
    power = df['0_P_AC'].astype(float).tolist()
    plt.scatter(irr, power,  c='b')
    plt.show()
    colonna_temperature, colonna_stringhe = estrazione_colonne_temp_string(df)
    df['avarage_temps'] = df[colonna_temperature].astype(float).mean(axis=1)
    
    # Maschera booleana - eliminazione dei valori nulli di irraggiamento e potenza
    masked_1 = df[
        (df['0_P_AC'].astype(float)!=0) & 
        (df['0_Irr'].astype(float)!=0)
    ]
    
    # Maschera booleana - riduzione della dimensione del df, eliminazione dei dati corrispondenti a valori nulli delle stringhe 
    for colonna in colonna_stringhe:
        masked_1 = masked_1[ 
        (masked_1[colonna].astype(float)!=0)
        ]
    res_irr = masked_1['0_Irr'].astype(float).tolist()
    res_power = masked_1['0_P_AC'].astype(float).tolist()
    #relazione potenza&irraggiamento per rimozione degli outliers legati a malfunzionamenti dell'impianto
    ratio = []
    for i in range(0, len(res_power)):
        ratio.append(res_power[i]/res_irr[i])
    masked_1['ratio'] = ratio
    limite_inferiore, limite_superiore = calcolo_indicatori_identificazione_outliers(masked_1['ratio'])
    masked_2 = masked_1[
        (masked_1['ratio']>limite_inferiore) &
        (masked_1['ratio']<limite_superiore)
    ]
    
    irr = masked_2['0_Irr'].astype(float).tolist()
    power = masked_2['0_P_AC'].astype(float).tolist()
    print('Dimensioni dataset finale: ', masked_2.shape[0])
    print('Irraggiamento - Potenza - Dataset senza outliers')
    plt.scatter(irr, power,  c='b')
    plt.show()
    masked_2.reset_index(inplace=True, drop=True)
    return(masked_2)

def normalizza_df(df):
    colonne_da_normalizzare = ['0_P_AC', '0_Irr', 'avarage_temps']
    global parametri
    parametri = {}
    for colonna in colonne_da_normalizzare:
        minimo = df[colonna].min()
        massimo = df[colonna].max()
        norm_range = massimo - minimo 
        colonna_normalizzata = []
        for i in range (0, len(df[colonna])):
            val = df[colonna].astype(float)[i]
            colonna_normalizzata.append((val - minimo)/(norm_range))
        df.drop(columns=colonna, inplace=True)
        df[colonna] = colonna_normalizzata
        dict_key = colonna
        parametri[dict_key] = [norm_range, minimo]
    return

    
def define_regressor_model(x_training, y_training, x_testing, y_testing, n_iter):
    model = MLPRegressor(solver='lbfgs',alpha=0.001,hidden_layer_sizes=(7,), max_iter=n_iter)
    model.fit(x_training,y_training)
    y_pred = model.predict(x_testing)
    e = []
    for i in range(len(y_testing)):
        delta = y_testing[i]-y_pred[i]
        e.append(delta) 
    e_av = statistics.mean(e)*100
    e_std = statistics.stdev(e)
    e_info = np.array([e_av, e_std])
    print('Avarage Error :', e_av)
    print('STD :', e_std)
    plt.scatter(y_pred, y_testing)
    plt.show()
    return (model, e_info)

def update_dataframe_with_new_values_from_db(user, password, host, db, now, fk_id, db_table, db_col, df):
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    last_entry = df[db_col].tail(1)
    now = datetime.now()
    if fk_id != 'none':
        sec_query = " AND fk_idParameter = '"+str(fk_id)+"'" #energia attiva prodotta
    else:
        sec_query='' 
    #query = "SELECT * FROM "+db_table+" WHERE "+db_col+" BETWEEN '" +str(last_entry.iloc[0])+ "' AND '"+str(now)+"'"+sec_query
    query = "SELECT * FROM "+db_table+" WHERE "+db_col+" > '" +str(last_entry.iloc[0])+"'"+sec_query
    update_df = pd.read_sql(query, connection)
    new_df = pd.concat([df, update_df])
    return(new_df)
    

def random_extract_dataset_for_training_and_testing(df):
    n_dati_totali = df.shape[0]
    n_dati_training = round(0.7*n_dati_totali)
    indici_df_training = np.random.choice(n_dati_totali,n_dati_training, replace=False).tolist()
    indici_df_training.sort()
    df_training = df.iloc[indici_df_training]
    df_testing = df.copy()
    df_testing.drop(index=indici_df_training, inplace=True)
    input_irr_training_set = df_training['0_Irr'].tolist()
    input_irr_test_set = df_testing['0_Irr'].tolist()
    input_temp_training_set = df_training['avarage_temps'].tolist()
    input_temp_test_set = df_testing['avarage_temps'].tolist()
    output_power_training_set = df_training['0_P_AC'].tolist()
    output_power_test_set = df_testing['0_P_AC'].tolist()
    
    print('input N. dati Training - Irraggiamento: ',len(input_irr_training_set))
    print('input N. dati Test - Irraggiamento: ',len(input_irr_test_set))
    print('input N. dati Training - Temperature: ',len(input_temp_training_set))
    print('input N. dati Test - Temperature: ',len(input_temp_test_set))
    print('output N. dati Training - Potenza: ',len(output_power_training_set))
    print('output N. dati Test - Potenza: ',len(output_power_test_set))
    print('Dati totali: ',len(output_power_test_set)+len(output_power_training_set))
    
    x1_training = np.array([input_irr_training_set], dtype=float).ravel()
    x2_training = np.array([input_temp_training_set], dtype=float).ravel()
    y_training = np.array([output_power_training_set], dtype=float).ravel()
    y_training.reshape(1,-1)
    x_training = np.array([x1_training,x2_training]).T
    x1_testing = np.array([input_irr_test_set], dtype=float).ravel()
    x2_testing = np.array([input_temp_test_set], dtype=float).ravel()
    y_testing = np.array([output_power_test_set], dtype=float).ravel()
    y_testing.reshape(1,-1)
    x_testing = np.array([x1_testing,x2_testing]).T
    return (x_training, y_training, x_testing, y_testing)


### MAIN CODE ###

now = datetime.now()
dataset = rimozione_outliers_df(df)

ans = str(input("Normalizzare il dataset? (y/n): ")).lower()
if (ans != 'y') and (ans != 'n'):
    sys.exit('Il valore inserito è errato')
if ans == 'y':
    print('sì')
    normalizza_df(dataset)
else:
    print('no')
    
    # parametri_potenza = normalizza_df(dataset, '0_P_AC')
    # parametri_irraggiamento = normalizza_df(dataset, '0_Irr')
    # parametri_temp = normalizza_df(dataset, 'temp_medie')

x_training, y_training, x_testing, y_testing = random_extract_dataset_for_training_and_testing(dataset)
forecasting_model, errore = define_regressor_model(x_training, y_training, x_testing, y_testing, n_iter=400)

##SEI ARRIVATO QUI. HAI NORMALIZZATO IL DF, DEVI VERIFICARE CHE L'OPZIONE DI NON NORMALIZZAZIONE PERMETTA DI ESTRARRE I DATI NON NORMALIZZATI SENZA ERRORI
## DEVI CALCOLARE I VALORI FORECAST E CONVERTIRLI
## CREARE UNA COLONNA DEL DB CON I VALORI PREVISTI E CON L'INDICE DI RENDIMENTO
## FILTRARE I DATI IN CORRISPONDENZA DEL CONFINE SUPERIORE ED INFERIORE? (MASSIMI ERRORI DI PREVISIONE??)

while True:
    df = update_dataframe_with_new_values_from_db(user, password, host_inverter, db_inverter, now, 'none', db_table_inverter, db_col_inverter, df)
    test_irr = df['0_Irr'].tail(1).iloc[0]
    test_temp = df[colonna_temperature].tail(1).mean().iloc[0]
    test_power= df['0_P_AC'].tail(1).iloc[0]
    test = np.array([test_irr, test_temp]).reshape(1,-1)
    prev= forecasting_model.predict(test)

    PR  = test_power/prev
    print(PR)
    break
    
    
## WORK IN PROGRESS - PARTE DI SCRIPT PER EFFETTUARE LE PREVISIONI UTILIZZANDO IL MODELLO REALIZZATO