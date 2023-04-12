from datetime import datetime
from configurazione.init import *
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import errorcode
import sqlalchemy
import time
import matplotlib.pyplot as plt
import matplotlib
import math
matplotlib.use("TkAgg")


def download_df_from_db(url, now):
    today = str(now)[0:11] + '00:00:00'
    connection = sqlalchemy.create_engine(url)
    query = "SELECT * FROM fv WHERE 0_Time BETWEEN '" +str(today)+ "' AND '"+str(now)+"' ORDER BY 0_Time"
    df_day = pd.read_sql(query, connection)
    return(df_day)

def update_valori_grafico(df_day, now):
    print('Nuovo inserimento dati, ora: ', str(now.hour).zfill(2)+':'+str(now.minute).zfill(2))
    update_potenza = df_day['0_P_AC'].tolist()
    update_data = df_day['0_Time'].tolist()
    update_irraggiamento = df_day['0_Irr'].tolist()
    update_orario = []
    for data in update_data:
        update_orario.append(str(data.hour).zfill(2)+':'+str(data.minute).zfill(2))
    return(update_potenza, update_irraggiamento, update_orario)

def get_performance_ratio(update_potenza, update_orario, update_irraggiamento):
        irr_std =1000
        p_std = 300
        power_performance = np.divide(update_potenza,p_std)
        irr_performance = np.divide(update_irraggiamento,irr_std)
        prestazione=[]
        for i in range (0, len(update_orario)):
            if irr_performance[i] == 0:
                prestazione.append(0);
            else:
                perf_ratio = power_performance[i]/irr_performance[i]
                if perf_ratio > 1:
                    prestazione.append(1)
                else:                
                    prestazione.append(perf_ratio)
        return(prestazione)

def update_plot(figure, ax, df_day, update_orario, update_potenza, prestazione):
    label_asse_x = update_orario     
    indici_asse_x = np.linspace(1, len(update_orario), len(update_orario))
    line1.set_xdata(indici_asse_x)
    line1.set_ydata(update_potenza)
    line2.set_xdata(indici_asse_x)
    line2.set_ydata(update_potenza)
    line3.set_xdata(indici_asse_x)
    line3.set_ydata(prestazione)
    energia_giornaliera_tot = df_day['0_E_DAY'].iloc[-1]
    
    if df_day['0_E_DAY'].shape[0] >=3:
        prev_delta = df_day['0_E_DAY'].iloc[-2]-df_day['0_E_DAY'].iloc[-3]
        actual_delta = df_day['0_E_DAY'].iloc[-1]-df_day['0_E_DAY'].iloc[-2]
        if prev_delta > 0:
            prod_ratio = round(((actual_delta - prev_delta)/prev_delta)*100,2)
    else:
        prod_ratio = '__'

    text1.set_text('Energia Prodotta: '+str(energia_giornaliera_tot)+ ' kWh')
    text2.set_text('Variazione produzione: '+str(prod_ratio)+ ' %')
    ax.relim()
    ax.autoscale_view(True,True,True)
    ax2.relim()
    ax2.autoscale_view(True,True,True)
    ax.relim()
    ax.autoscale_view(True,True,True)
    ax2.relim()
    ax2.autoscale_view(True,True,True)

    plt.title("Potenza Output - Giorno: "+str(now)[0:11], fontsize=16)
    plt.xticks (indici_asse_x, label_asse_x)#, rotation='vertical')
    nbins = int(math.ceil(len(indici_asse_x)/7)+1)
    plt.locator_params(nbins=nbins)
    ax.grid(visible=True)
    figure.canvas.draw()
    figure.canvas.flush_events()
    return
    

#SCRIPT AUTO-UPDATE PLOT

plt.ion()
figure, ax = plt.subplots(figsize=(15, 15))
x = 0
y = 0
line1, = ax.plot(x, y, '.')
line2, = ax.plot(x, y, 'r')
text1 = ax.text(15,120, 'Energia Prodotta: '+str(0)+ ' kWh', style='italic', bbox={'facecolor': 'red', 'alpha': 0.5, 'pad': 10})
text2 = ax.text(15,135, 'Variazione Produzione: '+str(0)+ ' %', style='italic', bbox={'facecolor': 'green', 'alpha': 0.5, 'pad': 10})
plt.xlabel("Orario di Acquisizione [HH:MM]")
plt.ylabel("Potenza elettrica [kW]")
ax2 = ax.twinx()
ax2.set_ylabel("indice di performance")
line3, = ax2.plot(x,y, 'b')

url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+database

while True:
    now = datetime.now()
    df_day= download_df_from_db(url, now)
    update_potenza, update_irraggiamento, update_orario = update_valori_grafico(df_day, now)
    prestazione = get_performance_ratio(update_potenza, update_orario, update_irraggiamento)
    update_plot(figure, ax, df_day, update_orario, update_potenza, prestazione)
    time.sleep(300)