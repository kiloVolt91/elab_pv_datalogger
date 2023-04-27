#PLOT GRAFICO STRINGHE - VARIANTE 2

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
import matplotlib.ticker as ticker
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

def get_string_box_indexes (df_day):
    colonne_df = df_day.columns.tolist()
    #creazione lista indirizzi di quadro
    primo_numero = [1,2,3,4,5,6]
    secondo_numero = [1,2]
    s =[]
    i = 1
    j = 1
    for j in range (0,6):
        for i in range (0,2):
            s.append(str(primo_numero[j]) +'.'+ str(secondo_numero[i]))
            i = i+1
        j = j+1

    liste = []
    for i in range(0, len(s)):
        stringhe = []
        for colonna in colonne_df:
            if colonna[4:5].isnumeric():
                if colonna[0:3] == s[i]:
                    stringhe.append(colonna)
        liste.append(stringhe)
    return(liste)
    

#SCRIPT AUTO-UPDATE PLOT

plt.ion()
plot_rows = 7
plot_cols = 12
figure, ax = plt.subplots(plot_rows, plot_cols, figsize=(35, 100))
figure.tight_layout(pad=10.0)

text_names = {}
k = 0
for i in range (0, plot_rows):
    for j in range (0, plot_cols):
        k = k+1
        text_names["text{0}".format(k)] = 0#ax[i,j].text(0, 0, '')
nomi = list(text_names)
k=0
for i in range (0, plot_rows):
    for j in range (0, plot_cols):
        nomi[k] = ax[i,j].text(0, 0, '')
        k=k+1
        

url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+database

while True:
    now = datetime.now()
    df_day= download_df_from_db(url, now)
    liste = get_string_box_indexes (df_day)
    update_potenza, update_irraggiamento, update_orario = update_valori_grafico(df_day, now)
    label_asse_x = update_orario  
    indici_asse_x = np.linspace(1, len(update_orario), len(update_orario))
    valore_nominale = 5210
    bar_width=0.2
    bottom = np.zeros(3)
    k=0
    if df_day.empty == False:
        for j in range (0, len(liste)): 
            for i in range (0, len(liste[j])):
                data = df_day[liste[j][i]].iloc[-1]
                delta = valore_nominale - data
                if delta < 0:
                    delta = 0
                nomi[k].remove()
                ax[i,j].cla()
                ax[i,j].bar(0,data,bar_width, color = '#ff7f0e') 
                ax[i,j].bar(0,delta,bar_width, color = '#1f77b4',bottom=data)
                nomi[k] = ax[i,j].text(0, 5500, str(data)[0:4], color="orangered", verticalalignment="center", horizontalalignment="center", size=18)
                ax[i,j].grid(visible=True)
                ax[i,j].title.set_text("Stringa - "+str(liste[j][i]))
                ax[i,j].set_ylim(ymin=0, ymax=6500)
                ax[i,j].yaxis.set_major_locator(ticker.MaxNLocator(7))
                ax[i,j].yaxis.set_minor_locator(ticker.MaxNLocator(7))
                ax[i,j].tick_params(axis='x', which='both', bottom=False, top=False, labelbottom=False)
                k=k+1

    figure.subplots_adjust(wspace=0.5, hspace=0.2)
    figure.canvas.draw()
    figure.canvas.flush_events()
    
    time.sleep(300)