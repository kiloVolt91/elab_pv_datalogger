## INSERIRE CORREZIONE PER PASSAGGIO DA ORA SOLARE AD ORA LEGALE CONSIDERANDO CHE L'ORARIO NEL DB_SENECA È SFASATO DEL VALORE GMT
## IN ALCUNI GRAFICI L'ENERGIA PRODOTTA È NULLA
## INSERIRE CONTROLLO PER LA PRESENTAZIONE DI DATI ANOMALI SU GRAFICO


from datetime import datetime
import time
import sqlalchemy
import math

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
import matplotlib
from matplotlib import gridspec
matplotlib.use("TkAgg")
from configurazione.init import *


def get_dataframe_from_database(user, password, host, db, now, fk_id, db_table, db_col, gmt):
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    today = str(now)[0:11] + '00:00:00'
    if gmt !=0:
        today = datetime.strptime(today, '%Y-%m-%d %H:%M:%S')
        today = today.timestamp() * 1000
        today = today - gmt*1000*3600 
        today=datetime.fromtimestamp(today/ 1000)
    if fk_id != 'none':
        sec_query = " AND fk_idParameter = '"+str(fk_id)+"'" #energia attiva prodotta
    else:
        sec_query='' 
    query = "SELECT * FROM "+str(db_table)+" WHERE "+str(db_col)+" BETWEEN '" +str(today)+ "' AND '"+str(now)+"'"+sec_query
    df = pd.read_sql(query, connection)
    return(df)

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
    
def get_vals_to_plot_from_seneca(df, k, gmt):
    numero_di_valori = df.shape[0]
    energia = df['converted_value'].tolist()
    tempo = df['msecs'].tolist()
    potenza_elettrica = []
    intervallo_temporale = []
    conversione_temporale = gmt*1000*3600 
    for i in range (1, numero_di_valori):
        delta_energia = k*(energia[i]-energia[i-1])
        delta_tempo = (tempo[i]-tempo[i-1])/3600000
        potenza_elettrica.append(delta_energia/delta_tempo)
        intervallo_temporale.append(tempo[i]+conversione_temporale)
    return (intervallo_temporale, potenza_elettrica)

def get_daily_energy_value_from_df(df, now, df_col, k):
    energia_iniziale = df[df_col].head(1).iloc[0]
    energia_finale = df[df_col].tail(1).iloc[0]
    energia_giornaliera = (energia_finale-energia_iniziale)*k
    return(energia_giornaliera)
    
def get_vals_to_plot_from_seneca_mean(df, k, gmt, mins):
    numero_di_valori = df.shape[0]
    energia = df['converted_value'].tolist()
    tempo = df['msecs'].tolist()
    potenza_elettrica = []
    intervallo_temporale = []
    conversione_temporale = gmt*1000*3600 
    for i in range (mins, numero_di_valori):
        delta_energia = k*(energia[i]-energia[i-mins])
        delta_tempo = (tempo[i]-tempo[i-mins])/3600000
        potenza_elettrica.append(delta_energia/delta_tempo)
        intervallo_temporale.append(tempo[i]+conversione_temporale)
    return (intervallo_temporale, potenza_elettrica)

now = datetime.now()
today = str(now)[0:11]

while True:
    if today[0:11] != str(now)[0:11]: 
        #plt.savefig(export_path+'/'+today+'_report.png')
        plt.close()
        today = str(now)[0:11]

    #INIZIALIZZAZIONE GRAFICO
    x = 0
    y = 0
    plt.ion()    
    fig = plt.figure(figsize=(25,25))
    gs = gridspec.GridSpec(2,1, height_ratios=[1, 0.25])
    ax1 = plt.subplot(gs[0])
    ax2 = plt.subplot(gs[1])
    
    line1, = ax1.plot(x, y, 'b', label ='Potenza lorda prodotta') #OUTPUT INVERTER
    line2, = ax1.plot(x, y, 'c', label='Potenza netta prodotta') #ENERGIA PRODOTTA - CONTATORE PROD
    line3, = ax1.plot(x, y, 'purple', label = 'Potenza immessa in rete') #ENERGIA IMMESSA IN RETE - CONTATORE SCAMBIO
    line4, = ax1.plot(x, y, 'g', label = 'Potenza autoconsumata') #AUTOCONSUMO AZIENDA - PRODOTTA-IMMESSA
    line5, = ax1.plot(x, y, 'orange', label='Potenza prelevata da rete - Stabilimento') #PRELIEVO DA RETE ENEL - CONTATORE SCAMBIO
    line6, = ax1.plot(x, y, 'yellow', label ='Potenza prelevata da rete - Ausiliari produzione') #PRELIEVO PER CONSUMO AUSILIARI 
    line7, = ax1.plot(x, y, 'red', label = 'Perdite ausiliari produzione') #PERDITE TRASFORMATORE+AUX
    ax1.yaxis.set_major_locator(ticker.LinearLocator(10))
    leg = ax1.legend();
    ax1.grid()
    ax1.set_xlabel("Orario di Acquisizione [HH:MM]")
    ax1.set_ylabel("Potenza elettrica [kW]")
    ax1.set_ylim(0,300)
    ax2.axis([0, 72.5, 0, 20])
    text1=ax2.text(1.5,9.5, 'Energia Lorda Prodotta: '+str(0)+ ' kWh', style='italic', size='medium', weight='bold', bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 10})
    text2=ax2.text(1.5,2.5, 'Energia Netta Prodotta: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'c', 'alpha': 0.5, 'pad': 10})
    text3=ax2.text(19.5,9.5, 'Energia Immessa in Rete: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'purple', 'alpha': 0.5, 'pad': 10})
    text4=ax2.text(19.5,2.5, 'Energia Autoconsumata: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'g', 'alpha': 0.5, 'pad': 10})
    text5=ax2.text(37.5,9.5, 'Energia Prelevata da Rete: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'orange', 'alpha': 0.5, 'pad': 10})
    text6=ax2.text(37.5,2.5, 'Energia Prelevata Aux: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'yellow', 'alpha': 0.5, 'pad': 10})
    text7=ax2.text(55.5,9.5, 'Perdite AC Impianto FV: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'red', 'alpha': 0.5, 'pad': 10})
    text8=ax2.text(55.5,2.5, 'Picco di Potenza: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'magenta', 'alpha': 0.5, 'pad': 10})

    text9=ax2.text(1.5,16.5, 'Introito Incentivo: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 10})
    text10=ax2.text(19.5,16.5, 'Introito Vendita Energia: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 10})
    text11=ax2.text(37.5,16.5, 'Spesa Acquisto Energia: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 10})
    text12=ax2.text(55.5,16.5, 'Risparmio Autoconsumo: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 10})

    ax2.set_xticks([])
    ax2.set_yticks([])
    
    fig.suptitle("Giorno: "+str(now)[0:11], fontsize=16)

    ## INIZIALIZZAZIONE DEI DATAFRAMES CON I DATI PROVENIENTI DAI DB
    df_day_inverter = get_dataframe_from_database(user, password, host_inverter, db_inverter, now, 'none', db_table_inverter, db_col_inverter,0)
    while df_day_inverter.empty==True:
        time.sleep(60) 
        
        ##VERIFICATO, FUNZIONA --> IL CICLO RESTA BLOCCATO QUI AL RIAVVO - IL DF RESTA SEMPRE VUOTO? aggiunto aggiornamento now a seguire, verificare
        #####AL CAMBIO DI GIORNO CREA UN NUOVO GRAFICO ANZICHE AGGIORNARE IL PRECEDENTE vedi sopra
        
        
        now = datetime.now()
        df_day_inverter = get_dataframe_from_database(user, password, host_inverter, db_inverter, now, 'none', db_table_inverter, db_col_inverter,0)   
    last_entry_time = df_day_inverter['0_Time'].tail(1).iloc[-1]
    df_day_seneca_prod = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 24, db_table_seneca, db_col_seneca,2)
    df_day_seneca_prel_aux = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 23, db_table_seneca, db_col_seneca,2)
    df_day_seneca_imm = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 11, db_table_seneca, db_col_seneca,2)
    df_day_seneca_prel = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 12, db_table_seneca, db_col_seneca,2)
    

    while today[0:11] == str(now)[0:11]:  
        now = datetime.now()
        print('Nuovo inserimento dati, ora: ', str(now.hour).zfill(2)+':'+str(now.minute).zfill(2))

        #AGGIORNAMENTO DEI DATAFRAMES
        df_day_inverter = update_dataframe_with_new_values_from_db(user, password, host_inverter, db_inverter, now, 'none', db_table_inverter, db_col_inverter, df_day_inverter)
        last_entry_time = df_day_inverter['0_Time'].tail(1).iloc[-1]
        df_day_seneca_prod = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 24, db_table_seneca, db_col_seneca, df_day_seneca_prod)
        df_day_seneca_prel_aux = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 23, db_table_seneca, db_col_seneca, df_day_seneca_prel_aux)
        df_day_seneca_imm = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 11, db_table_seneca, db_col_seneca, df_day_seneca_imm)
        df_day_seneca_prel = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 12, db_table_seneca, db_col_seneca, df_day_seneca_prel)

        #AGGIORNAMENTO DEI CONTATORI ENERGETICI
        energia_contatore_produzione = get_daily_energy_value_from_df(df_day_seneca_prod, now, 'converted_value', 1)
        energia_contatore_produzione_prelievo_aux = get_daily_energy_value_from_df(df_day_seneca_prel_aux, now, 'converted_value', 1)
        energia_immessa_contatore_scambio = get_daily_energy_value_from_df(df_day_seneca_imm, now, 'converted_value', 800)
        energia_prelevata_contatore_scambio = get_daily_energy_value_from_df(df_day_seneca_prel, now, 'converted_value', 800)
        energia_prodotta_inverter = get_daily_energy_value_from_df(df_day_inverter, now, '0_E_DAY', 1)
        energia_autoconsumata_stabilimento = energia_contatore_produzione-energia_immessa_contatore_scambio
        energia_perduta = energia_prodotta_inverter - energia_contatore_produzione

        #MANIPOLAZIONE DATI INVERTER 
        len_x_inv = df_day_inverter.shape[0]
        y_inv = df_day_inverter['0_P_AC'].tolist()
        time_inv = df_day_inverter['0_Time'].tolist()
        x_inv =[]
        conversione_temporale = 2*1000*3600
        for i in range (0,len(time_inv)):
            x_inv.append(time_inv[i].timestamp()*1000-conversione_temporale)

        #CREAZIONE DEI VALORI MEDI IN 5 MINUTI 
        ## NECESSARIO PER RENDERE OMOGENEA LA RAPPRESENTAZIONE GRAFICA DEI DATI INVERTER E DEI DATI SENECA AVENTI INTERVALLI DI ACQUISIZIONE DIFFERENTI
        gmt = 2
        mins = 5
        x_prod_sen, y_prod_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prod, 1, 0, mins)
        x_imm_sen, y_imm_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_imm, 800, 0, mins)
        x_prel_sen, y_prel_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prel, 800, 0, mins)
        x_prel_aux_sen, y_prel_aux_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prel_aux, 1, 0, mins)

        #CURVA POTENZA AUTOCONSUMATA
        x = x_prod_sen
        autoconsumo =[]
        perdite_aux = []
        for i in range (0, len(x)):
            autoconsumo.append( y_prod_sen[i] - y_imm_sen[i])

        #CURVA PERDITE AUSILIARI IMPIANTO FV
        x1 = len(x_inv) 
        x2 = len(x_prod_sen)
        index_list_inverter =[]
        index_list_seneca =[]
        y_perdite = []
        x_perdite =[]
        k=0
        for i in range (0,x1):
            for j in range (k, x2):
                delta = x_inv[i] - x_prod_sen[j]
                if abs(delta < 10000):
                    y_perdite.append(y_inv[i]-y_prod_sen[j])
                    x_perdite.append(x_inv[i])
                    k=j 
                    break
        #ALTRI DATI
        valore_incentivo = 0.361 #€/kWh II Conto energia
        prezzo_zonale = 0.075 #€/kWh cap extraprofitti 2023
        pun = 0.137 #€/kWh in attesa accesso ftp GME - valore medio aprile 2023
        picco_potenza = max(y_inv)

        introito_incentivo = energia_contatore_produzione*valore_incentivo
        introito_vendita = energia_immessa_contatore_scambio*prezzo_zonale #da aggiornare con ftp gme + conteggio iterato
        spesa_acquisto_energia = energia_prelevata_contatore_scambio*pun #da aggiornare con dati ftp gme + conteggio iterato
        risparmio_mancato_acquisto_energia = energia_autoconsumata_stabilimento*pun-energia_autoconsumata_stabilimento*prezzo_zonale #da aggiornare con dati ftp gme + conteggio iterato

        #AGGIORNAMENTO DEL LIVE PLOT

        line1.set_xdata(x_inv)
        line1.set_ydata(y_inv)
        line2.set_xdata(x_prod_sen)
        line2.set_ydata(y_prod_sen)    
        line3.set_xdata(x_imm_sen)
        line3.set_ydata(y_imm_sen)
        line4.set_xdata(x_prod_sen)
        line4.set_ydata(autoconsumo)
        line5.set_xdata(x_prel_sen)
        line5.set_ydata(y_prel_sen)
        line6.set_xdata(x_prel_aux_sen)
        line6.set_ydata(y_prel_aux_sen)
        line7.set_xdata(x_perdite)
        line7.set_ydata(y_perdite)

        text1.set_text('Energia Lorda Prodotta: '+str("{:.2f}".format(energia_prodotta_inverter)+ ' kWh'))
        text2.set_text('Energia Netta Prodotta: '+str("{:.2f}".format(energia_contatore_produzione)+ ' kWh'))
        text3.set_text('Energia Immessa in Rete: '+str("{:.2f}".format(energia_immessa_contatore_scambio)+ ' kWh'))
        text4.set_text('Energia Autoconsumata: '+str("{:.2f}".format(energia_autoconsumata_stabilimento)+ ' kWh'))
        text5.set_text('Energia Prelevata da rete: '+str("{:.2f}".format(energia_prelevata_contatore_scambio)+ ' kWh'))
        text6.set_text('Energia Prelevata Aux: '+str("{:.2f}".format(energia_contatore_produzione_prelievo_aux)+ ' kWh'))
        text7.set_text('Perdite AC Impianto FV: '+str("{:.2f}".format(energia_perduta)+ ' kWh'))
        text8.set_text('Picco di Potenza: '+str("{:.2f}".format(picco_potenza)+ ' kW'))

        text9.set_text('Introito Incentivo: '+str("{:.2f}".format(introito_incentivo)+ ' €'))
        text10.set_text('Introito Vendita Energia: '+str("{:.2f}".format(introito_vendita)+ ' €'))
        text11.set_text('Spesa Acquisto Energia: '+str("{:.2f}".format(spesa_acquisto_energia)+ ' €'))
        text12.set_text('Risparmio Autoconsumo: '+str("{:.2f}".format(risparmio_mancato_acquisto_energia)+ ' €'))

        #creazione ticks asse x
        label_asse_x=[]
        xticks_space = round(len(x_prod_sen)/59+1)
        xticks_index = list(range(0, len(x_prod_sen), xticks_space))
        xticks = []
        for i in range(0, len(xticks_index)):
            time_msec = x_prod_sen[xticks_index[i]]
            xticks.append(time_msec)
            time_datetime = datetime.fromtimestamp(time_msec/1000.0)
            label_asse_x.append(str(time_datetime.hour).zfill(2)+':'+str(time_datetime.minute).zfill(2))

        ax1.set_xticks(xticks, labels = label_asse_x) # set new tick positions
        ax1.tick_params(axis='x', rotation=90) # set tick rotation

        ax1.set_xlim(xticks[0], xticks[-1])
        ax1.autoscale_view(True,True,True)
        ax2.autoscale_view(True,True,True)
        fig.canvas.draw()
        fig.canvas.flush_events()
        
        fig.savefig(export_path+'/'+today+'_report.png')
        
        time.sleep(300)
