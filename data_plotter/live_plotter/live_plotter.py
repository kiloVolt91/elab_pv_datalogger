from datetime import datetime
import time
import sqlalchemy
import math
from sqlalchemy import text
import os 
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import ticker
import matplotlib
from matplotlib import gridspec
from configurazione.init import *
import sys
import joblib
from sklearn.neural_network import MLPRegressor
import statistics

matplotlib.use("TkAgg")

## IMPORTAZIONE DEL MODELLO ANN - MLPREGRESSOR (SKLEARN) ADDESTRATO CON I DATI STORICI DELL'IMPIANTO
os.chdir(prevision_model_path)
modello_previsionale = joblib.load("modello_previsionale_fv_asi.pkl")
parametri_conversione_modello = joblib.load("parametri.pkl")

#######################################################################################################
## DEFINIZIONE DELLE FUNZIONI DI BASE E CREAZIONE DEI PRINCIPALI TOOL UTILIZZATI DALLE MACROFUNZIONI ##
#######################################################################################################


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
    
    df = pd.DataFrame()   
    try:
        df = pd.read_sql(query, connection)
    except Exception as error:
        print(error)
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
    query = "SELECT * FROM "+db_table+" WHERE "+db_col+" > '" +str(last_entry.iloc[0])+"'"+sec_query
    
    update_df = pd.DataFrame()
    try:
        update_df = pd.read_sql(query, connection)
    except Exception as error:
        print(error)
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
    
def get_vals_to_plot_from_seneca_mean(df, k, mins): ## mins = numero di minuti su cui effettuare la media; k = costante di lettura del contatore
    numero_di_valori = df.shape[0]
    energia = df['converted_value'].tolist()
    tempo = df['msecs'].tolist()
    potenza_elettrica = []
    intervallo_temporale = []
    for i in range (mins, numero_di_valori):
        delta_energia = k*(energia[i]-energia[i-mins])
        delta_tempo = (tempo[i]-tempo[i-mins])/3600000
        potenza_elettrica.append(delta_energia/delta_tempo)
        intervallo_temporale.append(tempo[i])
    return (intervallo_temporale, potenza_elettrica)

def ottieni_prezzo_orario(prezzo):
    data_oggi = datetime.now().strftime('%Y%m%d')
    ora = datetime.now().strftime('%H')
    url = 'mysql+pymysql://'+user_inverter+':'+password_inverter+'@'+host_inverter+'/'+db_gme
    engine = sqlalchemy.create_engine(url)
    connection = engine.connect()
    query = "SELECT "+str(prezzo)+" FROM "+str(db_gme)+"."+str(db_table_gme)+" WHERE "+str(db_colonna_giorno_gme)+" = "+str(data_oggi)+" AND "+str(db_colonna_ora_gme)+" = "+str(ora)+";"
    cursor = connection.execute(text(query))
    result = cursor.fetchall()
    return (result[0][0]/1000)

def get_gme_dataframe_from_database(user, password, host, db, db_table, db_col):
    now = datetime.now()
    today = datetime.strftime(now, '%Y%m%d')
    url = 'mysql+pymysql://'+user+':'+password+'@'+host+'/'+db
    connection = sqlalchemy.create_engine(url)
    query = "SELECT * FROM "+str(db_table)+" WHERE "+str(db_col)+" = '" +str(today)+"'"
    df = pd.read_sql(query, connection)
    return(df)

def esporta_csv(df, nome_file, export_path):
    export_path = export_path +'/'+ nome_file + '.csv'
    df.to_csv(export_path, index=False, decimal =',', sep=';')
    return

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

def normalizza_valore(valore, parametro):
    valore_norm = ((valore - parametro[1])/(parametro[0]-parametro[1]))
    return (valore_norm)

def denormalizzazione_valore(valore, parametro):
    valore_denorm = (valore*(parametro[0]-parametro[1])+parametro[1])
    return(valore_denorm)

####################################################################
## DEFINIZIONE DELLE MACROFUNZIONI E DELLE PRINCIPALI OPERAZIONI ##
####################################################################


def inizializzazione_grafico ():
    global fig, gs, ax0, ax1, ax2, ax3, leg0, leg1, leg3
    global line1,line2,line3, line4, line5, line6, line7, line8, line9, line10
    global text1, text2, text3, text4, text5, text6, text7, text8, text9, text10, text11, text12, text13, text14, text15, text16, text17, text18

    plt.ion() ## ATTIVAZIONE DELLA MODALITA' "LIVE" 
    # INIZIALIZZAZIONE FIGURA 
    fig = plt.figure(figsize=(25,25))
    gs = gridspec.GridSpec(3,3, height_ratios=[0.45, 1, 0.25])
    ax0 = fig.add_subplot(gs[0,:2])
    ax1 = fig.add_subplot(gs[1, :])
    ax2 = fig.add_subplot(gs[2, :])
    ax3 = ax0.twinx()
    ax4 = fig.add_subplot(gs[0, 2])

    # INIZIALIZZAZIONE DELLE CURVE
    x = 0
    y = 0
    line1,= ax1.plot(x, y, 'b', label ='Potenza lorda prodotta') #OUTPUT INVERTER
    line2,= ax1.plot(x, y, 'c', label='Potenza netta prodotta') #ENERGIA PRODOTTA - CONTATORE PROD
    line3,= ax1.plot(x, y, 'purple', label = 'Potenza immessa in rete') #ENERGIA IMMESSA IN RETE - CONTATORE SCAMBIO
    line4,= ax1.plot(x, y, 'g', label = 'Potenza autoconsumata') #AUTOCONSUMO AZIENDA - PRODOTTA-IMMESSA
    line5,= ax1.plot(x, y, 'orange', label='Potenza prelevata da rete - Stabilimento') #PRELIEVO DA RETE ENEL - CONTATORE SCAMBIO
    line6,= ax1.plot(x, y, 'yellow', label ='Potenza prelevata da rete - Ausiliari produzione') #PRELIEVO PER CONSUMO AUSILIARI 
    line7,= ax1.plot(x, y, 'red', label = 'Perdite ausiliari produzione') #PERDITE TRASFORMATORE+AUX
    line8,= ax0.plot(x, y, 'green', label = 'Potenza Attiva Reale') #POTENZA ELETTRICA PRODOTTA DALL'IMPIANTO
    line9,= ax0.plot(x, y, 'red', label = 'Potenza Attiva Stimata') #P0TENZA ELETTRICA PREVISTA DAL MODELLO ANN
    line10, = ax3.plot(x, y, 'purple', label = 'Indice Prestazionale') #PRESTAZIONI IMPIANTISTICHE IN RELAZIONE AI DATI STORICI DEL MODELLO ANN IMPLEMENTATO
    
    # INIZIALIZZAZIONE ASSI E LEGENDA
    ax0.yaxis.set_major_locator(ticker.LinearLocator(10))
    leg0 = ax0.legend(loc='upper left', fontsize = 'xx-small')
    ax0.set_xlabel("Orario di Acquisizione [HH:MM]", fontsize = 'xx-small')
    ax0.set_ylabel("Potenza elettrica [kW]", fontsize = 'small')
    ax0.grid()
    
    leg1 = ax1.legend(fontsize = 'small');
    ax1.grid()
    ax1.set_ylabel("Potenza elettrica [kW]", fontsize = 'small')
    
    ax2.axis([0, 72.5, 0, 20])
    ax2.set_xticks([])
    ax2.set_yticks([])
    
    leg3 = ax3.legend(loc='center left', fontsize = 'xx-small')
    ax3.set_ylabel("Indice Prestazionale")
    
    ax4.xaxis.set_major_locator(ticker.NullLocator())
    ax4.yaxis.set_major_locator(ticker.NullLocator())
    
    fig.suptitle("Giorno: "+str(now)[0:11], fontsize=16)
    
    #INIZIALIZZAZIONE TESTI
    text1=ax2.text(1.5,9.25, 'Energia Lorda Prodotta: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'b', 'alpha': 0.5, 'pad': 6})
    text2=ax2.text(1.5,2.5, 'Energia Netta Prodotta: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'c', 'alpha': 0.5, 'pad': 6})
    text3=ax2.text(19.5,9.25, 'Energia Immessa in Rete: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'purple', 'alpha': 0.5, 'pad': 6})
    text4=ax2.text(19.5,2.5, 'Energia Autoconsumata: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'g', 'alpha': 0.5, 'pad': 6})
    text5=ax2.text(37.5,9.25, 'Energia Prelevata da Rete: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'orange', 'alpha': 0.5, 'pad': 6})
    text6=ax2.text(37.5,2.5, 'Energia Prelevata Aux: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'yellow', 'alpha': 0.5, 'pad': 6})
    text7=ax2.text(55.5,9.25, 'Perdite AC Impianto FV: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'red', 'alpha': 0.5, 'pad': 6})
    text8=ax2.text(55.5,2.5, 'Picco di Potenza: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'magenta', 'alpha': 0.5, 'pad': 6})
    text9=ax2.text(1.5,16, 'Introito Incentivo: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 6})
    text10=ax2.text(19.5,16, 'Introito Vendita Energia: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 6})
    text11=ax2.text(37.5,16, 'Spesa Acquisto Energia: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 6})
    text12=ax2.text(55.5,16, 'Risparmio Autoconsumo: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 6})
    
    text13=ax4.text(0.1,0.1, 'Potenza Attiva Misurata: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'lightgreen', 'alpha': 0.5, 'pad': 4.2})
    text14=ax4.text(0.1,0.25, 'Potenza Attiva Stimata: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'orangered', 'alpha': 0.5, 'pad': 4.2})
    text15=ax4.text(0.1,0.4, 'Irraggiamento: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'gold', 'alpha': 0.5, 'pad': 4.2})
    text16=ax4.text(0.1,0.55, 'Temperatura Media Pannello: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'lightskyblue', 'alpha': 0.5, 'pad': 4.2})
    text17=ax4.text(0.1,0.7, 'Indice di Prestazione: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'purple', 'alpha': 0.5, 'pad': 4.2})
    text18=ax4.text(0.1,0.85, 'Ora misure [HH:MM]: '+str(0)+ ' %', style='italic', size='medium', weight='bold', bbox={'facecolor': 'silver', 'alpha': 0.5, 'pad': 4.2})
    return 

def calcolo_contatori_energetici_ed_economicici():
    global energia_immessa_contatore_scambio, energia_prelevata_contatore_scambio, energia_autoconsumata_stabilimento, energia_contatore_produzione, picco_potenza
    global introito_vendita, spesa_acquisto_energia, risparmio_mancato_acquisto_energia, introito_incentivo
    global energia_contatore_produzione_prelievo_aux, energia_prodotta_inverter, energia_perduta
    
    ## INIZIALIZZAZIONE PARAMETRI
    picco_potenza = max(y_inv)
    valore_incentivo = 0.361 #€/kWh II Conto energia
    energia_immessa = [0]
    energia_prelevata = [0]
    energia_autoconsumata = [0]
    energia_prodotta = [0]
    vendita_energia = [0]
    acquisto_energia = [0]
    risparmio_energetico = [0]
    orario_precedente = 0
   
    ## INIZIALIZZAZIONE ORARI DI RIFERIMENTO
    today_gme = datetime.strftime ( now, '%Y%m%d')
    ora_corrente = int(datetime.strftime ( now, '%H'))
    orario_precedente = today_gme +' '+  str(orario_precedente).zfill(2) +':00'
    orario_precedente = datetime.strptime(orario_precedente, '%Y%m%d %H:%M')
    orario_precedente = datetime.timestamp(orario_precedente)*1000    

    df_debug = pd.DataFrame() ## INIZIALIZZAZIONE DATAFRAME DI DEBUGGING - POSSIBILE COMMENTARE LA SEZIONE

    ##ESTRAPOLAZIONE DEI PREZZI ORARI
    for ora in range (1, ora_corrente+1):  ##ATTENZIONARE DURANTE TRANSIZIONE ORA LEGALE - ORA SOLARE
        mercato_vendita = 'SICI'
        pun = 'PUN'
        prezzo_vendita_energia = ((df_day_gme.loc[df_day_gme['Ora'] == ora, [mercato_vendita]]).iloc[0])[0]/1000
        prezzo_acquisto_energia = ((df_day_gme.loc[df_day_gme['Ora'] == ora, [pun]]).iloc[0])[0]/1000
        
        ## CONVERSIONE DELL'ORA "GME" IN UN VALORE TIMESTAMP UTILIZZABILE PER I CONFRONTI CON I DATI PRESENTI SUI DATAFRAMES 
        if str(ora-1+2).zfill(2) == '24':
            selezione_ora == today_gme +' '+'23:59'
        else:
            selezione_ora = today_gme +' '+  str(ora-1+2).zfill(2) +':00' ##N.B. +2 VALORE GMT LOCALE
        orario_di_comparazione = datetime.strptime(selezione_ora, '%Y%m%d %H:%M')
        orario_attuale_msecs = datetime.timestamp(orario_di_comparazione)*1000
        
        ## ESTRAZIONE DEI DATAFRAME ORARI PER L'ORA DI RIFERIMENTO
        energia_oraria_immessa = df_day_seneca_imm.loc[(df_day_seneca_imm['msecs'] <= orario_attuale_msecs+60000) & (df_day_seneca_imm['msecs'] > orario_precedente)]
        energia_oraria_prelevata = df_day_seneca_prel.loc[(df_day_seneca_prel['msecs'] <= orario_attuale_msecs+60000) & (df_day_seneca_prel['msecs'] > orario_precedente)]
        energia_oraria_prodotta = df_day_seneca_prod.loc[(df_day_seneca_prod['msecs'] <= orario_attuale_msecs+60000) & (df_day_seneca_prod['msecs'] > orario_precedente)]
        orario_precedente = orario_attuale_msecs
        
        ## CONTATORE DELL'ENERGIA ELETTRICA IMMESSA IN RETE E DELLA RELATIVA VENDITA AL PREZZO ZONALE
        if energia_oraria_immessa['value'].tolist():
            delta_energia_immessa = energia_oraria_immessa['value'].tail(1).tolist()[0]-energia_oraria_immessa['value'].head(1).tolist()[0]
            energia_immessa[0] = energia_immessa[0]+delta_energia_immessa
            vendita_energia[0] = vendita_energia[0]+delta_energia_immessa*prezzo_vendita_energia 
            introito_vendita = vendita_energia[0]
            energia_immessa_contatore_scambio=energia_immessa[0]

        ## CONTATORE DELL'ENERGIA ELETTRICA PRELEVATA DA RETE E DEL RELATIVO ACQUISTO AL PREZZO UNICO NAZIONALE
        if energia_oraria_prelevata['value'].tolist():
            delta_energia_prelevata = energia_oraria_prelevata['value'].tail(1).tolist()[0]-energia_oraria_prelevata['value'].head(1).tolist()[0]
            energia_prelevata[0] = energia_prelevata[0]+delta_energia_prelevata
            acquisto_energia[0] = acquisto_energia[0]+delta_energia_prelevata*prezzo_acquisto_energia
            spesa_acquisto_energia = acquisto_energia[0] 
            energia_prelevata_contatore_scambio=energia_prelevata[0]
        
        ## CONTATORE DELL'ENERGIA ELETTRICA PRODOTTA -- modificato 28/11
        if energia_oraria_prodotta['converted_value'].tolist():
            delta_energia_prodotta = energia_oraria_prodotta['converted_value'].tail(1).tolist()[0]-energia_oraria_prodotta['converted_value'].head(1).tolist()[0]
            energia_prodotta[0] = energia_prodotta[0]+delta_energia_prodotta
            introito_incentivo = energia_prodotta[0]*valore_incentivo
            energia_contatore_produzione = energia_prodotta[0]
        
        ## CONTATORE DELL'ENERGIA ELETTRICA AUTOCONSUMATA
        try:
            delta_energia_autoconsumata = delta_energia_prodotta - delta_energia_immessa
            energia_autoconsumata[0] = energia_autoconsumata[0]+delta_energia_autoconsumata
            energia_autoconsumata_stabilimento = energia_autoconsumata[0]
        
        ## CONTATORE DELLA CONVENIENZA "TEORICA" DELL'AUTOCONSUMO - AL NETTO DEGLI ONERI DI SISTEMA E PERDITE DI RETE
            risparmio_energetico[0] = risparmio_energetico[0]+delta_energia_autoconsumata*prezzo_acquisto_energia-delta_energia_autoconsumata*prezzo_vendita_energia
            risparmio_mancato_acquisto_energia = risparmio_energetico[0]

            ## ALTRI CONTATORI
            energia_contatore_produzione_prelievo_aux = get_daily_energy_value_from_df(df_day_seneca_prel_aux, now, 'converted_value', 1)
            energia_prodotta_inverter = get_daily_energy_value_from_df(df_day_inverter, now, '0_E_AC', 1)
            energia_perduta = energia_prodotta_inverter - energia_contatore_produzione
        except err as Error:
            print(err)
            
    # DATAFRAME DI DEBUGGING - COMMENTARE 
        df_recap = pd.DataFrame ({
            "ora": [ora],
            "energia_immessa": energia_immessa,
            "vendita_energia": vendita_energia,
            "energia_prelevata":energia_prelevata,
            "acquisto_energia":acquisto_energia,
            "energia_prodotta":energia_prodotta
                                  })
        df_debug = pd.concat([df_debug, df_recap], axis = 0)  
    esporta_csv(df_debug, 'df_debug', export_path)
    return 

def aggiornamento_grafici():
    global label_asse_x, xticks
    #AGGIORNAMENTO DELLE CURVE
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
    line8.set_xdata(x_inv)
    line8.set_ydata(y_inv)
    line9.set_xdata(x_inv)
    line9.set_ydata(prev_data)
    line10.set_xdata(x_inv)
    line10.set_ydata(performance_index)
    
    #AGGIORNAMENTO DEI LABELS E DEGLI ASSI
    label_asse_x=[]
    tick_pos = [i for i in range(int(x_prod_sen[0]),  int(x_prod_sen[-2]), 1800000)] 
    tick_pos.append(x_prod_sen[-1])
    for i in range(0, len(tick_pos)):
        time_msec = tick_pos[i]
        time_datetime = datetime.fromtimestamp(time_msec/1000.0)
        label_asse_x.append(str(time_datetime.hour).zfill(2)+':'+str(time_datetime.minute).zfill(2))   
    
    ax1.xaxis.set_major_locator(ticker.FixedLocator([tick_pos])) 
    ax1.set_xticks(tick_pos, labels = label_asse_x) 
    ax1.tick_params(axis='x', rotation=90, labelsize='xx-small')
    ax1.set_xlim(tick_pos[0], tick_pos[-1])
    ax1.set_ylim(0,max([max(y_inv),max(y_prod_sen)])+10)
    ax1.autoscale_view(True,True,True)
    ax2.autoscale_view(True,True,True)
    
    label_asse_x0=[]
    tick_pos = [i for i in range(int(x_inv[0]),  int(x_inv[-2]), 900000)]  
    tick_pos.append(x_inv[-1])
    for i in range(0, len(tick_pos)):
        time_msec = tick_pos[i]
        time_datetime = datetime.fromtimestamp(time_msec/1000.0)
        label_asse_x0.append(str(time_datetime.hour).zfill(2)+':'+str(time_datetime.minute).zfill(2))
    ax0.xaxis.set_major_locator(ticker.FixedLocator([tick_pos]))     
    ax0.set_xticks(tick_pos, labels = label_asse_x0) 
    ax0.tick_params(axis='x', rotation=0, labelsize='xx-small')
    ax0.set_xlim(tick_pos[0], tick_pos[-1])
    ax0.set_ylim(0,max(y_inv)+10)
    ax0.autoscale_view(True,True,True)
    
    ax3.set_ylim(0,max(performance_index)+0.1 )
    ax3.autoscale_view(True,True,True)

    ## AGGIORNAMENTO DEI TESTI
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
    text13.set_text('Potenza Attiva Misurata: '+str("{:.2f}".format(y_inv[-1])+ ' kW'))
    text14.set_text('Potenza Attiva Stimata: '+str("{:.2f}".format(prev_data[-1].tolist()[0])+ ' kW'))
    text15.set_text('Irraggiamento: '+str("{:.2f}".format(irr_list[-1])+ ' W/mq'))
    text16.set_text('Temperatura Media Pannello: '+str("{:.2f}".format(temp_list[-1])+ ' °C'))
    text17.set_text('Indice di Prestazione: '+str("{:.2f}".format(performance_index[-1].tolist()[0])))
    text18.set_text('Ora misure [HH:MM]: '+str("{}".format(label_asse_x0[-1])))
    
    ## AGGIORNAMENTO DELLA FIGURA "LIVE"
    fig.canvas.draw()
    fig.canvas.flush_events()
    return

def estrapolazione_vettori_coordinate_cartesiane():
    global x_prod_sen, y_prod_sen, x_imm_sen, y_imm_sen, x_prel_sen, y_prel_sen, x_prel_aux_sen, y_prel_aux_sen, y_perdite, x_perdite, y_inv, x_inv, autoconsumo, prev_data, performance_index
    global irr_list, temp_list
    ## VETTORE X-Y INVERTER - PRODUZIONE LORDA DI ENERGIA ELETTRICA OGNI 5 MINUTI
    len_x_inv = df_day_inverter.shape[0]
    y_inv = df_day_inverter['0_P_AC'].tolist()
    time_inv = df_day_inverter['0_Time'].tolist()
    x_inv =[]
    utc_offset = time.localtime().tm_gmtoff
    for i in range (0,len(time_inv)):
        x_inv.append(time_inv[i].timestamp()*1000-utc_offset*1000)

    ## VETTORI X-Y ENERGIA ELETTRICA PRODOTTA, IMMESSA, PRELEVATA E PRELEVATA DAGLI AUX
    ## VALORI MEDIATI IN 5 MINUTI PER RENDERE OMOGENEA LA RAPPRESENTAZIONE GRAFICA CON IL VETTORE X-Y DELL'INVERTER
    mins = 5
    x_prod_sen, y_prod_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prod, 1, mins)
    x_imm_sen, y_imm_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_imm, 800, mins)
    x_prel_sen, y_prel_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prel, 800, mins)
    x_prel_aux_sen, y_prel_aux_sen = get_vals_to_plot_from_seneca_mean(df_day_seneca_prel_aux, 1, mins)

    ## CURVA POTENZA ENERGIA ELETTRICA AUTOCONSUMATA
    x = x_prod_sen
    autoconsumo =[]
    perdite_aux = []
    for i in range (0, len(x)):
        autoconsumo.append( y_prod_sen[i] - y_imm_sen[i])
        
    ##DATI PREVISIONALI MODELLO ANN
    colonna_temperature, colonna_stringhe = estrazione_colonne_temp_string(df_day_inverter)
    irr_list = df_day_inverter['0_Irr'].tolist() 
    temp_list = df_day_inverter[colonna_temperature].mean(axis=1).tolist() 
    
    prev_data=[]  
    performance_index = []
    for i in range (0, len(y_inv)):
        irr_val_norm = normalizza_valore(irr_list[i], parametri_conversione_modello['0_Irr'])
        temp_val_norm = normalizza_valore(temp_list[i], parametri_conversione_modello['avarage_temps'])
        
        data_pack = np.array([irr_val_norm, temp_val_norm]).reshape(1,-1)
        prevision_norm = modello_previsionale.predict(data_pack)
        prevision_denorm = denormalizzazione_valore(prevision_norm, parametri_conversione_modello['0_P_AC'])
        prev_data.append(prevision_denorm)
        if prevision_denorm >1:
            performance_index.append(y_inv[i]/prevision_denorm)
        else:
            performance_index.append(-0.01)

    ## CURVA DELLE PERDITE DEGLI AUSILIARI DELL'IMPIANTO FV (TRASFORMATORE ISOLAMENTO, CLIMATIZZAZIONE, SERVIZI ANCILLARI)
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
    return

def esportazione_csv_riepilogo():
    esporta_csv(df_day_inverter, 'df_day_inverter', export_path)
    esporta_csv(df_day_seneca_prod, 'df_day_seneca_prod', export_path)
    esporta_csv(df_day_seneca_prel_aux, 'df_day_seneca_prel_aux', export_path)
    esporta_csv(df_day_seneca_imm, 'df_day_seneca_imm', export_path)
    esporta_csv(df_day_seneca_prel, 'df_day_seneca_prel', export_path)
    esporta_csv(df_day_gme, 'df_day_gme', export_path)
    return 

def aggiornamento_datarames():
    global df_day_inverter, df_day_seneca_prod, df_day_seneca_prel_aux, df_day_seneca_imm, df_day_seneca_prel

    df_day_inverter = update_dataframe_with_new_values_from_db(user_inverter, password_inverter, host_inverter, db_inverter, now, 'none', db_table_inverter, db_colonna_temporale_inverter, df_day_inverter)
    last_entry_time = df_day_inverter['0_Time'].tail(1).iloc[-1]
    df_day_seneca_prod = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 24, db_table_seneca, db_col_seneca, df_day_seneca_prod)
    df_day_seneca_prel_aux = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 23, db_table_seneca, db_col_seneca, df_day_seneca_prel_aux)
    df_day_seneca_imm = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 11, db_table_seneca, db_col_seneca, df_day_seneca_imm)
    df_day_seneca_prel = update_dataframe_with_new_values_from_db(user, password, host_seneca, db_seneca, last_entry_time, 12, db_table_seneca, db_col_seneca, df_day_seneca_prel)
    return

def estrazione_dei_dataframes_dal_database():
    global df_day_inverter, df_day_seneca_prod, df_day_seneca_prel_aux, df_day_seneca_imm, df_day_seneca_prel, df_day_gme, now
    
    df_day_inverter = get_dataframe_from_database(user_inverter, password_inverter, host_inverter, db_inverter, now, 'none', db_table_inverter, db_colonna_temporale_inverter,gmt = 0)
    
    ## DURANTE LA NOTTE L'INVERTER ENTRA IN STANDBY E NON SI HANNO DATI DA RAPPRESENTARE
    while df_day_inverter.empty==True:
        time.sleep(300) 
        now = datetime.now()
        df_day_inverter = get_dataframe_from_database(user_inverter, password_inverter, host_inverter, db_inverter, now, 'none', db_table_inverter, db_colonna_temporale_inverter,0)   
        print('dataframe vuoto', now)
        
    last_entry_time = df_day_inverter['0_Time'].tail(1).iloc[-1]
    gmt = 1 ## ORA LEGALE: GMT +2, ORA SOLARE GMT +1
    df_day_seneca_prod = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 24, db_table_seneca, db_col_seneca,2)
    df_day_seneca_prel_aux = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 23, db_table_seneca, db_col_seneca,2)
    df_day_seneca_imm = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 11, db_table_seneca, db_col_seneca,2)
    df_day_seneca_prel = get_dataframe_from_database(user, password, host_seneca, db_seneca, last_entry_time, 12, db_table_seneca, db_col_seneca,2)
    df_day_gme = get_gme_dataframe_from_database(user_inverter, password_inverter, host_inverter, db_gme, db_table_gme, db_colonna_giorno_gme)
    return

#########################################
## FUNZIONE DI LIVE PLOTTING DEI DATI  ##
#########################################

def live_plotter():
    global now
    now = datetime.now()
    today = str(now)[0:11]
    while True:
        if today[0:11] != str(now)[0:11]: 
            plt.close()
            today = str(now)[0:11]
        inizializzazione_grafico ()
        estrazione_dei_dataframes_dal_database()
        while today[0:11] == str(now)[0:11]:  
            now = datetime.now()
            print('ASI liveplotter - Nuovo inserimento dati, ora: ', str(now.hour).zfill(2)+':'+str(now.minute).zfill(2))
            aggiornamento_datarames()
            esportazione_csv_riepilogo()   
            estrapolazione_vettori_coordinate_cartesiane()
            calcolo_contatori_energetici_ed_economicici()
            aggiornamento_grafici()
            fig.savefig(export_path+'/'+today+'_report.png')
            time.sleep(300)
    return

#########################################
##           MAIN PROGRAM              ##
#########################################

while True:
    try:
        live_plotter()
    except Exception as error:
        print(datetime.now())
        print('Errore :'+ str(error))
    
