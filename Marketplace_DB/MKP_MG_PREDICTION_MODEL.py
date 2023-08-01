#This code uses a prediction model , and forecasts if sales are trending upwards or downwards
#Uses a Linear and Non Linear Model
#The sale data is from Wallmart and MercadoLibre and is obtained from MongoDB
from datetime import datetime, timedelta
from pymongo import MongoClient
from plotly.io import to_html
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import curve_fit
import warnings
import mplcursors
import plotly.graph_objects as go
from dotenv import load_dotenv, set_key
import os
#Load .env file
load_dotenv()
#Obtain Mongo Connection String
MongoOnlineConnection=os.environ.get('MongoOnlineConnectionString')
client = MongoClient(MongoOnlineConnection)
cnn1 = client["GAON_MLB"]
cnn2= client['GAON_WMT']
def import_historic_MLB(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {
                "$and": [
                    {"Status": "paid"},
                    {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
                ]
            }
        },
        # Second stage: group by month and sum up the quantities
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$Date_Created"},
                    "month": {"$month": "$Date_Created"},
                },
                "Ventas_Totales": {"$sum": "$Final_Amount"},

            }
        },
        {
            "$sort": {"_id.month": 1}
        }
    ]
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    for doc in result_month:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        total_quantity = doc["Ventas_Totales"]
        a += total_quantity
        results_list.append((year, month, total_quantity))
    return results_list, a
def import_historic_WMT(cnn,mes_inicial, mes_final):
    collection = cnn["Sale_Detail_Package"]
    pipeline_month = [
        # First stage: filter the documents
        {
            "$match": {"Date_Created": {"$gte": mes_inicial, "$lte": mes_final}}
#
        },
        # Second stage: group by month and sum up the quantities
        {
            "$group": {
                "_id": {
                    "year": {"$year": "$Date_Created"},
                    "month": {"$month": "$Date_Created"},
                },
                "Ventas_Totales": {"$sum": "$Total_Paid"}
            }
        },
        {
            "$sort": {"_id.month": 1}
        }
    ]
    result_month = collection.aggregate(pipeline_month)
    results_list = []
    a = 0
    for doc in result_month:
        year = doc["_id"]["year"]
        month = doc["_id"]["month"]
        total_quantity = doc["Ventas_Totales"]
        a += total_quantity
        results_list.append((year, month, total_quantity))
    return results_list, a
def regression_lineal(lista_cantidad,rango_meses):
    #print("El promedio final del historico es: " + str(promedio))
    dia_prueba = []
    for i in range(rango_meses):
        dia_prueba.append(i + 1)
    dia = np.array(dia_prueba)
    Px = np.array([item[2] for item in lista_cantidad])
    model = LinearRegression()
    model.fit(dia.reshape(-1, 1), Px)
    test = np.array([rango_meses + dias_predict])
    sum_predict = 0
    sum_real = 0
    pronostico = []
    for aux in range(dias):
        test_aux = np.array([rango_meses + aux])
        PL = model.predict(test_aux.reshape(-1, 1))
        PLfinal = float(np.round(PL, 2))
        print(f"Pronóstico Lineal de {aux + rango_meses + 1} dias: " + str(PLfinal))
        pronostico.append(PLfinal)
        sum_predict += PLfinal
    test_aux = np.array([rango_meses])
    PL = model.predict(test_aux.reshape(-1, 1))
    PLfinal = float(np.round(PL, 2))
    print(f"Pronostico total de {dias} dias: ", sum_predict)
    print(f"Ventas Real de {dias} dias: ", sum_real)
    print(f"Pronóstico Lineal de {rango_meses + 1} dias: " + str(PLfinal))
    # Promedio de 21 dias
    SumPx = np.sum(Px)
    #print(SumPx)
    promPx = SumPx / rango_meses
    # promPx = SumPx / 21
    print(f"Promedio de {rango_meses} dias: " + str(promPx))
    # CP_prueba=promPx/PLfinal
    # print(CP_prueba)
    # Cambio Porcentual
    # CP = int(np.round(promPx / PLfinal))
    # print("Cambio Porcentual:", CP)
    try:
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        CP = np.round(promPx / PLfinal)
        porcentajeCP = CP * 0.3
        print("Cambio Porcentual:", CP)
        print("Porcentaje:", porcentajeCP)
        if PLfinal < 0 and CP > -40:
            print('Baja')
        elif CP > 40:
            print('Alta')
        elif CP > 0 or CP < 40:
            print('Media')
        else:
            print('Media alta')
    except Exception as e:
        print(f"No hay ventas en los ultimos {rango_meses} dias")
        print("Error:", e)
    predicciones_lineales = model.predict(dia.reshape(-1, 1))
    n = 0
    lista_lineal = []
    lista_lineal.append(predicciones_lineales)
    colors = ['blue' if item[2] == 0 else 'green' for item in lista_cantidad]
    scatter = plt.scatter(dia, Px, c=colors)
    future_days = np.array([i for i in range(rango_meses, rango_meses + dias)])
    future_sales = model.predict(future_days.reshape(-1, 1))
    plt.scatter(dia, Px, c=colors)
    plt.plot(dia, model.predict(dia.reshape(-1, 1)), color='red')
    plt.scatter(future_days, future_sales, color='red', linestyle='dashed')
    scatter_predict = plt.scatter(future_days, future_sales, color='cyan', linestyle='dashed')
    cursor = mplcursors.cursor(scatter_predict, hover=True)
    cursor.connect("add", lambda sel: sel.annotation.set_text(
        'Dia: {}, Ventas_Predict: {:.2f}'.format(sel.target[0] + 1, sel.target[1])))
    cursor = mplcursors.cursor(scatter, hover=True)
    cursor.connect("add", lambda sel: sel.annotation.set_text(
        'Dia: {}, Ventas: {}'.format(sel.target[0], sel.target[1])))
    plt.title(f"Datos de entrenamiento y Regresión lineal ")
    plt.xlabel('Dia')
    plt.ylabel('Ventas')

    lista = [Px, future_sales]
    fig = go.Figure()
    # Scatter plot
    fig.add_trace(go.Scatter(x=dia, y=Px, mode='markers',
                             marker=dict(color=colors),
                             name='Real data',hovertemplate='Day: %{x}<br>Sales: %{y}<br><extra></extra>'))
    # Line plot
    fig.add_trace(go.Scatter(x=dia, y=model.predict(dia.reshape(-1, 1)),
                             mode='lines', name='Regression line',
                             line=dict(color='red')))
    # Predicted future sales
    fig.add_trace(go.Scatter(x=future_days, y=future_sales,
                             mode='lines+markers', name='Future sales',
                             line=dict(color='cyan', dash='dash'),hovertemplate='Day: %{x}<br>Predicted Sales: %{y}<extra></extra>'))
    print("Modelo_LinealGrafica")
    print("Grafica_Ventas Reales")
    print(dia,Px)
    print("Grafica_regression_lineal")
    print(dia, model.predict(dia.reshape(-1,1)))
    print("Grafica_futuras ventas")
    print(future_days, future_sales)
    # Title and axis labels
    fig.update_layout(
        title={
            'text': f"Datos de entrenamiento y Regresión lineal ",
            'x': 0.5,  # Set the x-position of the title to the middle of the graph
            'y': 0.95,  # Set the y-position of the title above the graph
            'xanchor': 'center',  # Set the x-anchor of the title to the center
            'yanchor': 'top',  # Set the y-anchor of the title to the top
            'font': {'size': 18}  # Set the font size of the title
        }
        ,
        xaxis_title='Dia',
        yaxis_title='Ventas',legend=dict(
            x=0,  # Set the x-position of the legend to the left
            y=1.1,  # Set the y-position of the legend above the graph
            orientation='h',  # Set the orientation to horizontal
            font=dict(
                size=13  # Set the font size of the legend to a small value, e.g., 8
            )
        ))
    # Save to HTML
    fig_html = to_html(fig, full_html=False)
    #fig.show()
    plt.show()
    # Now you can return the HTML string

    return lista,fig_html
def modelo_no_lineal(x, a, b, c, d):
    return a * x ** 3 + b * x ** 2 + c * x + d  # Cubic
def regression_no_lineal(lista_cantidad,rango_meses):
    historico = np.array(([item[2] for item in lista_cantidad]))
    total = np.sum(historico)
    #print(f"Total de {rango_dias} dias:", total)
    # Promedio final del historico
    promedio = total / len(([item[2] for item in lista_cantidad]))
    mes_prueba = []
    for i in range(rango_meses):
        mes_prueba.append(i + 1)
    mes = np.array(mes_prueba)
    ventas = np.array([item[2] for item in lista_cantidad])
    PLfinalsum = 0
    PLfinal_list = []
    real_suma = 0
    Tendencia=""
    try:
        popt, pcov = curve_fit(modelo_no_lineal, mes, ventas)
        for i in range(dias):
            test = np.array([rango_meses + i])  # dias_prediccion
            PL = modelo_no_lineal(test, *popt)
            PLfinal = float(np.round(PL, 2))
            PLfinal_list.append(PLfinal)
            PLfinalsum += PLfinal
            #print(f"Pronóstico no lineal de {i + 1} dias: ", PLfinal)
        #print(f"Pronostico total de ventas a {i + 1} dias: ", PLfinalsum)
        #print(f"Ventas Reales de {i + 1} dias: ", real_suma)
        test = np.array([rango_meses + dias_predict])  # dias_prediccion
        if PLfinal>=ventas[-1]:Tendencia=True
        if PLfinal< ventas[-1]: Tendencia = False
        PL = modelo_no_lineal(test, *popt)
        PLfinal = float(np.round(PL, 2))
        #print(f"Pronóstico no lineal de {dias} dias: ", PLfinal)
        predicciones_no_lineales = modelo_no_lineal(mes, *popt)
        mse_no_lineal = mean_squared_error(ventas, predicciones_no_lineales)
        #print("MSE del modelo no lineal: ", mse_no_lineal)
    except Exception as e:
        test = np.array([rango_meses + dias_predict])  # dias_prediccion
        PL = 0
        print(e)
    # Promedio de 21 dias
    SumPx = np.sum(ventas)
    promedioProducto = SumPx / rango_meses
    #print(f"Promedio de {rango_meses} dias:", promedioProducto)
    """
    try:
        warnings.filterwarnings("ignore", category=RuntimeWarning)
        CP = int(np.round(promedioProducto / PLfinal))
        print("Cambio Porcentual:", CP)
        # Tendencia
        if PLfinal < 0 and CP > -40:
            print("Baja")
        elif CP > 40:
            print("Alta")
        elif CP > 0 or CP < 40:
            print("Media")
        else:
            print("Media alta")
    except Exception as e:
        print(f"No hay ventas en los ultimos {rango_meses} dias")
    """
        # print("Error:", e)
    fig, ax = plt.subplots()
    ax.plot(historico, label='Historico')
    colors = ['green' if item[2] == 1 else 'blue' for item in lista_cantidad]
    scatter_historico = ax.scatter(range(len(historico)), historico, c=colors)
    predictx = []
    predicty = []
    for i in range(len(historico)):
        ax.plot(i, historico[i], 'o', color=colors[i])
    # Add an invisible green data series for the legend
    ax.plot([], [], 'o', color='green', label='Campaign')
    if PLfinal_list:
        test_values = np.array([rango_meses + i for i in range(dias)])  # Array of days to predict
        for i, (t, pl) in enumerate(zip(test_values, PLfinal_list)):
            label = 'Pronóstico no lineal' if i == 0 else None
            ax.plot(t, pl, 'ro', label=label)
            predictx.append(t)
            predicty.append(pl)
    scatter_predictions = ax.scatter(predictx, predicty, color='red')

    ax.set_title(f'Historico y Pronostico no lineal {rango_meses} dias')

    ax.set_xlabel('Dias')
    ax.set_ylabel('Ventas')
    ax.legend()
    cursor_historico = mplcursors.cursor(scatter_historico, hover=True)
    cursor_historico.connect("add", lambda sel: sel.annotation.set_text(
        'Dia: {}, Ventas: {}'.format(sel.target[0], sel.target[1])))
    cursor_predictions = mplcursors.cursor(scatter_predictions, hover=True)
    cursor_predictions.connect("add", lambda sel: sel.annotation.set_text(
        'Dia: {}, Pronostico: {}'.format(sel.target[0], sel.target[1])))

    listas = [ventas, predicty]
    fig = go.Figure()
    # Real data scatter plot
    fig.add_trace(go.Scatter(x=mes-1, y=ventas, mode='markers',
                             marker=dict(color=colors),
                             name='Real data',hovertemplate='Day: %{x}<br>Sales: %{y}<br><extra></extra>'))
    # Non-linear model prediction
    predicciones_no_lineales = modelo_no_lineal(mes, *popt)
    fig.add_trace(go.Scatter(x=mes, y=predicciones_no_lineales,
                             mode='lines', name='Non-linear regression',
                             line=dict(color='red')))
    # Future predictions
    future_days = np.array([rango_meses + i for i in range(dias)])
    future_sales = modelo_no_lineal(future_days, *popt)
    fig.add_trace(go.Scatter(x=future_days, y=future_sales,
                             mode='lines+markers', name='Future sales',
                             line=dict(color='cyan', dash='dash'),hovertemplate='Day: %{x}<br>Predicted Sales: %{y}<extra></extra>'))
    # Title and axis labels
    fig.update_layout(
        title={
            'text': f"Datos de entrenamiento y Regresión no lineal ",
            'x': 0.5,  # Set the x-position of the title to the middle of the graph
            'y': 0.95,  # Set the y-position of the title above the graph
            'xanchor': 'center',  # Set the x-anchor of the title to the center
            'yanchor': 'top',  # Set the y-anchor of the title to the top
            'font': {'size': 18}  # Set the font size of the title
        },
        xaxis_title='Dias',
        yaxis_title='Ventas',legend=dict(
            x=0,  # Set the x-position of the legend to the left
            y=1.1,  # Set the y-position of the legend above the graph
            orientation='h',  # Set the orientation to horizontal
            font=dict(
                size=13  # Set the font size of the legend to a small value, e.g., 8
            )
        ))
    # Convert plotly figure to HTML
    fig_html = to_html(fig, full_html=False)
    #fig.show()
    plt.show()
    #return listas,fig_html
    return Tendencia
mes_inicial=datetime(2023,1,1)
mes_final=datetime(2023,7,1)
rango_meses=6
dias = 2
dias_predict = dias - 1
result_general_MLB,MLB_Total=importar_historico_MLB(cnn1,mes_inicial,mes_final)
result_general_WMT,WMT_Total=importar_historico_WMT(cnn2,mes_inicial,mes_final)
print(result_general_MLB)
print(MLB_Total)
Tendencia_MLB=regression_no_lineal(result_general_MLB,rango_meses)
print(Tendencia_MLB)
#regression_lineal(result_general_MLB,rango_meses)
Tendencia_WMT=regression_no_lineal(result_general_WMT,rango_meses)
print(Tendencia_WMT)
#regression_lineal(result_general_WMT,rango_meses)
print(result_general_WMT)
print(WMT_Total)