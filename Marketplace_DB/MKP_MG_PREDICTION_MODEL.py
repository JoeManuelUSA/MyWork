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
def import_historic_MLB(cnn,mes_inicial, mes_final):#Imports MercadoLibre sales by month
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
def import_historic_WMT(cnn,mes_inicial, mes_final):#Imports Wallmart sales by month
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
def regression_lineal(lista_cantidad,rango_meses):#Linear Regression Model
    dia_prueba = []
    #Generate List
    for i in range(rango_meses):
        dia_prueba.append(i + 1)
    dia = np.array(dia_prueba)
    #Generate Sale List
    Px = np.array([item[2] for item in lista_cantidad])
    #LinearModel
    model = LinearRegression()
    model.fit(dia.reshape(-1, 1), Px)
    #Test List
    test = np.array([rango_meses + dias_predict])
    sum_predict = 0
    pronostico = []
    PL = model.predict(test_aux.reshape(-1, 1))
    PLfinal = float(np.round(PL, 2))
    predicciones_lineales = model.predict(dia.reshape(-1, 1))
    n = 0
    lista_lineal = []
    lista_lineal.append(predicciones_lineales)
    colors = ['blue' if item[2] == 0 else 'green' for item in lista_cantidad]
    scatter = plt.scatter(dia, Px, c=colors)
    future_days = np.array([i for i in range(rango_meses, rango_meses + dias)])
    future_sales = model.predict(future_days.reshape(-1, 1))
    #Generates Graph
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
    plt.show()
    return lista,fig_html
def modelo_no_lineal(x, a, b, c, d):#NonLinear Model
    return a * x ** 3 + b * x ** 2 + c * x + d  # Cubic
def regression_no_lineal(lista_cantidad,rango_meses):#NonLinear Model function that indicates if trend is upwards or downwards
    historico = np.array(([item[2] for item in lista_cantidad]))
    total = np.sum(historico)
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
            test = np.array([rango_meses + i])
            PL = modelo_no_lineal(test, *popt)
            PLfinal = float(np.round(PL, 2))
        test = np.array([rango_meses + dias_predict]) 
        #Checks if trend is upwards or downwards
        if PLfinal>=ventas[-1]:Tendencia=True#True if upwards
        if PLfinal< ventas[-1]: Tendencia = False#False if downwards
        PL = modelo_no_lineal(test, *popt)
        PLfinal = float(np.round(PL, 2))
        predicciones_no_lineales = modelo_no_lineal(mes, *popt)
        mse_no_lineal = mean_squared_error(ventas, predicciones_no_lineales)
    except Exception as e:
        test = np.array([rango_meses + dias_predict])
        PL = 0
        print(e)
    #generates graph
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
    plt.show()
    return Tendencia
mes_inicial=datetime(2023,1,1) #Start Date 
mes_final=datetime(2023,7,1)#End Date
rango_meses=6
dias = 2
dias_predict = dias - 1

result_general_MLB,MLB_Total=import_historic_MLB(cnn1,mes_inicial,mes_final)#Obtains MLB Sale Data by date 
Tendencia_MLB=regression_no_lineal(result_general_MLB,rango_meses)#Uses a Nonlinear model for forecast of sales
regression_lineal(result_general_MLB,rango_meses)#Uses a linear model for forecast of sales
print(Tendencia_MLB)#Will print if the trend is upwards or downwards

result_general_WMT,WMT_Total=import_historic_WMT(cnn2,mes_inicial,mes_final)#Obtains WMT Sale Data by date
Tendencia_WMT=regression_no_lineal(result_general_WMT,rango_meses)#Uses a Nonlinear model for forecast of sales
regression_lineal(result_general_WMT,rango_meses)#Uses a linear model for forecast of sales
print(Tendencia_WMT)#Will print if the trend is upwards or downwards
