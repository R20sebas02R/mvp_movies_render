# Importamos librerias
from fastapi import FastAPI
import pandas as pd
import numpy as np
import json
import ast


#-------------------------------------------------------------------------------------------------------------------------------


# Instanciamos nuestra clase de la API
app = FastAPI()


#-------------------------------------------------------------------------------------------------------------------------------


# Cargamos los archivos
df_consultas = pd.read_csv(r'data/data_consultas.csv')
data_ml = pd.read_csv(r'data/data_ml.csv')


#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/cantidad_filmaciones_mes/{mes}')
def cantidad_filmaciones_mes(mes: str):
    
    '''Se ingresa el mes y la funcion retorna la cantidad de peliculas que se estrenaron ese mes historicamente'''
    
    # Creamos un diccionario auxiliar para que el usuario ingrese tranquilamente los meses en español
    meses = {
        'enero': 'January',
        'febrero': 'February',
        'marzo': 'March',
        'abril': 'April',
        'mayo': 'May',
        'junio': 'June',
        'julio': 'July',
        'agosto': 'August',
        'septiembre': 'September',
        'octubre': 'October',
        'noviembre': 'November',
        'diciembre': 'December'
    }
    
    # Convertimos el campo 'release_date' en formato datetime
    df_consultas['release_date'] = pd.to_datetime(df_consultas['release_date'], format='%Y-%m-%d')
    
    # Convertimos el mes ingresado a su equivalente en inglés
    mes_ingles = meses.get(mes.lower())
    
    if mes_ingles is None:
        return {'error': 'Mes no válido'}
    
    # Filtramos el dataframe con aquellas peliculas cuyo mes de lanzamiento coincida con el mes ingresado
    df_mes = df_consultas[df_consultas['release_date'].dt.strftime('%B') == mes_ingles]
    
    # Contamos la cantidad de peliculas que coinciden
    respuesta = len(df_mes)
    
    return {'mes': mes, 'cantidad': respuesta}


#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/cantidad_filmaciones_dia/{dia}')
def cantidad_filmaciones_dia(dia: str):
    
    '''Se ingresa el dia y la funcion retorna la cantidad de peliculas que se estrebaron ese dia historicamente'''
    
    # Creamos un diccionario de los días en español para evitar problemas de idioma
    dias = {
        'lunes': 'Monday',
        'martes': 'Tuesday',
        'miércoles': 'Wednesday',
        'jueves': 'Thursday',
        'viernes': 'Friday',
        'sábado': 'Saturday',
        'domingo': 'Sunday'
    }
    
    # Convertimos el campo 'release_date' en formato datetime
    df_consultas['release_date'] = pd.to_datetime(df_consultas['release_date'], format='%Y-%m-%d')
    
    # Convertimos el día ingresado a su equivalente en inglés
    dia_ingles = dias.get(dia.lower())
    
    if dia_ingles is None:
        return {'error': 'Día no válido'}
    
    # Filtramos el dataframe con aquellas películas cuyo día de lanzamiento coincida con el día ingresado
    df_dia = df_consultas[df_consultas['release_date'].dt.strftime('%A') == dia_ingles]
    
    # Contamos la cantidad de películas que coinciden
    respuesta = len(df_dia)

    return {'dia': dia, 'cantidad': respuesta}


#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/score_titulo/{titulo}')
def score_titulo(titulo: str):
    '''Se ingresa el título de una filmación esperando como respuesta el título, el año de estreno y el score'''
    
    # Convertir el título ingresado a minúsculas y eliminar espacios adicionales
    titulo = titulo.lower().strip()
    
    # Filtrar el DataFrame por el título de la filmación
    filtro_titulo = df_consultas['title'].str.lower().str.strip() == titulo
    pelicula = df_consultas[filtro_titulo]
    
    if len(pelicula) == 0:
        return "No se encontró la película."
    
    # Obtener el título, el año de estreno y el score de la película
    titulo = pelicula['title'].values[0]
    año_estreno = pelicula['release_year'].values[0]
    score = pelicula['popularity'].values[0]
    
    # Convertir el tipo de datos a nativo de Python
    titulo = str(titulo)
    año_estreno = int(año_estreno)
    score = float(score)

    return {'titulo': titulo, 'anio': año_estreno, 'popularidad': score}


#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/votos_titulo/{titulo}')
def votos_titulo(titulo:str):
    titulo_filtrar = titulo.lower()
    filtro_titulo = df_consultas['title'].str.lower() == titulo_filtrar
    pelicula = df_consultas[filtro_titulo]
    
    if len(pelicula) == 0:
        return "No se encontró la película."
    
    votos = pelicula['vote_count'].values[0]
    
    if votos < 2000:
        return "La película no cumple con la cantidad mínima de valoraciones."
    
    promedio = pelicula['vote_average'].values[0]
    
    titulo = pelicula['title'].values[0]
    año_estreno = pelicula['release_year'].values[0]

    return {
        'titulo': titulo,
        'anio': int(año_estreno),
        'voto_total': int(votos),
        'voto_promedio': float(promedio)
    }


#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/get_actor/{nombre_actor}')
def get_actor(nombre_actor: str):
    
    '''Se ingresa el nombre de un actor que se encuentre dentro de un dataset debiendo devolver el éxito del mismo medido a través del retorno. 
    Además, la cantidad de películas en las que ha participado y el promedio de retorno'''
    
    # Iterar sobre cada fila del DataFrame
    for _, row in df_consultas.iterrows():
        # Obtener la cadena de actores
        cadena_actores = row['cast']
        
        # Convertir la cadena de actores a una lista utilizando eval()
        lista_actores = eval(cadena_actores)
        
        # Verificar si el nombre del actor se encuentra en la lista de actores
        if nombre_actor in lista_actores:
            cantidad_filmaciones = len(lista_actores)
            retorno_total = row['revenue']
            promedio_retorno = retorno_total / cantidad_filmaciones if cantidad_filmaciones > 0 else 0
            
            return {
                'actor': nombre_actor,
                'cantidad_filmaciones': cantidad_filmaciones,
                'retorno_total': retorno_total,
                'retorno_promedio': promedio_retorno
            }
    
    return f"No se encontraron registros para el actor {nombre_actor}."

    

#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/get_director/{nombre_director}')
def get_director(nombre_director: str):
    
    '''Se ingresa el nombre de un director que se encuentre dentro de un dataset debiendo devolver el éxito del mismo medido a través del retorno. 
    Además, deberá devolver el nombre de cada película con la fecha de lanzamiento, retorno individual, costo y ganancia de la misma.'''
    
    fecha_lanzamiento = []
    retorno = []
    costo = []
    ganancia = []

    df_filtrado = df_consultas[df_consultas['crew'].apply(lambda x: nombre_director in ast.literal_eval(x))]

    if not df_filtrado.empty:
        peliculas = df_filtrado['title'].tolist()
        fecha_lanzamiento = df_filtrado['release_date'].tolist()
        retorno = df_filtrado['return'].tolist()
        costo = df_filtrado['budget'].tolist()
        ganancia = df_filtrado['revenue'].tolist()

        return {
            'director': nombre_director,
            'peliculas': peliculas,
            'anio': fecha_lanzamiento,
            'retorno_pelicula': retorno,
            'budget_pelicula': costo,
            'revenue_pelicula': ganancia
        }
    else:
        return f"No se encontraron registros para el director {nombre_director}."
    

#-------------------------------------------------------------------------------------------------------------------------------


@app.get('/recomendacion/{titulo}')
def recomendacion(titulo:str):
    
    '''Ingresas un nombre de pelicula y te recomienda las similares en una lista'''

    similitudes = {}

    def calculo_Jaccard_1(valor1, valor2, distancia_maxima):
        return 1 - abs(valor1 - valor2) / distancia_maxima

    def calculo_Jaccard_2(vector1, vector2):
        cantidad_total = len(vector1)
        comparacion = vector1 == vector2
        cantidad_elementos_en_comun = np.sum(comparacion)
        if cantidad_elementos_en_comun == 0:
            return 0.000001
        else:
            return cantidad_elementos_en_comun / cantidad_total

    def calculo_Jaccard(film1, film2):
        similitud_release_year = calculo_Jaccard_1(film1['release_year'], film2['release_year'], year_distance)
        similitud_popularity = calculo_Jaccard_1(film1['popularity'], film2['popularity'], popularity_distance)
        similitud_vote_average = calculo_Jaccard_1(film1['vote_average'], film2['vote_average'], vote_average_distance)
        similitud_runtime = calculo_Jaccard_1(film1['runtime'], film2['runtime'], runtime_distance)
        similitud_genres = calculo_Jaccard_2(film1['genres'], film2['genres'])
        similitud_countries_production = calculo_Jaccard_2(film1['production_countries'], film2['production_countries'])
        similitud = similitud_release_year * similitud_popularity * similitud_vote_average * similitud_runtime * similitud_genres * similitud_countries_production
        return similitud

    def obtener_titulos_recomendados(diccionario_recomendacion, dataframe):
        titulos_recomendados = []
        for key in diccionario_recomendacion.keys():
            film_id = int(key)
            row = dataframe.loc[dataframe['id'] == film_id]
            titulo = row['title'].values[0]
            titulos_recomendados.append(titulo)
        return titulos_recomendados

    # Buscar el film por título
    film_encontrado = data_ml[data_ml['title'] == titulo]
    if film_encontrado.empty:
        return []  # Si no se encuentra el título, retornar lista vacía

    # Cálculo de las distancias máximas
    year_distance = data_ml['release_year'].max() - data_ml['release_year'].min()
    popularity_distance = data_ml['popularity'].max() - data_ml['popularity'].min()
    vote_average_distance = data_ml['vote_average'].max() - data_ml['vote_average'].min()
    runtime_distance = data_ml['runtime'].max() - data_ml['runtime'].min()

    # Dividir el DataFrame en lotes
    lotes = np.array_split(data_ml, np.ceil(len(data_ml) / 507))

    for lote in lotes:
        for _, inner_row in lote.iterrows():
            if inner_row['id'] != film_encontrado.iloc[0]['id']:
                similitud = calculo_Jaccard(film_encontrado.iloc[0], inner_row)
                similitudes[inner_row['id']] = similitud

    diccionario_ordenado = dict(sorted(similitudes.items(), key=lambda x: x[1], reverse=True))
    diccionario_recomendacion = dict(list(diccionario_ordenado.items())[:5])
    recomendacion = obtener_titulos_recomendados(diccionario_recomendacion, data_ml)

    return {'lista_recomendada': recomendacion}