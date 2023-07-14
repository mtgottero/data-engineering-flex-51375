#Importación de librerías
import pandas as pd
import requests
import psycopg2
from psycopg2.extras import execute_values
from datetime import timedelta,datetime
from pathlib import Path
from airflow import DAG
import json
# Operadores
from airflow.operators.python_operator import PythonOperator
import os
import smtplib
from airflow.models import Variable


#esto direcciona al path original.. el home de la maquina de docker
dag_path = os.getcwd()     

# Definición de variables, tomando variables de entorno
data_base = "data-engineer-database"
url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws"
user = "martintomasgottero_coderhouse"
pwd = Variable.get("PASSWORD_REDSHIFT")
pwd_g = Variable.get("PASSWORD_GMAIL")
api_key = Variable.get("API_KEY") 



conn = {
    'host': url,
    'username': user,
    'database': data_base,
    'port': '5439',
    'pwd': pwd
}

# argumentos por defecto para el DAG
default_args = {
    'owner': 'MGottero',
    'start_date': datetime(2023,6,19),
    'retries':5,
    'retry_delay': timedelta(minutes=5)
}

# Creación del Dag principal
WHE_dag = DAG(
    dag_id='ETL_Wheather',
    default_args=default_args,
    description='Extrae datos de los proximos 3 dias del tiempo(temperaturas, lluvia, viento)',
    schedule_interval="@daily",
    catchup=False
)

# E - Funcion de extrar datos de Api Publica
def extraer_data(exec_date):
    try:
         print(f"Adquiriendo data para la fecha: {exec_date}")
         date = datetime.strptime(exec_date, '%Y-%m-%d %H')
         url = 'http://api.weatherapi.com/v1/forecast.json'
         parameters = {"key": api_key,
                      'q' : 'Esperanza,Argentina',
                      'days' : '3'
                      }
         response = requests.get(url, parameters)
         if response:
              print('Success!')
              data = response.json()
              with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "w") as json_file:
                   json.dump(data, json_file)
         else:
              print('An error has occurred.') 
    except ValueError as e:
        print("Formato datetime deberia ser %Y-%m-%d %H", e)
        raise e       


# T - Funcion de transformacion en tabla
def transformar_data(exec_date, ti):       
    print(f"Transformando la data para la fecha: {exec_date}") 
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    with open(dag_path+'/raw_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".json", "r") as json_file:
         loaded_data=json.load(json_file)
  
    #se genera cada dataframes de cada día, ingresando a cada diccionario de cada día.    
    df0 = pd.DataFrame.from_dict(loaded_data['forecast']['forecastday'][0]['hour'],orient='columns')
    df1 = pd.DataFrame.from_dict(loaded_data['forecast']['forecastday'][1]['hour'],orient='columns')
    df2 = pd.DataFrame.from_dict(loaded_data['forecast']['forecastday'][2]['hour'],orient='columns')     
    actual = pd.DataFrame.from_dict(loaded_data['current'],orient='columns')
    # convertir a formato fecha 
    actual = pd.to_datetime(actual["last_updated"], infer_datetime_format=True)
    #se colocan los dataframes en la variable frames
    actual = actual.values.tolist()
    frames = [df0, df1, df2]
    #se unen los dataframes.
    result = pd.concat(frames)
    result = result[['time_epoch', 'time', 'temp_c', 'wind_kph', 'wind_dir', 'precip_mm', 'humidity', 'chance_of_rain', 'vis_km']]


    print('Actualizacion de Api:', actual)
    ti.xcom_push(key='transformar_data', value= actual)
  
   # Se envian los datos a un CSV
    result.to_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv", index=False, mode='a')

# L - Funcion conexion a redshift
def conexion_redshift(exec_date):
    print(f"Conectandose a la BD en la fecha: {exec_date}") 

    try:
        conn = psycopg2.connect(
            host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
            dbname=data_base,
            user=user,
            password=pwd,
            port='5439'
            )
        print("Conectado a Redshift correctamente!")
            
    except Exception as e:
        print("No se pudo contectar a Redshift.")
        print(e)
# L - Funcion para vaciar tabla antes de ingresar información nueva.
def vaciar_tabla(exec_date):
    print(f"Vaciando la tabla en la fecha: {exec_date}") 
    #def cargar_en_bd(redshift_conn, table_name, dataframe):
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    db_conn = psycopg2.connect(
        host=url,
        dbname=conn["database"],
        user=conn["username"],
        password=conn["pwd"],
        port='5439')
    
    table_name = 'tiempo_x3'
    cur = db_conn.cursor()
           
    #Vaciar la tabla
    truncate_table = f"""
        TRUNCATE TABLE {table_name};
        """
    cur.execute(truncate_table)

    print('Tabla Vacia')

# L - Funcion de carga de data en Redshift
def cargar_data(exec_date):
    print(f"Cargando la data para la fecha: {exec_date}")
    date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    #date = datetime.strptime(exec_date, '%Y-%m-%d %H')
    records=pd.read_csv(dag_path+'/processed_data/'+"data_"+str(date.year)+'-'+str(date.month)+'-'+str(date.day)+'-'+str(date.hour)+".csv")
    print(records.shape)
    print(records.head())
    #def cargar_en_bd(redshift_conn, table_name, dataframe):
    url="data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com"
    db_conn = psycopg2.connect(
        host=url,
        dbname=conn["database"],
        user=conn["username"],
        password=conn["pwd"],
        port='5439')
    
    table_name = 'tiempo_x3'
    dtypes= records.dtypes
    cols= list(dtypes.index )
    tipos= list(dtypes.values)
    type_map = {'int64': 'INT','float64': 'FLOAT','object': 'VARCHAR(50)'}
    sql_dtypes = [type_map[str(dtype)] for dtype in tipos]
    # Definir formato SQL VARIABLE TIPO_DATO
    column_defs = [f"{name} {data_type}" for name, data_type in zip(cols, sql_dtypes)]
    # creacion de tabla
    table_schema = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {', '.join(column_defs)}
        );
        """
    
    cur = db_conn.cursor()
    
    cur.execute(table_schema)
    

    # Generar los valores a insertar
    values = [tuple(x) for x in records.to_numpy()]
    # Definir el INSERT
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    # Se ejecuta el insert.
    cur.execute("BEGIN")
    execute_values(cur, insert_sql, values)
    cur.execute("COMMIT")
    print('Proceso terminado')

# Enviar aviso de ejecución de ETL por email mediante SMTP
def enviar(ti):
    try:
        x=smtplib.SMTP('smtp.gmail.com',587)
        x.starttls()
        x.login('mtgottero@gmail.com', pwd_g) 
        fecha = ti.xcom_pull(key='transformar_data', task_ids='transformar_data')
        subject='Nueva data del tiempo en Esperanza'
        body_text='Se agregaron datos del tiempo: '
        num_ = '{}'.format(fecha)
        body_text_new = body_text + num_
        print(body_text_new)
        message='Subject: {}\n\n{}'.format(subject, body_text_new)
        x.sendmail('mtgottero@gmail.com','martintomasgottero@gmail.com',message)
        print('Exito')   
    except Exception as exception:
        print(exception)
        print('Failure')


  
# Tareas
##1. Extraccion
task_1 = PythonOperator(
    task_id='extraer_data',
    python_callable=extraer_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag,
)

#2. Transformacion
task_2 = PythonOperator(
    task_id='transformar_data',
    python_callable=transformar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag,
)

# 3. Carga de data 
# 3.1 Conexion a base de datos
task_31= PythonOperator(
    task_id="conexion_BD",
    python_callable=conexion_redshift,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag
)

# 3.2 Vaciar Tabla
task_32 = PythonOperator(
    task_id='vaciar_tabla',
    python_callable=vaciar_tabla,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag,
)

# 3.3 Envio final
task_33 = PythonOperator(
    task_id='cargar_data',
    python_callable=cargar_data,
    op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag,
)

# 4 Envio de aviso por Email
task_4 = PythonOperator(
    task_id='send_mail',
    python_callable=enviar,
    #op_args=["{{ ds }} {{ execution_date.hour }}"],
    dag=WHE_dag,
)
# Definicion orden de tareas
task_1 >> task_2 >> task_31 >> task_32 >> task_33 >> task_4