from datetime import timedelta, datetime
import requests as r
import datetime as dt
import json
import pandas as pd
from sqlalchemy import create_engine
import smtplib

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable


def _search_items_ml(ti):
    # Lanzar una excepción si se corre esta función con otra fecha que no sea la actual, en backfill por ejemplo
    if ti.execution_date.strftime("%Y-%m-%d") != str(dt.date.today()):
        raise Exception(
            "La fecha de hoy no coincide con la fecha de ejecución. \n Ejecución cancelada para evitar cargar datos incorrectos"
        )

    # Cargar el json de los ítems a buscar en ML
    with open(
        f'{Variable.get("API_ML_FILEPATH")}/api_ml_config.json'
    ) as search_list_json:
        search_list = json.load(search_list_json)
        print("SEARCH LIST:", search_list)

    raw_data_array = []

    for item in search_list:
        # Request a ML para cada item
        search_terms = item["search_terms"]
        site = "MLA"
        acces_token = Variable.get("API_ML_SECRET_ACCESS_TOKEN")
        search_expression = search_terms.replace(" ", "%20")
        paging_limit = 50

        headers = {"Authorization": f"Bearer {acces_token}"}
        url = f"https://api.mercadolibre.com/sites/{site}/search?q={search_expression}"
        params = {"limit": paging_limit, "offset": 0}

        response = r.request("GET", url, headers=headers, params=params)
        data = response.json()

        metadata = {
            "site_id": data.get("site_id"),
            "query": data.get("query"),
            "paging": data.get("paging"),
        }

        productos = data.get("results")

        # Paginación sobre primary_results y no sobre total para trabajar con respuestas que contengan
        # todas las palabras de la búsqueda.
        if metadata.get("paging").get("primary_results") > paging_limit:
            iter_paging = metadata.get("paging").get("primary_results") // paging_limit
            for i in range(1, iter_paging + 1):
                params["offset"] = paging_limit * i
                response = r.request("GET", url, headers=headers, params=params)
                if response.status_code == 200:
                    iter_productos = response.json().get("results")
                    productos += iter_productos
        else:
            productos = productos[: metadata.get("paging").get("primary_results")]

        print("METADATA", metadata)
        print("CANTIDAD DE PRODUCTOS", len(productos))

        # Seleccionar datos de interés
        productos_selected_data = []
        for p in productos:
            attr = {}
            for i in p["attributes"]:
                attr[i["id"]] = i["value_name"]
            fila = {
                "id": p["id"],
                "title": p["title"],
                "condition": p["condition"],
                "price": p["price"],
                "installments_rate": p["installments"].get("rate"),
                "seller_id": p["seller"].get("id"),
                "shipping_free": p["shipping"].get("free_shipping"),
            }
            productos_selected_data.append(fila)

        # Juntar TODOS LOS DATOS DE INTERÉS (productos_selected_data + el resto de la información de search_list)
        item["data"] = productos_selected_data
        raw_data_array.append(item)

    # Guardar la información en un json
    filepath_raw_data = f'{Variable.get("API_ML_FILEPATH")}/files/{ti.execution_date.strftime("%Y-%m-%d")}-raw_data.json'
    with open(filepath_raw_data, "w") as archivo:
        json.dump(raw_data_array, archivo)

    # Pasar la ruta del json por XCOMS a la tarea siguiente
    ti.xcom_push(key="raw_data_array", value=filepath_raw_data)


def _filter_items(ti):
    # Obtener lso datos desde la ruta pasada por XCOMS
    filepath_raw_data = ti.xcom_pull(key="raw_data_array")
    with open(filepath_raw_data) as raw_data_array_json:
        raw_data_array = json.load(raw_data_array_json)

    filtered_data_array = []

    for item in raw_data_array:
        prod = pd.DataFrame(item["data"])
        prod["seller_id"] = prod["seller_id"].astype(object)

        # Filtrado, sólo productos nuevos
        prod = prod.drop(prod[~(prod["condition"] == "new")].index)

        # Filtrado, exclusión de productos por palabras clave
        prod["title.lower"] = prod["title"].str.lower()
        for kw in item["keywords"]:
            prod = prod.drop(prod[prod["title.lower"].str.contains(kw)].index)
        prod = prod.drop("title.lower", axis=1)

        # Filtrado, exclusión de productos por shipping_free
        # prod = prod.drop(prod[(prod['price'] < int(os.environ.get('ML_FREE_SHIPPING_PRICE'))) & (prod['shipping_free'] == True)].index)
        prod = prod.drop(
            prod[
                (prod["price"] < int(Variable.get("API_ML_FREE_SHIPPING_PRICE")))
                & (prod["shipping_free"] == True)
            ].index
        )

        # Filtrado, exclusión de productos por cuotas sin interés
        prod = prod.drop(prod[(prod["installments_rate"] == 0)].index)

        # Filtrado, exclusión de productos por outsider: 2 StD +- de la mediana
        median = prod["price"].median()
        std = prod["price"].std()
        prod = prod.drop(
            prod[
                (prod["price"] < median - 2 * std) | (prod["price"] > median + 2 * std)
            ].index
        )

        # Agregado de fecha y términos de búsqueda en ML, nombre y categoría definidos en el archivo de configuración
        prod["search_date"] = ti.execution_date.strftime("%Y-%m-%d")
        prod["search_terms"] = item["search_terms"]
        prod["product_category"] = item["category"]
        prod["product_name"] = item["name"]

        # Eliminar las columnas que ya no son más necesarias
        prod = prod.drop(["installments_rate", "shipping_free", "condition"], axis=1)

        # Guardar la información de cada producto en un json
        prod.to_json(
            f'{Variable.get("API_ML_FILEPATH")}/files/{ti.execution_date.strftime("%Y-%m-%d")}-{item["category"]}_{item["name"]}-products_filtered.json'
        )
        filtered_data_array.append(prod)

    # Guardar la información en un json
    all_pf = pd.concat(filtered_data_array)
    filepath_all_pf = f'{Variable.get("API_ML_FILEPATH")}/files/{ti.execution_date.strftime("%Y-%m-%d")}.all_pf.json'
    all_pf.to_json(filepath_all_pf, orient="records", lines=True)

    # Pasar la ruta del json por XCOMS a la tarea siguiente
    ti.xcom_push(key="all_pf", value=filepath_all_pf)


def db_connection():
    # Obtener la conexión desde Airflow utilizando BaseHook
    redshift_conn = BaseHook.get_connection("redshift_conn")

    # Configurar la conexión a la base de datos de Redshift utilizando SQLAlchemy
    engine = create_engine(
        f"postgresql://{redshift_conn.login}:{redshift_conn.password}@{redshift_conn.host}:{redshift_conn.port}/{redshift_conn.schema}"
    )
    return engine


def load_to_db(data, table, pk_date):
    # Función para cargar los datos en redshift, con funcionalidad adicional para evitar cargar duplicados
    engine = db_connection()

    query = "SELECT DISTINCT " + pk_date + " FROM " + table
    existing_pk = pd.read_sql_query(query, engine)
    print("RESPUESTA A QUERY REDSHIFT", existing_pk)
    # Comprobar si hay duplicados.
    lista_fechas = []
    for i in existing_pk[pk_date].tolist():
        fecha = str(i)
        lista_fechas.append(fecha)

    if data[pk_date].isin(lista_fechas).any():
        print("Carga de datos cancelada. Hay registros duplicados")

    else:
        print("No hay registros duplicados")

        # Carga de los datos en Redshift
        data.to_sql(table, engine, if_exists="append", index=False)
        print("La carga de datos fue exitosa")


def _all_pf_load_to_db(ti):
    filepath_all_pf = ti.xcom_pull(
        key="all_pf"
    )  # Obtener la ruta del archivo desde XCOMS
    all_pf = pd.read_json(filepath_all_pf, orient="records", lines=True)  # Leer el json
    all_pf["load_date"] = str(dt.date.today())  # Agregar la columna load_date
    load_to_db(all_pf, "products_filtered", "search_date")  # Cargar en la base de datos


def _price_analysis(ti):
    # Obtener los datos desde redshift
    engine = db_connection()
    query = f"""SELECT product_category, product_name, search_date, price FROM products_filtered 
        WHERE search_date = '{ti.execution_date.strftime("%Y-%m-%d")}'"""

    data = pd.read_sql_query(query, engine)

    # Algunas funciones de agrupación y agregación
    all_pa = (
        data.groupby(["search_date", "product_category", "product_name"])["price"]
        .agg(["count", "mean", "median", "min"])
        .reset_index()
    )
    all_pa["search_date"] = all_pa["search_date"].astype(str)

    # Guardar la información en un json
    filepath_all_pa = f'{Variable.get("API_ML_FILEPATH")}/files/{ti.execution_date.strftime("%Y-%m-%d")}-all_pa.json'
    all_pa.to_json(filepath_all_pa, orient="records", lines=True)

    # Pasar la ruta del json por XCOMS a la tarea siguiente
    ti.xcom_push(key="all_pa", value=filepath_all_pa)


def _all_pa_load_to_db(ti):
    filepath_all_pa = ti.xcom_pull(
        key="all_pa"
    )  # Obtener la ruta del archivo desde XCOMS
    all_pa = pd.read_json(filepath_all_pa, orient="records", lines=True)  # Leer el json
    all_pa["load_date"] = str(dt.date.today())  # Agregar la columna load_date
    load_to_db(all_pa, "price_analysis", "search_date")  # Cargar en la base de datos


def _alerts(ti):
    # Leer la configuración de las alertas
    with open(
        f'{Variable.get("API_ML_FILEPATH")}/alerts_config.json'
    ) as alerts_config_json:
        alerts_config = json.load(alerts_config_json)

    # Conexión a la base de datos
    engine = db_connection()

    # Función para cargar las alertas en el json si superan el valor de threshold
    def create_alerts(row, alert):
        if row[f"{alert['type']}_variation"] > alert["threshold"]:
            alert = {
                "alert_type": alert["type"],
                "alert_period": alert["period"],
                "alert_threshold": alert["threshold"],
                "category_and_name": row['category_and_name'],
                "new_value": row[f"{alert['type']}_today"],
                "old_value": row[f"{alert['type']}_old"],
                "variation": row[f"{alert['type']}_variation"],
            }

            alert_list.append(alert)

    alert_list = []

    # Ciclo para cada alerta definida en alerts_config.json
    for alert in alerts_config:
        # Construcción de las queries de acuerdo a los parámetros confgurables en la alerta
        str_period_query = (  # Período en string para la query
            f"'{(ti.execution_date - dt.timedelta(days = alert['period'])).strftime('%Y-%m-%d')}'"
            + " AND "
            + f"'{ti.execution_date.strftime('%Y-%m-%d')}'"
        )
        if alert["category"] != "all" and alert["name"] != "all":
            query_old = f"""SELECT DISTINCT product_category, product_name, AVG({alert['type']}) {alert['type']}_old FROM price_analysis
                WHERE search_date BETWEEN {str_period_query}
                AND product_category = '{alert['category']}'
                AND product_name = '{alert['name']}'
                GROUP BY product_category, product_name"""
            query_today = f"""SELECT product_category, product_name, {alert['type']} {alert['type']}_today FROM price_analysis 
                WHERE search_date = '{ti.execution_date.strftime("%Y-%m-%d")}'
                    AND product_category = '{alert['category']}'
                    AND product_name = '{alert['name']}'"""
             
        elif alert["category"] == "all" and alert["name"] != "all":
            query_old = f"""SELECT DISTINCT product_category, product_name, AVG({alert['type']}) {alert['type']}_old FROM price_analysis
                WHERE search_date BETWEEN {str_period_query}
                AND product_name = '{alert['name']}'
                GROUP BY product_category, product_name"""
            query_today = f"""SELECT product_category, product_name, {alert['type']} {alert['type']}_today FROM price_analysis 
                WHERE search_date = '{ti.execution_date.strftime("%Y-%m-%d")}'
                    AND product_name = '{alert['name']}'"""
            
        elif alert["category"] != "all" and alert["name"] == "all":
            query_old = f"""SELECT DISTINCT product_category, product_name, AVG({alert['type']}) {alert['type']}_old FROM price_analysis
                WHERE search_date BETWEEN {str_period_query}
                AND product_category = '{alert['category']}'
                GROUP BY product_category, product_name"""
            query_today = f"""SELECT product_category, product_name, {alert['type']} {alert['type']}_today FROM price_analysis 
                WHERE search_date = '{ti.execution_date.strftime("%Y-%m-%d")}'
                    AND product_category = '{alert['category']}'"""
                        
        else:
            query_old = f"""SELECT DISTINCT product_category, product_name, AVG({alert['type']}) {alert['type']}_old FROM price_analysis
                WHERE search_date BETWEEN {str_period_query}
                GROUP BY product_category, product_name"""
            query_today = f"""SELECT product_category, product_name, {alert['type']} {alert['type']}_today FROM price_analysis 
                WHERE search_date = '{ti.execution_date.strftime("%Y-%m-%d")}'"""
            
        old_data = pd.read_sql_query(query_old, engine)
        today_data = pd.read_sql_query(query_today, engine)

        # Combinar las columnas category y name para el merge
        old_data["category_and_name"] = (
            old_data["product_category"] + " " + old_data["product_name"]
        )
        old_data = old_data.drop(["product_category", "product_name"], axis=1)
        print("OLD DATA: ", old_data)

        today_data = pd.read_sql_query(query_today, engine)
        today_data["category_and_name"] = (
            today_data["product_category"] + " " + today_data["product_name"]
        )
        today_data = today_data.drop(["product_category", "product_name"], axis=1)
        print("TODAY DATA: ", today_data)

        # Merge de los df para facilitar el procesamiento
        data = pd.merge(old_data, today_data, on="category_and_name")
        print("MERGED DATA: ", data)
        
        # Cálculo de la variación entre el valor viejo y el nuevo
        data[f"{alert['type']}_variation"] = (
            abs(data[f"{alert['type']}_old"] - data[f"{alert['type']}_today"])
            / data[f"{alert['type']}_today"]
        )

        # aplicación de la función que carga los valores en alert_list para pasarle a la siguiente tarea.
        data.apply(create_alerts, args=(alert,), axis=1)

    filepath_alerts = f'{Variable.get("API_ML_FILEPATH")}/files/{ti.execution_date.strftime("%Y-%m-%d")}-alerts.json'

    # Salida de datos a un .json
    with open(filepath_alerts, "w") as archivo:
        json.dump(alert_list, archivo)

    # Ruta al archivo en XCOMS
    ti.xcom_push(key="alert_list", value=filepath_alerts)


def _send_alerts(ti):
    # Leer el json con las alertas
    filepath_alerts = ti.xcom_pull(key="alert_list")
    with open(filepath_alerts) as alert_list_json:
        alert_list = json.load(alert_list_json)

    # Escribir y enviar el mail si hay algo cargado en alert_list
    if len(alert_list) > 0:
        msg = f"""Subject: Alertas ETL de API_ML. Fecha: {ti.execution_date.strftime("%Y-%m-%d")} \n\n
            Se listan debajo los items y valores que dispararon alertas.\n\n"""

        for alert in alert_list:
            alert_string = f"""Alerta para el producto {alert['category_and_name']}. 
                \t Alerta disparada: {alert['alert_type']} | para el período {alert['alert_period']} días
                \t Valor actual: {round(alert['new_value'], 2)} | Valor viejo: {round(alert['old_value'], 2)}
                \t Variación: {round(alert['variation'], 3)} | Valor de alerta en variación: {alert['alert_threshold']}\n\n"""
            msg += alert_string

        # Configuración de smtplib
        server = smtplib.SMTP(
            Variable.get("ALERTS_SMTP_HOST"), Variable.get("ALERTS_SMTP_PORT")
        )
        server.starttls()
        server.login(
            Variable.get("ALERTS_SMTP_USER"), Variable.get("ALERTS_SMTP_PASSWORD")
        )
        server.sendmail(
            Variable.get("ALERTS_SMTP_FROM"),
            Variable.get("ALERTS_SMTP_TO"),
            msg.encode("utf-8"),
        )  # Codificación del string en utf-8
        server.quit()
        print("Se envío correctamente el mail con las alertas.")

    else:
        print(
            "No hubo valores por encima de los valores de alerta configurados. No se envía mail de alerta."
        )


default_args = {
    "owner": "Franco González",
    "start_date": datetime(2023, 7, 20),
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_apiML",
    catchup=False,
    schedule_interval="@daily",
    default_args=default_args,
    description="DAG que controla el ETL de data de MercadoLibre para construir un historial de precios de determinados productos de interés",
) as dag:
    search_items = PythonOperator(
        task_id="search_items.extract",
        python_callable=_search_items_ml
    )

    filter_items = PythonOperator(
        task_id="filter_items.transform",
        python_callable=_filter_items
    )

    price_analysis = PythonOperator(
        task_id="build_pa_dataframe.transform",
        python_callable=_price_analysis
    )

    load_pf = PythonOperator(
        task_id="pf_to_db.load",
        python_callable=_all_pf_load_to_db
    )

    load_pa = PythonOperator(
        task_id="pa_to_db.load",
        python_callable=_all_pa_load_to_db
    )

    alerts = PythonOperator(
        task_id="alerts_creation",
        python_callable=_alerts
    )

    send_mail = PythonOperator(
        task_id="send_alerts_mail",
        python_callable=_send_alerts
    )

(
    search_items
    >> filter_items
    >> load_pf
    >> price_analysis
    >> load_pa
    >> alerts
    >> send_mail
)
