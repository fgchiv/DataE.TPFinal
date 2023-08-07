# ApiML
ETL de datos de MercadoLibre para construir un historial de precios de determinados productos de interés. Además de eso, el script tiene un mecanismo de alertas configurable.

## Instrucciones para instalar la ETL en un DAG de Airflow.

1. Clonar el repositorio en un directorio local.
2. Comprobar que estén las carpetas './dags', './api_ml_files', y los archivos './docker-compose.yaml', './dags/dag_api_ml.py', './api_ml_files/alerts_config.json' y './api_ml_files/api_ml_config.json'
3. Crear las carpetas **./logs, ./plugins y './api_ml_files/files'**.
4. Abrir el docker; asegurarse de que el docker engine esté ejecutándose
5. Con la terminal situada en la raíz donde está docker-compose.yaml, ejecutar el comando "docker-compose up airflow-init"
6. Luego, ejecutar "docker-compose up". En la primera ejecución va a tomar su tiempo
7. Podrá acceder a la interfaz web de Airflow en la dirección localhost:8080. Allí encontrará el dag_apiML pausado
8. Configure la conexión a la base de datos con el nombre "redshift_conn", tipo "Postgres".
9. Importe las variables desde el archivo variables.json (se adjunta un template en la raíz del repositorio) con las siguientes claves (el filepath es el definido en docker-compose.yaml):

    "ALERTS_SMTP_FROM":
    "ALERTS_SMTP_HOST":
    "ALERTS_SMTP_PASSWORD":
    "ALERTS_SMTP_PORT":
    "ALERTS_SMTP_TO":
    "ALERTS_SMTP_USER":
    "API_ML_FILEPATH": "/usr/local/airflow/api_ml_files",
    "API_ML_FREE_SHIPPING_PRICE": 
    "API_ML_SECRET_ACCESS_TOKEN":

10. Puede activar el dag y configurar las alertas y el dag desde los archivso de configuración './api_ml_files/alerts_config.json' y './api_ml_files/api_ml_config.json'. Asegúrese de que las tablas estén creadas en la base de datos.

## Creación de las tablas en la base de datos

Se adjunta el script de creación de base datos en el repositorio 'create_tables_api_ml.sql'.

## Cargar nuevos productos para obtener datos.

Puede definir nuevos productos para incluir en el script, modificando el archivo './api_ml_files/api_ml_config.json'. Debe agregar los datos estructurados de la siguiente forma, separados de los anteriores y posteriores por ",":

    {
        "search_terms": "palabras a buscar",
        "keywords": ["par", "set", "juego"...], 
        "category": "categoría",
        "name": "nombre del producto"
    }

- "search_terms" es la búsqueda que el script realizará en la tienda de MercadoLibre Argentina. Sólo se tomarán en cuenta resultados que contengan todas las palabras incluidas.
- "keywords" son palabras que el script utilizará para filtrar la búsqueda, buscando delimitar productos indivuales y no sets o conjuntos de productos que respondan a la búsqueda.
- "category" y "name" son dos datos que el ETL agregará a la tabla para hacer identificables a los productos de manera homogénea.

Además de filtrar por estas palabras clave, el script utiliza otros métodos para reconocer y filtrar resultados no deseados o con particularidades que puedan alterar el precio (cuotas sin interés, envío gratis).

## Definir las alertas

El archivo de configuración de las alertas es './api_ml_files/alerts_config.json'. Se configuran para umbrales de variación de algún valor de la tabla price_analysis entre el dato actual y un dato histórico (el del día anterior, o un promedio de los valores de un período configurable). Cada alerta es un objeto de este tipo:

    {
        "category": ,
        "name": ,
        "period": ,
        "type": ,
        "threshold": 
    }

- "category" y "name" son los campos que identifican cada producto en la tabla price_analysis. Se corresponden con los valores de "category" y "name" del archivo de configuración del ETL. Si la alerta corresponde a todos los productos debe escribirse **"all"** en ambos campos. También puede ser de una categoría ("cubiertas", por ejemplo), cualquier valor de nombre ("name": "all").
- "period" define el período en días contra el que se va a comparar el dato actual. Debe ser un valor entero positivo. En el caso de ser "1", comparará los valores actuales contra los del día anterior. Si es un número mayor a "1", comparará los valores actuales contra el promedio de los valores del período definido.
- "type" define la métrica de price_analysis a la que corresponde la alerta. Las opciones son **"min"** para el precio mínimo, **"count"** para la cantidad de productos en oferta, **"mean"** para el promedio de precios de los productos en oferta, **"median"** para la mediana de precios de los productos en oferta.
- "threshold" define el umbral de variación como un número de coma flotante (0.05 corresponde a 5%).

## Instrucciones de backfill

Pueden realizarse backfill de todas las tareas del ETL menos de la extracción de datos desde MercadoLibre porque este es un dato que este script captura en tiempo real. Si no se cargaron los datos de hace un mes, los datos qe puedo obtener ahora son los actuales, no los históricos (de hace un mes) que sería lo necesario para realizar esta tarea. De todas las demás tareas (cargar datos a la base de datos, procesar datos desde los json guardados previamente, ejecutar el mecanismo de alertas, por ejemplo) el backfill funciona correctamente.
El comando a usar, desde la consola del contenedor en docker es:

    airflow dags backfill dag_apiML -t <TASK_ID> -s <START DATE> -e <END DATE> -i

- Los task_id son los accesibles desde la interfaz visual de airflow. **No realizar backfill de: "search_items.extract"** porque cargará datos incorrectos como se explicó más arriba.
- El flag -i (--ignore-dependencies) se usa para evitar la ejecución de la tarea "search_items.extract" al realizar el backfill de una tarea que depende de ella (es decir, cualquiera de las otras).
- Las fechas deben escribirse en formato 'YYYY-MM-DD'.
