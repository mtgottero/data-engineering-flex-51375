# Proyecto Final - Curso Data Engineering Flex - Coderhouse - Comision 51375
## Curso Data Engineering - Martin Gottero

El proyecto se basa en extraer la información hora por hora de variables del tiempo desde una API con licencia gratuita de datos del tiempo, se transforma, para solo obtener variables principales y agruparlas, luego se carga en Amazon Redshift.

En esta entrega, adapte el ETL para procesar DAGs  desde Airflow ejecutado desde Docker.
Van a encontrar:


| nombre | descripcion |
| --- | --- |
| README.md | Este documento de presentación del proyecto |
| docker-compose.yaml |  en el cual levanta el servicio de Airflow en Docker. |
| ETL_Wheather.py | Scrit Principal del ETL |
| airflow.cfg |  Archivo de configuracion de Airflow |
| webserver_config.py |  Archivo de configuracion de Airflow |

