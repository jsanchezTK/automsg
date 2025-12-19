# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(woo_commerce_update) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints

import azure.functions as func
import logging
import json
# from json import JsonDecodeError
import requests
from requests.auth import HTTPBasicAuth
import os
import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient, generate_container_sas, ContainerSasPermissions
import datetime
from datetime import timedelta, date, datetime, time
import pytz
import pyodbc as odbc
import re
import traceback

woo_commerce_update = func.Blueprint()

WC_CLIENT_KEY = os.getenv("WC_CLIENT_KEY")
WC_CLIENT_SECRET = os.getenv("WC_CLIENT_SECRET")
WC_PRODUCTS_API = os.getenv("WC_PRODUCTS_API")
PA_WC_PRODUCTS = os.getenv("PA_WC_PRODUCTS")

BS_ACCOUNT_NAME = os.getenv("BS_ACCOUNT_NAME")
BS_KEY = os.getenv("BS_KEY")
BS_CONTAINER_NAME = os.getenv("BS_CONTAINER_NAME")
BS_CONNECTION_STRING = os.getenv("BS_CONNECTION_STRING")

BLOB_SERVICE_CLIENT = BlobServiceClient.from_connection_string(BS_CONNECTION_STRING)
CONTAINER_CLIENT = BLOB_SERVICE_CLIENT.get_container_client(BS_CONTAINER_NAME)

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")
CONNECTION_STRING = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"

def clean_data(data):
    """
    Replace line breaks in string fields with '^' in the given data.

    Args:
        data (list of tuples): The table data where each tuple represents a row.

    Returns:
        list of tuples: Cleaned data with line breaks replaced by '^'.
    """
    cleaned_data = []
    for row in data:
        cleaned_row = tuple(
            # Replace line breaks only if the value is a string
            value.replace('\n', '^').replace('\r', '^') if isinstance(value, str) else value
            for value in row
        )
        cleaned_data.append(cleaned_row)
    return cleaned_data


def export_to_csv_with_pandas(data, headers, filename):
    """
    Export data to a CSV file using pandas after cleaning line breaks.

    Args:
        data (list of tuples): The table data where each tuple represents a row.
        headers (list): List of column headers.
        filename (str): The name of the CSV file to be created.
    """
    try:
        if type(data[0]) == tuple:
            # Clean the data by replacing line breaks with '^'
            cleaned_data = clean_data(data)
            # Create a pandas DataFrame from the cleaned data
            df = pd.DataFrame(cleaned_data, columns=headers)
        elif type(data[0]) == dict:
            df = pd.DataFrame.from_dict(data)
        else:
            raise TypeError

        # # Export the DataFrame to a CSV file
        # df.to_csv(filename, index=False, encoding="utf-8", sep='|')

        # Name with extension regardless of the extension being specified or not
        blob_name_csv = filename.replace(".csv", "")+".csv"

        # Convert DataFrame to CSV
        csv_content = df.to_csv(index=False, encoding="utf-8", sep='|')
        csv_io = BytesIO(csv_content.encode(encoding="utf-8"))

        # Upload the CSV back to Azure Blob Storage
        blob_client = CONTAINER_CLIENT.get_blob_client(blob_name_csv)
        blob_client.upload_blob(csv_io, overwrite=True)
        blob_url = f"https://{BS_ACCOUNT_NAME}.blob.core.windows.net/{BS_CONTAINER_NAME}/{blob_name_csv}"
        op = blob_url
    except Exception as e:
        op = f"Error: {filename}"
    return op


def generate_container_sas_token(account_name, account_key, container_name):
    sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True, write=True, list=True),
        expiry=datetime.utcnow() + timedelta(hours=1)  # Adjust expiry as needed
    )
    # sas_url = f"https://{account_name}.blob.core.windows.net/{container_name}?{sas_token}"
    return sas_token


def create_scoped_credential_and_external_data_source(sas_token, cursor: odbc.Cursor, credential_name="AzureBlobScopedCredential", data_source_name = "AzureBlobExternalDataSource"):
    try:
        # Define the scoped credential SQL
        drop_credential_sql = f"IF EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = '{credential_name}') DROP DATABASE SCOPED CREDENTIAL [{credential_name}];"
        create_credential_sql = f"""
        CREATE DATABASE SCOPED CREDENTIAL [{credential_name}]
        WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = '{sas_token}';
        """
        storage_account_name = BS_ACCOUNT_NAME
        container_name = BS_CONTAINER_NAME
        # Define the external data source SQL
        drop_data_source_sql = f"IF EXISTS (SELECT * FROM sys.external_data_sources WHERE name = '{data_source_name}') DROP EXTERNAL DATA SOURCE [{data_source_name}];"
        create_data_source_sql = f"""
        CREATE EXTERNAL DATA SOURCE [{data_source_name}]
        WITH (
            TYPE = BLOB_STORAGE,
            LOCATION = 'https://{storage_account_name}.blob.core.windows.net/{container_name}',
            CREDENTIAL = [{credential_name}]
        );
        """

        # Execute SQL commands to create the credential and data source
        cursor.execute(drop_data_source_sql)
        cursor.execute(drop_credential_sql)
        cursor.execute(create_credential_sql)
        cursor.execute(create_data_source_sql)
        cursor.commit()
        logging.info("Scoped credential and external data source created successfully.")

    except Exception as e:
        logging.info("An error occurred:", e)


def wc_list_products_library(): 
    from woocommerce import API 
    tz = pytz.timezone('Chile/Continental')
    tz_utc = pytz.timezone('UTC')
    delta = datetime.now(tz=tz_utc).hour - datetime.now(tz=tz).hour
    iso_time = (datetime.combine((datetime.now() + timedelta(hours=delta, days=-8)).date(),time.min)).isoformat()
    wcapi = API(
        url="https://turistik.com", # Your store URL
        consumer_key=WC_CLIENT_KEY, # Your consumer key
        consumer_secret=WC_CLIENT_SECRET, # Your consumer secret
        wp_api=True, # Enable the WP REST API integration
        version="wc/v3" # WooCommerce WP REST API version
    )
    payload = f"per_page=100&modified_after={iso_time}"
    response = wcapi.get(f"products?{payload}")
    return response


def wc_list_products_request():
    basic_auth = HTTPBasicAuth(WC_CLIENT_KEY,WC_CLIENT_SECRET)
    payload = {"per_page": 20}
    headers = {"tbp": "tr101"}
    response = requests.get(WC_PRODUCTS_API, auth=basic_auth, params=payload, headers=headers)
    return response


def wc_list_products_pa():
    response = requests.get(PA_WC_PRODUCTS)
    return response



def get_country_codes() -> pd.DataFrame:
    url = "https://www.countrycode.org/"
    tables = pd.read_html(url)
    codigos = tables[1]
    dict_table = codigos.to_dict(orient="records")
    corrected = [{"iso_code": x["ISO CODES"].split(" / ")[1], "code": x["COUNTRY CODE"].split(", ")[0]} for x in dict_table]
    df = pd.DataFrame.from_dict(corrected)
    return df


def bulk_insert(blob_name: str, bulk_table: str, stored_procedure: str):
    conx = odbc.connect(CONNECTION_STRING)
    tz = pytz.timezone('Chile/Continental') 
    cursor = conx.cursor()
    timekey = datetime.now(tz=tz).strftime("%Y%m%d%H%M%S")
    scoped_credential = f"devturistik_files_{timekey}"
    external_data_source = f"blobstorage_devturistik_{timekey}"
    sas_token = generate_container_sas_token(BS_ACCOUNT_NAME, BS_KEY, BS_CONTAINER_NAME)
    create_scoped_credential_and_external_data_source(sas_token=sas_token, cursor=cursor, credential_name=scoped_credential, data_source_name=external_data_source)
    this_filename = blob_name
    this_table = bulk_table
    this_sp = stored_procedure
    logging.info(f"Cleaning table {this_table}")
    query = f"truncate table {this_table}"
    cursor.execute(query)
    logging.info(f"Bulk inserting: {this_filename}")
    query = f"""
            BULK INSERT {this_table}
            FROM '{this_filename}'
            with (
                FIELDTERMINATOR = '|',
                ROWTERMINATOR = '0x0A',
                FIRSTROW = 2,
                DATA_SOURCE = '{external_data_source}',
                CODEPAGE = '65001',
                FORMAT = 'CSV'
            )
    """
    cursor.execute(query)
    cursor.commit()
    logging.info(f"Executing Stored Procedure: {this_sp}")
    cursor.execute(f"{{CALL {this_sp}}}")
    cursor.commit()


# @woo_commerce_update.timer_trigger(schedule="0 30 1 * * *", arg_name="myTimer", run_on_startup=False,
#               use_monitor=False) 
# def get_products(myTimer: func.TimerRequest) -> None:
def get_products() -> None:
    query = ""
    try:
        tz = pytz.timezone('Chile/Continental') 
        response = wc_list_products_pa()
        lista = response.json()
        tabla_cabecera = []
        tabla_metadata = []
        tabla_mapas = []
        # tabla_imagenes = []
        logging.info("Iterating through products...")
        for i in lista:
            # Datos principales
            (service_id, created, modified, name, service_slug, permalink, categories_name, categories_slug) = (
                i["id"], i["date_created_gmt"], i["date_modified_gmt"], i["name"], i["slug"], i["permalink"], i["categories"][0]["name"], i["categories"][0]["slug"])
            logging.info(f"{service_id}: {name}")
            # METADATOS
            metadatos = i["meta_data"]
            tabla_raw = [(k["key"], k["value"], service_id) for k in metadatos]
            patrones = [r"incluye-texto.*\d{1}$",
                        # r"incluye-texto.*\d{1}$",
                        r"no-incluye-texto.*\d{1}$",
                        # r"itinerario_descripcion_.*\d{2}$",
                        r"recomendacion_.*\d{1}$"
                        ]
            logging.info(f"Filtering metadata...")
            for p in patrones:
                # filtros incluye, no incluye, recomendaciones
                filtro = list(filter(lambda x: bool(re.match(p, x[0])), tabla_raw))
                tabla_metadata = tabla_metadata + filtro
            # filtro descripción
            logging.info("Filtering description...")
            filtro = list(filter(lambda x: x[0] in ["descripcion", "descripcion_corta", "location_key"], tabla_raw))
            tabla_metadata = tabla_metadata + filtro
            # filtro maps
            # filtro = list(filter(lambda x: "map" in x[1], tabla_raw))
            logging.info("Setting ozytrip_tourcode...")
            try:
                ozytrip_id = list(filter(lambda x: x[0] == "ozytrip_tourcode", tabla_raw))[0][1]
                if len(ozytrip_id) < 5:
                    ozytrip_id = "N/A"
            except IndexError:
                ozytrip_id = "N/A"

            # MAPAS
            logging.info("Filtering maps...")
            mapas = list(
                filter(lambda x: "https://goo.gl/maps/" in x[2], [(service_id, x["key"], x["value"]) for x in metadatos]))
            for mapa in mapas:
                these_maps = mapa[2].split("</a>")
                for this_map in these_maps:
                    if "https://goo.gl/maps/" in this_map:
                        try:
                            this_map = this_map.replace("target=”_blank”", "target=\"”_blank”\"")
                            par_0 = list(filter(lambda x: x[0] in ["h", ">"], this_map.split("\"")))
                            par_1 = (par_0[1][1:], par_0[0])
                            if par_1 not in tabla_mapas:
                                tabla_mapas.append(par_1)
                        except IndexError:
                            logging.info(f"Error in map: {this_map}")
            try:
                img_url = i["yoast_head_json"]["schema"]["@graph"][0]["thumbnailUrl"]
            except IndexError:
                img_url = ""
            # APPEND CABECERA
            tabla_cabecera.append((service_id, created, modified, name, service_slug, permalink, categories_name, categories_slug, ozytrip_id, img_url))


        headers_cabecera = ["service_id", "created", "modified", "name", "slug", "permalink", "categories_name", "categories_slug", "ozytrip_id", "img_url"]
        headers_metadata = ["key", "value", "service_id"]
        headers_mapas = ["lugar", "link"]

        logging.info(export_to_csv_with_pandas(tabla_cabecera, headers_cabecera, "wc_cabecera.csv"))
        logging.info(export_to_csv_with_pandas(tabla_metadata, headers_metadata, "wc_metadatos.csv"))
        logging.info(export_to_csv_with_pandas(tabla_mapas, headers_mapas, "wc_mapas.csv"))
        extractions = [
            ("wc_cabecera.csv","OzyTrip.bulkInsertServiciosWooCommerce", "[OzyTrip].[insertar_staging_servicioswoocommerce]"),
            ("wc_metadatos.csv","OzyTrip.bulkInsertServiciosWooCommerceMetaData", "[OzyTrip].[insertar_staging_servicioswoocommercemapas]"),
            ("wc_mapas.csv","OzyTrip.bulkInsertServiciosWooCommerceMapas", "[OzyTrip].[insertar_staging_servicioswoocommercemetadata]")
            ]
        conx = odbc.connect(CONNECTION_STRING)
        cursor = conx.cursor()
        timekey = datetime.now(tz=tz).strftime("%Y%m%d%H%M%S")
        scoped_credential = f"devturistik_files_{timekey}"
        external_data_source = f"blobstorage_devturistik_{timekey}"
        sas_token = generate_container_sas_token(BS_ACCOUNT_NAME, BS_KEY, BS_CONTAINER_NAME)
        create_scoped_credential_and_external_data_source(sas_token=sas_token, cursor=cursor, credential_name=scoped_credential, data_source_name=external_data_source)
        for this_extraction in extractions:
            this_filename = this_extraction[0]
            this_table = this_extraction[1]
            this_sp = this_extraction[2]
            logging.info(f"Cleaning table {this_table}")
            query = f"truncate table {this_table}"
            cursor.execute(query)
            logging.info(f"Bulk inserting: {this_filename}")
            query = f"""
                    BULK INSERT {this_table}
                    FROM '{this_filename}'
                    with (
                        FIELDTERMINATOR = '|',
                        ROWTERMINATOR = '0x0A',
                        FIRSTROW = 2,
                        DATA_SOURCE = '{external_data_source}',
                        CODEPAGE = '65001',
                        FORMAT = 'CSV'
                    )
            """
            cursor.execute(query)
            cursor.commit()
            logging.info(f"Executing Stored Procedure: {this_sp}")
            cursor.execute(f"{{CALL {this_sp}}}")
            cursor.commit()
        conx.close()
    except Exception as e:
        tb = traceback.format_exc()
        tipo_error = type(e).__name__
        logging.error(f"{tipo_error}: {str(e)}")
        logging.error(tb)
        cod_status = 500
        reply = "Se ha generado una excepción del tipo "+tipo_error+"\nDetalles: "+tb
        if query:
            logging.error(f"Last query: {query}")
        raise type(e)
        #if "JSONDecodeError" in tipo_error:
        #    reply = f"{tipo_error}\n{response.content}"
        #    return func.HttpResponse(reply, status_code=cod_status)
    else:
        cod_status = 200
        reply = "Función ejecutada correctamente."
    logging.info(f"Función ejecutada con código {str(cod_status)}")


if __name__ == "__main__":
    products = wc_list_products_request()
    try:
        print(products.json())
    except json.JSONDecodeError:
        print(products.text)
    print("Done")