import azure.functions as func
import logging
import requests
from requests.auth import HTTPBasicAuth
from PIL import Image
from io import BytesIO
from azure.storage.blob import BlobServiceClient, ContentSettings
import os
import pyodbc as odbc
import datetime
from datetime import timedelta, datetime
from woocommerce import API
import json

#from woo_commerce_update import woo_commerce_update, get_products
#from gpt_translate import gpt_translate, translate_wc_values
from get_template_data import get_template_data

from woo_commerce_update import get_products
from gpt_translate import translate_wc_values
from get_template_data import test_fun, send_notifications, update_domains, check_quotas, notificacion_traslado_cyt

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)
# app.register_functions(woo_commerce_update)
# app.register_functions(gpt_translate)
app.register_functions(get_template_data)

BS_ACCOUNT_NAME = os.getenv("BS_ACCOUNT_NAME")
BS_KEY = os.getenv("BS_KEY")
BS_CONTAINER_NAME = os.getenv("BS_CONTAINER_NAME")
BS_CONNECTION_STRING = os.getenv("BS_CONNECTION_STRING")

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")
CONNECTION_STRING = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"

WC_CLIENT_KEY = os.getenv("WC_CLIENT_KEY")
WC_CLIENT_SECRET = os.getenv("WC_CLIENT_SECRET")
WC_PRODUCTS_API = os.getenv("WC_PRODUCTS_API")
PA_WC_PRODUCTS = os.getenv("PA_WC_PRODUCTS")

# @app.route(route="pic2png")
# def pic2png(req: func.HttpRequest) -> func.HttpResponse:
#     blob_service_client = BlobServiceClient.from_connection_string(BS_CONNECTION_STRING)
#     container_client = blob_service_client.get_container_client(BS_CONTAINER_NAME)
# 
#     url = req.params.get('url')
#     img_name = url.split("/")[-1].split(".")[0]
#     response = requests.get(url)
#     if response.status_code == 200:
#         try:
#             img = Image.open(BytesIO(response.content))
#             img_io = BytesIO()
#             img.save(img_io,'PNG')
#             img_io.seek(0)
#             blob_name = img_name+".png"
#             blob_client = container_client.get_blob_client(blob_name)
#             blob_client.upload_blob(img_io, blob_type="BlockBlob", overwrite=True)
#             blob_url = f"https://{BS_ACCOUNT_NAME}.blob.core.windows.net/{BS_CONTAINER_NAME}/{blob_name}"
#             reply = f"Imagen transformada: {blob_url}"
#             code_status = 200
#         except Exception as e:
#             reply = type(e).__name__
#             code_status = 500
#     else:
#         reply = "No se pudo descargar la imagen."
#         code_status = 400
#     return func.HttpResponse(reply, status_code=code_status)


# @app.timer_trigger(schedule="0 40 1 * * *", arg_name="myTimer", run_on_startup=False,
#               use_monitor=False) 
# def crear_imagenes_wp(myTimer: func.TimerRequest) -> None:
def crear_imagenes_wp() -> None:
    conx = odbc.connect(CONNECTION_STRING)
    cursor = conx.cursor()
    query = """
        select service_id, img_origen from [OzyTrip].[stagingServiciosWooCommerce]
        where 
        LEFT(
                RIGHT(img_origen, CHARINDEX('/', REVERSE(img_origen)) - 1),
                CHARINDEX('.', RIGHT(img_origen, CHARINDEX('/', REVERSE(img_origen)) - 1)) - 1
            )
            !=  
        LEFT(
                RIGHT(img_png, CHARINDEX('/', REVERSE(img_png)) - 1),
                CHARINDEX('.', RIGHT(img_png, CHARINDEX('/', REVERSE(img_png)) - 1)) - 1
            )
            and ozytrip_id != 'N/A'
    """
    cursor.execute(query)
    data = cursor.fetchall()

    blob_service_client = BlobServiceClient.from_connection_string(BS_CONNECTION_STRING)
    container_client = blob_service_client.get_container_client(BS_CONTAINER_NAME)
    count = 0
    error_count = 0
    for img in data:
        count += 1
        img_url = img.img_origen.replace("\r", "")
        service_id = img.service_id
        img_name = img_url.split("/")[-1].split(".")[0]
        response = requests.get(img_url)
        if response.status_code == 200:
            try:
                img = Image.open(BytesIO(response.content))
                img_io = BytesIO()
                img.save(img_io,'PNG')
                img_io.seek(0)
                blob_name = img_name+".png"
                blob_url = f"https://{BS_ACCOUNT_NAME}.blob.core.windows.net/{BS_CONTAINER_NAME}/{blob_name}"
                blob_client = container_client.get_blob_client(blob_name)
                blob_client.upload_blob(img_io,
                                        content_settings=ContentSettings(content_type="image/png"),
                                        blob_type="BlockBlob", 
                                        overwrite=True)
                logging.info(f"{str(count)}.1 Imagen transformada: {blob_url}")
                update_query = f"""
                                UPDATE [OzyTrip].[stagingServiciosWooCommerce]
                                set img_png = '{blob_url}'
                                where service_id = {service_id}
                """
                cursor.execute(update_query)
                cursor.commit()
                logging.info(f"{str(count)}.2 Imagen insertada: {blob_url}")
            except Exception as e:
                logging.error(f"Error: {type(e).__name__}")
                logging.error(f"Img: {img_url}")
                error_count += 1
        else:
            logging.info(f"{str(count)} No se pudo descargar: {img_url}")
            error_count += 1
    conx.close()
    success_count = count - error_count
    logging.info(f"Finalizado: {str(success_count)} correctos y {str(error_count)} erróneos.")

# @app.route(route="get_drivers", auth_level=func.AuthLevel.ANONYMOUS)
# def get_drivers(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')
#     drivers = odbc.drivers()
#     str_drivers = ""
#     for i in drivers:
#         str_drivers += f"{i}\n"
#     return func.HttpResponse(test_fun(str_drivers))


@app.route(route="test_wc_products", auth_level=func.AuthLevel.ANONYMOUS)
def test_wc_products(req: func.HttpRequest) -> func.HttpResponse:
    tipo = req.params.get("type")
    try:
        if tipo == "library":
            response = wc_list_products_library()
        elif tipo == "requests":
            response = wc_list_products_request()
        elif tipo == "PA":
            response = wc_list_products_pa()
        else:
            raise ValueError("Invalid type")
        data = response.json()
        return func.HttpResponse(str(data), status_code=200)
    except Exception as e:
        error_type = type(e).__name__
        if error_type == "JSONDecodeError":
            reply = f"{error_type}\n\n{str(response.text)}"
        else:
            reply = error_type
        return func.HttpResponse(reply, status_code=500)
    

def wc_list_products_library():
    wcapi = API(
        url="https://turistik.com", # Your store URL
        consumer_key=WC_CLIENT_KEY, # Your consumer key
        consumer_secret=WC_CLIENT_SECRET, # Your consumer secret
        wp_api=True, # Enable the WP REST API integration
        version="wc/v3", # WooCommerce WP REST API version,,
        timeout=30,
        tbp = "tr101"
    )
    pm = f"per_page=10"
    response = wcapi.get(f"products?{pm}")
    return response


def wc_list_products_request():
    basic_auth = HTTPBasicAuth(WC_CLIENT_KEY,WC_CLIENT_SECRET)
    payload = {"per_page": 10}
    headers = {"tbp": "tr101"}
    response = requests.get("https://turistik.com/wp-json/wc/v3/products", auth=basic_auth, params=payload, headers=headers)
    return response


def wc_list_products_pa():
    response = requests.get(PA_WC_PRODUCTS)
    return response


@app.timer_trigger(schedule="0 25 20,23 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def enviar_recordatorios(myTimer: func.TimerRequest) -> None:
    # Actualizar Productos
    logging.info("Actualizar productos WooComnmerce...")
    try:
        get_products()
    except Exception as e:
        logging.error(f"No se pudo actualizar los productos: {type(e).__name__}")
    # Crear imágenes
    logging.info("Convertir y almacenar imágenes...")
    try:
        crear_imagenes_wp()
    except Exception as e:
        logging.error(f"No se pudo convertir y/o almacenar imágenes: {type(e).__name__}")
    # Generar traducciones
    logging.info("Generar traducciones de textos...")
    try:
        translate_wc_values()
    except Exception as e:
        logging.error(f"No se pudo traducir y/o almacenar traducciones: {type(e).__name__}")   
    # Enviar notificaciones
    fecha = (datetime.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    logging.info(f"Enviar notificaciones: {fecha}...")
    try:
        send_notifications(fecha)
        logging.info("Notificaciones enviadas")
    except Exception as e:
        logging.error(f"No se pudo enviar notificaciones: {type(e).__name__}")


@app.route(route="enviar_recordatorio_manual", auth_level=func.AuthLevel.FUNCTION)
def enviar_recordatorio_manual(req: func.HttpRequest) -> func.HttpResponse:
    # Actualizar Productos
    logging.info("Actualizar productos WooComnmerce...")
    try:
        get_products()
    except Exception as e:
        logging.error(f"No se pudo actualizar los productos: {type(e).__name__}")
    # Crear imágenes
    logging.info("Convertir y almacenar imágenes...")
    try:
        crear_imagenes_wp()
    except Exception as e:
        logging.error(f"No se pudo convertir y/o almacenar imágenes: {type(e).__name__}")
    # Generar traducciones
    logging.info("Generar traducciones de textos...")
    try:
        translate_wc_values()
    except Exception as e:
        logging.error(f"No se pudo traducir y/o almacenar traducciones: {type(e).__name__}")   
    # Enviar notificaciones
    fecha = req.params.get("service_date")
    logging.info(f"Enviar notificaciones: {fecha}...")
    try:
        send_notifications(fecha)
        reply = "Notificaciones enviadas"
        cod = 200
    except Exception as e:
        logging.error(f"No se pudo enviar notificaciones: {type(e).__name__}")
        reply = f"No se pudo enviar: {type(e).__name__}"
        cod = 500
    return func.HttpResponse(reply, status_code=cod)
    

@app.route(route="test_domains_insert", auth_level=func.AuthLevel.FUNCTION)
def test_domains_insert(req: func.HttpRequest) -> func.HttpResponse:
    try:
        service_date = req.params.get("service_date")
        check_quotas(service_date)
        reply = "Test ok"
        cod = 200
    except Exception as e:
        logging.error(f"Test failed: {type(e).__name__}")
        reply = f"Test failed: {type(e).__name__}"
        cod = 500
    return func.HttpResponse(reply, status_code=cod)


@app.timer_trigger(schedule="0 2 * * *", arg_name="myTimer", run_on_startup=False,
              use_monitor=False) 
def update_domains_timer(myTimer: func.TimerRequest) -> None:
    try:
        update_domains()
        reply = "Dominios actualizados"
        logging.info(reply)
    except Exception as e:
        logging.error(f"No se pudo actualizar: {type(e).__name__}")


@app.route(route="test_translations", auth_level=func.AuthLevel.FUNCTION)
def test_translations(req: func.HttpRequest) -> func.HttpResponse:
    try:
        translate_wc_values()
        reply = "Translations ok"
        cod = 200
    except Exception as e:
        logging.error(f"Translations failed: {type(e).__name__}")
        reply = f"Translations failed: {type(e).__name__}"
        cod = 500
    return func.HttpResponse(reply, status_code=cod)

@app.route(route="http_trigger", auth_level=func.AuthLevel.FUNCTION)
def notificacion_cyt_traslado(req: func.HttpRequest) -> func.HttpResponse:
    try:
        envio = notificacion_traslado_cyt()
        reply = json.dumps(envio)
        cod = 200
    except Exception as e:
        logging.error(f"Notificaciones CyT Error: {type(e).__name__}")
        reply = f"Notificaciones CyT Error: {type(e).__name__}"
        cod = 500
    return func.HttpResponse(reply, status_code=cod)


@app.route(route="registrar_clientes")
def registrar_clientes(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    userid = req_body.get('userid')
    telefono = req_body.get('telefono')
    valoracion = req_body.get('valoracion')

    SQL_SERVER = os.getenv('SQL_SERVER')
    SQL_DATABASE = os.getenv('SQL_DATABASE')
    SQL_USERNAME = os.getenv('SQL_USERNAME')
    SQL_PASSWORD = os.getenv('SQL_PASSWORD')
    SQL_DRIVER = os.getenv('SQL_DRIVER')
    connection_string = f"Driver={{{SQL_DRIVER}}};Server={SQL_SERVER};Database={SQL_DATABASE};UID={SQL_USERNAME};PWD={SQL_PASSWORD};"

    conn = odbc.connect(connection_string)
    cursor = conn.cursor()
    try:
        cursor.execute("INSERT INTO btmkr.leads_fotos (userid, telefono, valoracion, createdAt) VALUES (?, ?, ?, GETDATE())", (userid, telefono, valoracion))
        conn.commit()
        return func.HttpResponse("Cliente registrado exitosamente.", status_code=200)
    except Exception as e:
        logging.error(f"Error al registrar cliente: {e}")
        return func.HttpResponse("Error al registrar cliente.", status_code=500)
    finally:
        cursor.close()
        conn.close()


@app.route(route="update_tren", auth_level=func.AuthLevel.FUNCTION)
def update_tren(req: func.HttpRequest) -> func.HttpResponse:
    from update_leads_tren import actualizar_tren
    try:
        payload = actualizar_tren()
        return func.HttpResponse(
        body=json.dumps(payload, ensure_ascii=False),
        status_code=200,
        mimetype="application/json"  # sets Content-Type: application/json; charset=utf-8
    )
    except Exception as e:
        logging.error(f"Error al actualizar leads: {e}")
        return func.HttpResponse("Error al actualizar leads.", status_code=500)


@app.route(route="update_imagenes", auth_level=func.AuthLevel.FUNCTION)
def update_imagenes(req: func.HttpRequest) -> func.HttpResponse:
    try:
        crear_imagenes_wp()
        return func.HttpResponse("Imágenes actualizadas exitosamente.", status_code=200)
    except Exception as e:
        logging.error(f"Error al actualizar imágenes: {e}")
        return func.HttpResponse("Error al actualizar imágenes.", status_code=500)