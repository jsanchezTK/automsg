# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(get_template_data) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
import requests
from requests.auth import HTTPBasicAuth
import os
import pyodbc as odbc
import pandas as pd
import datetime
import time
import json
from woo_commerce_update import export_to_csv_with_pandas, bulk_insert

get_template_data = func.Blueprint()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")
SQL_DATABASE_TEMPLATES = os.getenv("SQL_DATABASE_TEMPLATES")
CONNECTION_STRING = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"
CONNECTION_STRING_TEMPLATES = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE_TEMPLATES}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"
BTMKR_ACCESS_TOKEN = os.getenv("BTMKR_ACCESS_TOKEN")


def standardize_phone_numbers(passenger_contacts):
    """
    Standardizes phone numbers in passenger contact information.

    Args:
        passenger_contacts (list): List of dictionaries containing passenger contact info.
                                   Each dictionary should include 'Country' and 'ContactNumber'.
        country_codes (dict): Dictionary where keys are country ISO codes, and values are area codes.

    Returns:
        list: A list of dictionaries with standardized phone numbers.
    """
    standardized_contacts = []

    # Obtener códigos
    url = "https://www.countrycode.org/"
    tables = pd.read_html(url)
    codigos = tables[1]
    country_codes = {}
    for index, i in codigos.iterrows():
        iso_code = i["ISO CODES"].split(" / ")[1]
        country_code = i["COUNTRY CODE"]
        country_codes[iso_code] = country_code

    for contact in passenger_contacts:
        iso_code = contact.get('Country')
        phone_number = contact.get('ContactNumber', '').replace(" ", "")  # Remove spaces
        country_area_code = country_codes.get(iso_code)

        if not iso_code or not phone_number or not country_area_code:
            # Skip contacts with missing information
            standardized_contacts.append(contact)
            continue

        # # Ensure area code starts with "+"
        # if not country_area_code.startswith("+"):
        #     country_area_code = f"+{country_area_code}"


        # Remove leading zeros and "+" from the phone number if it exists
        if phone_number.startswith("0"):
            phone_number = phone_number.lstrip("0")
        elif phone_number.startswith("+"):
            phone_number = phone_number.lstrip("+")

        # Add country area code if it's not already included
        if not phone_number.startswith(country_area_code):
            phone_number = f"{country_area_code}{phone_number}"

        # Update the contact with the standardized phone number
        standardized_contacts.append({**contact, "ContactNumber": phone_number})

    return standardized_contacts


# Obtener access token
def ozytrip_access_token():
    """Retorna el access token de OzyTrip"""
    DEV_ENVIRONMENT = os.getenv("DEV_ENVIRONMENT", 'False').lower() == 'true'
    if DEV_ENVIRONMENT:
        url = os.getenv("OT_AT_URL_DEV")
        # Basic authentication credentials
        username = os.getenv("OT_AT_USER_DEV")
        password = os.getenv("OT_AT_PASS_DEV")
    else:
        url = os.getenv("OT_AT_URL")
        # Basic authentication credentials
        username = os.getenv("OT_AT_USER")
        password = os.getenv("OT_AT_PASS")

    # Data to send in the POST request
    payload = {
        'grant_type': 'client_credentials',
        'scope': os.getenv("OT_AT_SCOPE"),
    }


    # Headers
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    # Making the POST request with basic authentication
    response = requests.post(url, data=payload, headers=headers, auth=HTTPBasicAuth(username, password))
    json_data = response.json()
    return json_data["access_token"]

# Obtener manifiesto
def get_manifest(date: str, access_token="") -> list:
    """Obtiene el manifiesto de notificaciones de OzyTrip, dada una fecha"""
    DEV_ENVIRONMENT = os.getenv("DEV_ENVIRONMENT", 'False').lower() == 'true'
    if DEV_ENVIRONMENT:
        # URL of the API endpoint
        base_url = os.getenv("OT_MANIFEST_URL_DEV")
        url = f"{base_url}/{date}/false"
    else:
        # URL of the API endpoint
        base_url = os.getenv("OT_MANIFEST_URL")
        url = f"{base_url}/{date}/false"

    # Access token
    if not access_token:
        access_token = ozytrip_access_token()

    # Headers, including Authorization for Bearer token
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Bearer {access_token}'
    }

    # Making the POST request with Bearer token authentication
    response = requests.get(url, headers=headers)

    return response.json()


def get_images() -> list:
    """Devuelve una lista de diccionarios que representa una tabla que contiene las columnas TourCode, img_png."""
    conn = odbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    query = """SELECT distinct [ozytrip_id]
                ,case
                    when [img_png] = '' then 'https://automsg.blob.core.windows.net/files/icono_turistik.png'
                    else [img_png] end as [img_png]
            FROM [OzyTrip].[stagingServiciosWooCommerce]
            where ozytrip_id != 'N/A'"""
    cursor.execute(query)
    data = cursor.fetchall()
    data_dict = [{"Tourcode": x.ozytrip_id, "img": x.img_png} for x in data]
    conn.close()
    return data_dict


def get_service_data() -> list:
    conn = odbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    service_data = []
    query = """select wc_key, ozytrip_id, txt_esp, txt_por, txt_eng from OzyTrip.stagingServiciosWooCommerceMetaData as md
left join OzyTrip.stagingServiciosWooCommerce as ss on md.service_id = ss.service_id
left join [OzyTrip].[stagingServiciosWooCommerceMetadataTraducciones] as td on md.id_traduccion = td.id_traduccion
where ozytrip_id != 'N/A' and (
wc_key like '%incluye%'
	or	wc_key like '%recomendacion%')"""
    cursor.execute(query)
    data = cursor.fetchall()
    tour_code_list = list(set([row.ozytrip_id for row in data]))

    # Consultas detalle
    for tour_code in tour_code_list:
        metadata = list(filter(lambda x: x.ozytrip_id == tour_code, data))
        incluye = list(filter(lambda x: "incluye" in x.wc_key and "no-incluye" not in x.wc_key, metadata))
        # incluye_esp = " • "+"\\n • ".join([x.txt_esp for x in incluye])+"\\n"
        # incluye_por = " • "+"\\n • ".join([x.txt_por for x in incluye])+"\\n"
        # incluye_eng = " • "+"\\n • ".join([x.txt_eng for x in incluye])+"\\n"
        incluye_esp = " • "+" • ".join([x.txt_esp for x in incluye])
        incluye_por = " • "+" • ".join([x.txt_por for x in incluye])
        incluye_eng = " • "+" • ".join([x.txt_eng for x in incluye])
        no_incluye = list(filter(lambda x: "no-incluye" in x.wc_key, metadata))
        # no_incluye_esp = " • "+"\\n • ".join([x.txt_esp for x in no_incluye])+"\\n"
        # no_incluye_por = " • "+"\\n • ".join([x.txt_por for x in no_incluye])+"\\n"
        # no_incluye_eng = " • "+"\\n • ".join([x.txt_eng for x in no_incluye])+"\\n"
        no_incluye_esp = " • "+" • ".join([x.txt_esp for x in no_incluye])
        no_incluye_por = " • "+" • ".join([x.txt_por for x in no_incluye])
        no_incluye_eng = " • "+" • ".join([x.txt_eng for x in no_incluye])
        recomendaciones = list(filter(lambda x: "recomendacion" in x.wc_key, metadata))
        # recomendaciones_esp = " • "+"\\n • ".join([x.txt_esp for x in recomendaciones])+"\\n"
        # recomendaciones_por = " • "+"\\n • ".join([x.txt_por for x in recomendaciones])+"\\n"
        # recomendaciones_eng = " • "+"\\n • ".join([x.txt_eng for x in recomendaciones])+"\\n"
        recomendaciones_esp = " • "+" • ".join([x.txt_esp for x in recomendaciones])
        recomendaciones_por = " • "+" • ".join([x.txt_por for x in recomendaciones])
        recomendaciones_eng = " • "+" • ".join([x.txt_eng for x in recomendaciones])
        this_row = {
            "Tourcode": tour_code,
            "includes_esp": incluye_esp,
            "not_includes_esp": no_incluye_esp,
            "recommendations_esp": recomendaciones_esp,
            "includes_por": incluye_por,
            "not_includes_por": no_incluye_por,
            "recommendations_por": recomendaciones_por,
            "includes_eng": incluye_eng,
            "not_includes_eng": no_incluye_eng,
            "recommendations_eng": recomendaciones_eng
        }
        service_data.append(this_row)
    conn.close()
    return service_data


def send_msg(campaign, channel_id, notification_name, template, contacts: list[dict]):
    url = "https://api.botmaker.com/v2.0/notifications"
    payload = {
        "campaign": campaign,
        "channelId": channel_id,
        "name": notification_name,
        "intentIdOrName": template,
        "contacts": contacts
    }
    headers = {
        "Content-Type": "application/json",
        "access-token": BTMKR_ACCESS_TOKEN
    }
    response = requests.post(url, json=payload, headers=headers)
    return response


def get_country_codes() -> pd.DataFrame:
    url = "https://www.countrycode.org/"
    tables = pd.read_html(url)
    codigos = tables[1]
    dict_table = codigos.to_dict(orient="records")
    corrected = [{"iso_code": x["ISO CODES"].split(" / ")[1], "code": x["COUNTRY CODE"].split(", ")[0]} for x in dict_table]
    df = pd.DataFrame.from_dict(corrected)
    return df


def test_fun(ip):
    return f"Este es tu driver: {ip}"


def get_domains(access_token="") -> dict:
    """Obtiene el manifiesto de notificaciones de OzyTrip, dada una fecha"""
    DEV_ENVIRONMENT = os.getenv("DEV_ENVIRONMENT", 'False').lower() == 'true'
    if DEV_ENVIRONMENT:
        # URL of the API endpoint
        url = os.getenv("OT_DOMAINS_URL_DEV")
    else:
        # URL of the API endpoint
        url = os.getenv("OT_DOMAINS_URL")

    # Access token
    if not access_token:
        access_token = ozytrip_access_token()

    # Headers, including Authorization for Bearer token
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Bearer {access_token}'
    }

    # Making the POST request with Bearer token authentication
    response = requests.get(url, headers=headers)

    return response.json()


def update_domains():
    domains = get_domains()
    domain_types = ["Servicios", "Escenarios de Cupo", "Puntos de Venta"]
    domain_properties = {
        "Servicios": {
            "headers": [
                "TourCode",
                "Aka",
                "Service",
                "Category",
                "ServiceType",
                "CostCenter",
                "ScenarioQuotaId",
                "ScenarioQuota"
            ],
            "bulk_table": "[OzyTrip].[bulkInsertDomainServicios]",
            "stored_procedure": "ozytrip.insertar_domain_servicios"
        },
        "Escenarios de Cupo": {
            "headers": [
                "ScenarioQuotaId",
                "ScenarioQuota",
                "IsSharedScenario",
                "Quota"

            ],
            "bulk_table": "[OzyTrip].[bulkInsertDomainScenarioQuota]",
            "stored_procedure": "[OzyTrip].[insertar_domain_quotas]"
        },
        "Puntos de Venta": {
            "headers": [
                "SalePoint",
                "Description",
                "Group"
            ],
            "bulk_table": "",
            "stored_procedure": ""
        }
    }
    for dom in domain_types:
        current_domain = domains[dom]
        sp = domain_properties[dom]["stored_procedure"]
        bt = domain_properties[dom]["bulk_table"]
        if sp:
            dom_headers = domain_properties[dom]["headers"]
            logging.info(export_to_csv_with_pandas(current_domain, dom_headers, f"{dom}.csv"))
            bulk_insert(f"{dom}.csv", bt, sp)
            logging.info(f"Domain {dom} inserted properly")


def check_quotas(service_date: str):
    limite_reservas = 5
    manifest = get_manifest(service_date)
    manifest = list(filter(lambda x: x["ServicesGrouping"] != "City Tour" and x["ServiceType"]=="Compartidos", manifest))
    conx = odbc.connect(CONNECTION_STRING)
    cursor = conx.cursor()
    query = """SELECT TourCode, ScenarioQuotaId, ScenarioQuota
FROM [OzyTrip].[domainServicios]
where Category != 'City Tour' and ServiceType = 'Compartidos'"""
    cursor.execute(query)
    tour_quota = {row.TourCode: row.ScenarioQuotaId for row in cursor.fetchall()}
    scenarios = {}
    for pax in manifest:
        scenario_id = tour_quota[pax["Tourcode"]]
        current_passengers = pax["PaxQuantity"]
        if scenario_id not in scenarios:
            scenarios[scenario_id] = current_passengers
        else:
            scenarios[scenario_id] += current_passengers
    query = f"""delete ozytrip.recordatorios_cancelables
    where fecha_servicio = '{service_date}'"""
    cursor.execute(query)
    cursor.commit()
    for sc in scenarios:
        if scenarios[sc] <= limite_reservas:
            query = f"""insert into ozytrip.recordatorios_cancelables
            values ({sc}, '{service_date}')"""
            cursor.execute(query)
            cursor.commit()
            logging.info(f"Servicio cancelable: {sc}")
    conx.close()
    


# @get_template_data.route(route="send_notifications", auth_level=func.AuthLevel.FUNCTION)
# def send_notifications(req: func.HttpRequest) -> func.HttpResponse:
def send_notifications(service_date) -> None:
    # service_date = req.params.get("service_date")
    pre_manifest = get_manifest(service_date)
    no_incluidos = ["ec0c8e9a-6465-418f-8fd7-ab0593cdb4c9"]
    check_quotas(service_date)
    conx = odbc.connect(CONNECTION_STRING)
    cursor = conx.cursor()
    query = f"""SELECT rca.[ScenarioQuotaId]
      ,ds.[TourCode]
	  ,rca.fecha_servicio
  FROM [OzyTrip].[recordatorios_cancelables] as rca

left join [OzyTrip].[domainServicios] as ds on rca.[ScenarioQuotaId] = ds.[ScenarioQuotaId]
left join ozytrip.recordatorios_contactados as rco on ds.TourCode = rco.tourcode and rca.fecha_servicio = rco.fecha_servicio

where rca.fecha_servicio = '{service_date}' and fecha_contacto is null

union 

SELECT dse.ScenarioQuotaId
	  ,rcon.[tourcode]
      ,[fecha_servicio]
  FROM [OzyTrip].[recordatorios_contactados] as rcon

left join OzyTrip.domainServicios as dse on rcon.tourcode = dse.TourCode

where rcon.fecha_servicio = '{service_date}'"""
    cursor.execute(query)
    cancelables = [row.TourCode for row in cursor.fetchall()]
    no_incluidos += cancelables
    manifest_filtered = list(filter(lambda x: x["ServicesGrouping"] != "City Tour" and x["ServiceType"] == "Compartidos" and x["Tourcode"] not in no_incluidos, pre_manifest))
    # servicios_envios = [
    #     "e3595a66-94dd-4509-a209-ac17296e721b",
    #     "a0bd6e32-98ec-4556-8d65-a5d99e58fdc7",
    #     "894d491a-6dd5-4878-aba2-3b98b4d0626a",
    #     "a0bd6e32-98ec-4556-8d65-a5d99e58fdc7",
    #     "ec0c8e9a-6465-418f-8fd7-ab0593cdb4c9",
    #     "a42b1bd8-a2d6-40c6-b9e0-b1bcf2251817",
    #     "6eb6d11c-9dee-414b-a6b1-83bb526963ef",
    #     "982067db-b3af-4e9f-9373-952bc071a051"
    # ]
    # manifest_filtered = list(filter(lambda x: x["Tourcode"] in servicios_envios,pre_manifest))
    manifest_filtered = standardize_phone_numbers(manifest_filtered)
    tour_info = pd.DataFrame(get_service_data())
    tour_img = pd.DataFrame(get_images())
    manifest = pd.DataFrame(manifest_filtered)
    manifest['ContactNumber'] = manifest['ContactNumber'].str.replace('+', '', regex=False)
    manifest = manifest[["Tourcode", "SaleId", "Service", "ServiceDate", "ServiceHour", "MeetingPoint", "MeetingPointAddress", "MeetingHour", "ContactName", "Language", "ContactNumber", "PaxQuantity"]]
    merged_data = manifest.merge(tour_info,on="Tourcode", how="inner").merge(tour_img, on="Tourcode", how="left")
    contactados = merged_data['Tourcode'].unique().tolist()
    manifest_esp = merged_data[merged_data["Language"] == "es"].to_dict(orient="records")
    manifest_por = merged_data[merged_data["Language"] == "pt"].to_dict(orient="records")
    manifest_eng = merged_data[merged_data["Language"] == "en"].to_dict(orient="records")

    logging.info("Separando listados...")
    # armar estructura
    contact_info_esp = []
    for i in manifest_esp:
        dir = i["MeetingPoint"]+" ("+i["MeetingPointAddress"]+")"
        this_contact = {
            "contactId": i["ContactNumber"],
            "variables": {
                "customer_name": i["ContactName"],
                "current_service": i["Service"],
                "service_pickup_place": dir,
                "service_pickup_time": i["MeetingHour"][:-3],
                "service_passenger_number": str(i["PaxQuantity"]),
                "weather_condition": "⛅ Parcialmente nublado, 22°C.",
                "headerImageUrl": i["img"],
                "service_includes": i["includes_esp"],
                "service_doesnt_include": i["not_includes_esp"],
                "service_recommendations": i["recommendations_esp"],
                "customer_sale_id": i["SaleId"]
            }
        }
        contact_info_esp.append(this_contact)

    contact_info_por = []
    for i in manifest_por:
        dir = i["MeetingPoint"]+" ("+i["MeetingPointAddress"]+")"
        this_contact = {
            "contactId": i["ContactNumber"],
            "variables": {
                "customer_name": i["ContactName"],
                "current_service": i["Service"],
                "service_pickup_place": dir,
                "service_pickup_time": i["MeetingHour"][:-3],
                "service_passenger_number": str(i["PaxQuantity"]),
                "weather_condition": "⛅ Parcialmente nublado, 22°C.",
                "headerImageUrl": i["img"],
                "service_includes": i["includes_por"],
                "service_doesnt_include": i["not_includes_por"],
                "service_recommendations": i["recommendations_por"],
                "customer_sale_id": i["SaleId"]
            }
        }
        contact_info_por.append(this_contact)

    contact_info_eng = []
    for i in manifest_eng:
        dir = i["MeetingPoint"]+" ("+i["MeetingPointAddress"]+")"
        this_contact = {
            "contactId": i["ContactNumber"],
            "variables": {
                "customer_name": i["ContactName"],
                "current_service": i["Service"],
                "service_pickup_place": dir,
                "service_pickup_time": i["MeetingHour"][:-3],
                "service_passenger_number": str(i["PaxQuantity"]),
                "weather_condition": "⛅ Parcialmente nublado, 22°C.",
                "headerImageUrl": i["img"],
                "service_includes": i["includes_eng"],
                "service_doesnt_include": i["not_includes_eng"],
                "service_recommendations": i["recommendations_eng"],
                "customer_sale_id": i["SaleId"]
            }
        }
        contact_info_eng.append(this_contact)
    
    whatsapp_channel_id = "turistik-whatsapp-56957661080"
    txt_output = ""
    response = send_msg(campaign="Recordatorios V1", channel_id=whatsapp_channel_id, notification_name="Recordatorios Español V1.2", template="recordatorio_esp_1_2", contacts=contact_info_esp)
    logging.info(str(response.content))
    txt_output += datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+str(response.content)
    txt_output.replace("b''", f"ESP: {len(contact_info_esp)} notificaciones enviadas.")
    time.sleep(2)
    response = send_msg(campaign="Recordatorios V1", channel_id=whatsapp_channel_id, notification_name="Recordatorios Portugués V1.2", template="recordatorio_por_1_2", contacts=contact_info_por)
    logging.info(str(response.content))
    txt_output += "\n"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+str(response.content)
    txt_output.replace("b''", f"POR: {len(contact_info_por)} notificaciones enviadas.")
    time.sleep(2)
    response = send_msg(campaign="Recordatorios V1", channel_id=whatsapp_channel_id, notification_name="Recordatorios Inglés V1.2", template="recordatorio_eng_1_2", contacts=contact_info_eng)
    logging.info(str(response.content))
    txt_output += "\n"+datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")+": "+str(response.content)
    txt_output.replace("b''", f"ENG: {len(contact_info_eng)} notificaciones enviadas.")

    for tc in contactados:
        query = f"""insert into ozytrip.recordatorios_contactados
        values ('{tc}', '{service_date}', CAST(CAST(GETUTCDATE() AS DATETIMEOFFSET) AT TIME ZONE 'Pacific SA Standard Time' AS DATETIME))"""
        cursor.execute(query)
        cursor.commit()
    conx.close()
    return func.HttpResponse(txt_output, status_code=200)


def registrar_contactados(pax_contactados: list) -> None:
    conx = odbc.connect(CONNECTION_STRING_TEMPLATES)
    cursor = conx.cursor()
    for pax in pax_contactados:
        notificado = pax["variables"]["customer_sale_id"]
        origen = "msg.notificaciones_no_contactados"
        uso = "Notificacion Traslado CyT"
        medio = "Botmaker/Whatsapp"
        query = f"exec msg.registrar_contactados '{notificado}', '{origen}', '{uso}', '{medio}'"
        cursor.execute(query)
        cursor.commit()
    conx.close()


def notificacion_traslado_cyt() -> list:
    """Envía notificaciones de Traslado CyT a los contactos que no han sido contactados previamente."""
    logging.info("Enviando notificaciones de Traslado CyT...")
    logging.info("Conectando a la base de datos...")
    conx = odbc.connect(CONNECTION_STRING_TEMPLATES)
    cursor = conx.cursor()
    query = "SELECT * FROM msg.notificaciones_no_contactados"
    logging.info(f"Ejecutando consulta:\n{query}")
    cursor.execute(query)
    rows = cursor.fetchall()
    contactos_esp = []
    contactos_por = []
    contactos_eng = []
    logging.info("Recopilando datos...")
    contactos_all = []
    for row in rows:
        idioma = row.Language
        phone_number = row.PhoneNumber
        customer_name = row.Name
        if row.LastName:
            customer_name += f" {row.LastName}"
        date = row.Date.strftime("%d/%m/%Y")
        time = row.Time[:-3]
        saleid = row.ozyTripSalesCode
        qty = row.QtyPax
        this_contact = {
            "contactId": phone_number,
            "variables": {
                "customer_name": customer_name,
                "customer_sale_id": saleid,
                "service_passenger_number": str(qty),
                "cyt_date": date,
                "cyt_time": time,
                "headerImageUrl": "https://automsg.blob.core.windows.net/files/horarios_cyt.png",
            }
        }
        ct_gen = {
            "contactId": phone_number,
            "customer_name": customer_name,
            "customer_sale_id": saleid,
            "service_passenger_number": str(qty),
            "cyt_date": date,
            "cyt_time": time,
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "language": idioma
        }
        contactos_all.append(ct_gen)
        if idioma == "ES":
            contactos_esp.append(this_contact)
        elif idioma == "PT":
            contactos_por.append(this_contact)
        elif idioma == "EN":
            contactos_eng.append(this_contact)
    logging.info(f"Contactos en español: {len(contactos_esp)}")
    logging.info(f"Contactos en portugués: {len(contactos_por)}")
    logging.info(f"Contactos en inglés: {len(contactos_eng)}")
    whatsapp_channel_id = "turistik-whatsapp-56957661080"
    if contactos_esp:
        response = send_msg(campaign="CYT TRASLADO V2", channel_id=whatsapp_channel_id, notification_name="CyT Traslado ESP V2", template="traslado_cyt_esp_2", contacts=contactos_esp)
        registrar_contactados(contactos_esp)
        logging.info("Notificación enviada a contactos en español.")
    if contactos_por:
        response = send_msg(campaign="CYT TRASLADO V2", channel_id=whatsapp_channel_id, notification_name="CyT Traslado POR V2", template="traslado_cyt_por_2", contacts=contactos_por)
        registrar_contactados(contactos_por)
        logging.info("Notificación enviada a contactos en portugués.")
    if contactos_eng:
        response = send_msg(campaign="CYT TRASLADO V2", channel_id=whatsapp_channel_id, notification_name="CyT Traslado ENG V2", template="traslado_cyt_eng_2", contacts=contactos_eng)
        registrar_contactados(contactos_eng)
        logging.info("Notificación enviada a contactos en inglés.")
    return contactos_all
        

@get_template_data.route(route="test_dataframe", auth_level=func.AuthLevel.FUNCTION)
def test_dataframe(req: func.HttpRequest) -> func.HttpResponse:
    service_date = req.params.get("service_date")
    pre_manifest = get_manifest(service_date) 
    # manifest_filtered = list(filter(lambda x: x["ServicesGrouping"] != "City Tour" and x["ServiceType"] == "Compartidos",pre_manifest))
    servicios_envios = [
        "e3595a66-94dd-4509-a209-ac17296e721b",
        "a0bd6e32-98ec-4556-8d65-a5d99e58fdc7",
        "894d491a-6dd5-4878-aba2-3b98b4d0626a",
        "a0bd6e32-98ec-4556-8d65-a5d99e58fdc7",
        "ec0c8e9a-6465-418f-8fd7-ab0593cdb4c9",
        "a42b1bd8-a2d6-40c6-b9e0-b1bcf2251817"
    ]
    manifest_filtered = list(filter(lambda x: x["Tourcode"] in servicios_envios,pre_manifest))
    tour_info = pd.DataFrame(get_service_data())
    tour_img = pd.DataFrame(get_images())
    manifest = pd.DataFrame(manifest_filtered)
    country_codes = get_country_codes()
    manifest['ContactNumber'] = manifest['ContactNumber'].str.replace('+', '', regex=False)
    manifest = manifest[["Tourcode", "SaleId", "Service", "ServiceDate", "ServiceHour", "MeetingPoint", "MeetingPointAddress", "MeetingHour", "ContactName", "Language", "ContactNumber", "PaxQuantity"]]
    merged_data = manifest.merge(tour_info,on="Tourcode", how="inner").merge(tour_img, on="Tourcode", how="left").merge(country_codes, left_on="Country", right_on="iso_code", how="left")
    manifest = merged_data.to_dict(orient="records")
    contact_info = []
    for i in manifest:
        dir = i["MeetingPoint"]+" ("+i["MeetingPointAddress"]+")"
        this_contact = {
            "contactId": i["ContactNumber"],
            "variables": {
                "customer_name": i["ContactName"],
                "current_service": i["Service"],
                "service_pickup_place": dir,
                "service_pickup_time": i["MeetingHour"][:-3],
                "service_passenger_number": str(i["PaxQuantity"]),
                "weather_condition": "⛅ Parcialmente nublado, 22°C.",
                "headerImageUrl": i["img"],
                "service_includes": i["includes_esp"],
                "service_doesnt_include": i["not_includes_esp"],
                "service_recommendations": i["recommendations_esp"],
                "customer_sale_id": i["SaleId"]
            }
        }
        contact_info.append(this_contact)
    return func.HttpResponse(json.dumps(contact_info), status_code=200)