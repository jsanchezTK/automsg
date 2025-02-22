# Register this blueprint by adding the following line of code 
# to your entry point file.  
# app.register_functions(gpt_translate) 
# 
# Please refer to https://aka.ms/azure-functions-python-blueprints


import azure.functions as func
import logging
from openai import OpenAI
import json
import traceback
import os
import pyodbc as odbc
import time

gpt_translate = func.Blueprint()

client = OpenAI()

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")
CONNECTION_STRING = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"


def translation(txt: str) -> dict:
    completion = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": """Translate the provided text from spanish into Portuguese and English. Return a JSON with both translations and original text in spanish"""},
            {
                "role": "user",
                "content": txt
            }
        ],
        response_format={
            "type": "json_schema",
            "json_schema": {
                "name": "translation_chema",
                "schema": {
                    "type": "object",
                    "properties": {
                        "spanish": {
                            "description": "Original text in spanish.",
                            "type": "string"
                        },
                        "portuguese": {
                            "description": "Portuguese translation.",
                            "type": "string"
                        },
                        "english": {
                            "description": "English translation.",
                            "type": "string"
                        },
                        "additionalProperties": False
                    }
                }
            }
        }
    )
    reply = completion.choices[0].message.content
    tokens = completion.usage.total_tokens
    # json_start = reply.index("{")
    # json_end = reply.index("}")+1
    # dict_translation = json.loads(reply[json_start:json_end])
    dict_translation = json.loads(reply)
    dict_translation["total_tokens"] = tokens
    return dict_translation


# @gpt_translate.route(route="translate_wc_values", auth_level=func.AuthLevel.ANONYMOUS)
# def translate_wc_values(req: func.HttpRequest) -> func.HttpResponse:
def translate_wc_values() -> None:
    try:
        logging.info("Setting connection...")
        conn = odbc.connect(CONNECTION_STRING)
        logging.info("Creating cursor...")
        cursor = conn.cursor()
        logging.info("Cleaning wrong translations...")
        update_query_0 = """update u
                            set u.id_traduccion = null
                            from [OzyTrip].[stagingServiciosWooCommerceMetaData] u
                                inner join OzyTrip.stagingServiciosWooCommerceMetadataTraducciones s on
                                    u.id_traduccion = s.id_traduccion
                            where replace(wc_value,'&amp;','&') != txt_raw"""
        cursor.execute(update_query_0)
        cursor.commit()
        fetch_query = """select distinct wc_value from [OzyTrip].[stagingServiciosWooCommerceMetaData]
                        where 
                            id_traduccion is null and (
                                wc_key like '%incluye%'
                            or	wc_key like '%recomendacion%')"""
        logging.info(f"Executing query:\n{fetch_query}")
        cursor.execute(fetch_query)
        texts = cursor.fetchall()
        success_count = 0
        error_count = 0
        logging.info("Data collected succesfully.")
        for row in texts:
            logging.info("Next string...")
            try:
                input_string = row.wc_value
                logging.info(input_string)
                dict_output = translation(input_string)
                sp = dict_output["spanish"].replace("'", "''")
                po = dict_output["portuguese"].replace("'", "''")
                en = dict_output["english"].replace("'", "''")
                tokens = dict_output["total_tokens"]
                insert_query = f"""insert into [OzyTrip].[stagingServiciosWooCommerceMetadataTraducciones]
                values ('{input_string}', '{sp}', '{po}','{en}', {tokens})"""
                cursor.execute(insert_query)
                update_query = """update u
                                set u.id_traduccion = s.id_traduccion
                                from [OzyTrip].[stagingServiciosWooCommerceMetaData] as u
                                    inner join [OzyTrip].[stagingServiciosWooCommerceMetadataTraducciones] as s on
                                        replace(wc_value,'&amp;','&') = txt_raw
                                where 
                                        wc_key like '%incluye%'
                                    or	wc_key like '%recomendacion%'"""
                cursor.execute(update_query)
                cursor.commit()
                success_count += 1
                time.sleep(1)
            except Exception as e:
                tipo_error = type(e).__name__
                logging.error(f"Could not translate: {tipo_error}")
                error_count += 1
        # input_string = req.params.get("txt")
        reply = f"Translations: {str(success_count)}, Aborted: {str(error_count)}."
        logging.info(reply)
        print(reply)
        cod = 200
    except Exception as e:
        tb_type = type(e).__name__
        tb = traceback.format_exc()
        additional_info = ""
        cod = 500
        reply = f"{tb_type}\n{tb}\n{additional_info}"
        logging.error(reply)
        raise type(e)
    finally:
        conn.close()
        # return func.HttpResponse(reply, status_code=cod)