import pyodbc as odbc
import os

SQL_SERVER = os.getenv("SQL_SERVER")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USERNAME = os.getenv("SQL_USERNAME")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER")
CONNECTION_STRING = f"driver={{{SQL_DRIVER}}}; server={SQL_SERVER}; database={SQL_DATABASE}; UID={SQL_USERNAME}; PWD={SQL_PASSWORD}"


# get chat botmaker
def get_chat_botmaker(chatReference):
    import requests
    url = f"https://api.botmaker.com/v2.0/chats/{chatReference}"
    botmaker_token = os.getenv("BTMKR_ACCESS_TOKEN")
    headers = {
        "Accept": "application/json",
        "access-token": botmaker_token
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        print(f"Error fetching chat: {response.status_code} - {response.text}")
        return None

# obtener leads desde la base de datos
def actualizar_tren():
    conn = odbc.connect(CONNECTION_STRING)
    cursor = conn.cursor()
    sp_query = """exec [btmkr].[traspaso_leads_fotos]"""
    cursor.execute(sp_query)
    conn.commit()
    query = """select * from [btmkr].[leads_fotos_info]
    where json_data is null and id_lead >= 68 and userid not in ('XTTEUVS650PSQ6GZDN20', 'EGBY86EZO2P788O2FIZL')
    order by id_lead"""
    cursor.execute(query)
    leads = cursor.fetchall()
    correctos = 0
    erroneos = 0
    fallidos = []
    for lead in leads:
        lead_id = lead.id_lead
        chat_reference = lead.userid
        print(f"Processing lead {lead_id} with chat reference {chat_reference}")
        chat_data = get_chat_botmaker(chat_reference)
        if chat_data:
            update_query = """UPDATE [btmkr].[leads_fotos_info]
                            SET json_data = ?, updatedAt = GETDATE()
                            WHERE id_lead = ?"""
            cursor.execute(update_query, chat_data, lead_id)
            conn.commit()
            print(f"Updated lead {lead_id} with chat data.")
            correctos += 1
        else:
            print(f"Failed to update lead {lead_id} due to missing chat data.")
            erroneos += 1
            fallidos.append((lead_id, chat_reference))
    # Close the cursor and connection
    cursor.close()
    conn.close()
    print("Finished processing leads.")
    detalle = {
        "Correctos": correctos,
        "Fallidos": erroneos,
        "Detalle Fallidos": fallidos
    }
    return detalle
