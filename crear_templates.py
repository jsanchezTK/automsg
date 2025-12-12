import requests
import os

BTMKR_ACCESS_TOKEN = os.getenv("BTMKR_ACCESS_TOKEN")

def create_template(tp_name: str, phone_number: str, bot_name: str, category: str, opt_in_image: str,
                    header_format: str, header_text: str, header_url: str,
                    template_text: str, footer_text: str, locale: str, buttons=None):
    if buttons is None:
        buttons = []
    global BTMKR_ACCESS_TOKEN
    url = "https://api.botmaker.com/v2.0/whatsapp/templates"
    # Required payload
    payload = {
        "name": tp_name,
        "phoneLineNumber": phone_number,
        "botName": bot_name,
        "category": category,
        "locale": locale,
        "optInImage": opt_in_image,
        "header": {
            "format": header_format,
            "text": header_text,
            "exampleURL": header_url
        },
        "body": {"text": template_text},
        "footer": {"text": footer_text}
    }
    # optional payload
    if buttons:
        payload["buttons"] = buttons
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "access-token": BTMKR_ACCESS_TOKEN
    }
    response = requests.post(url, json=payload, headers=headers)
    return response.json()


def create_template_recordatorio_excursiones():
    # Recordatorio de excursiones
    TEMP = {
        "POR": 
            "📢 *Entramos em contato pela TURISTIK, Operadora de Transporte para o Centro do Vinho da Vinícola Concha y Toro* 🍷\n\n"
            "Lembre-se de que o horário indicado na sua reserva corresponde ao início do tour na vinícola.\n"
            "🕑 Você deve embarcar no ônibus da Turistik em qualquer um dos 2 pontos de encontro (verifique o horário de partida):\n\n"
            "📍 *Pontos de encontro:*\n"
            "*Parque Arauco:* Av. Kennedy Lateral 5059, Las Condes (https://maps.app.goo.gl/kPnDXKuH1F2Rq3STA)\n"
            "*Plaza de Armas:* Monjitas 821, Santiago (https://maps.app.goo.gl/BNeEAHo4Rgx5zD2S6)\n\n"
            "🕓 *Exemplo:* Se o seu tour na vinícola começa às 10:10 hrs, o ônibus sai às 7:45 da Plaza de Armas e às 8:15 do Parque Arauco.\n\n"
            "⏱ Chegue pelo menos 10 minutos antes do horário de saída do ônibus.\n\n"
            "📸 Na imagem em anexo, você pode ver os horários de saída dos ônibus de cada ponto de encontro.\n\n"
            "Se tiver dúvidas, escreva para nós! Esperamos por você para viver uma experiência incrível! 🍇🥂"
        ,
        "ENG": 
            "📢 *This is a message from TURISTIK, Transport Operator to the Wine Center of Concha y Toro Winery*🍷\n\n"
            "Please remember that the time shown on your booking corresponds to the start of the tour at the winery.\n"
            "🕑 You must board the Turistik bus from either of our 2 meeting points (check the exact departure times):\n\n"
            "📍 *Meeting points:*\n"
            "*Parque Arauco:* Av. Kennedy Lateral 5059, Las Condes (https://maps.app.goo.gl/kPnDXKuH1F2Rq3STA)\n"
            "*Plaza de Armas:* Monjitas 821, Santiago (https://maps.app.goo.gl/BNeEAHo4Rgx5zD2S6)\n\n"
            "🕓 *Example:* If your winery tour starts at 10:10 hrs, your bus will depart at 7:45 from Plaza de Armas and 8:15 from Parque Arauco.\n\n"
            "⏱ Please arrive at least 10 minutes before your bus departure time.\n\n"
            "📸 In the attached image, you can see the departure times from each meeting point.\n\n"
            "If you have any questions, feel free to write us! We look forward to sharing an amazing experience with you! 🍇🥂"
        ,
        "ESP": 
            "📢 *Te contactamos desde TURISTIK, Operador de Transporte al Centro del Vino de la Viña Concha y Toro*🍷\n\n"
            "Recuerda que la hora indicada en tu reserva corresponde al inicio del tour en la viña.\n"
            "🕑 Debes tomar el bus de Turistik desde cualquiera de los 2 puntos de encuentro (Verifica el horario)\n\n"
            "📍 Punto de encuentro:\n"
            "*Parque Arauco:* Av. Kennedy Lateral 5059, Las Condes (https://maps.app.goo.gl/kPnDXKuH1F2Rq3STA)\n"
            "*Plaza de Armas:* Monjitas 821, Santiago (https://maps.app.goo.gl/BNeEAHo4Rgx5zD2S6)\n\n"
            "🕓 *Ejemplo:* Si tu tour  en la viña es a las 10:10 hrs, tu bus sale a las 7:45 de Plaza de Armas y 8:15 Parque Arauco hrs.\n\n"
            "⏱ Llega al menos 10 minutos antes de la salida del bus.\n\n"
            "📸 En la imagen adjunta puedes ver los horarios de salida de los buses desde cada punto de encuentro.\n\n"
            "Si tienes dudas, ¡escríbenos! ¡Te esperamos para vivir una gran experiencia! 🍇🥂"
        
    }

    WHATSAPP_LINE_NUMBER = "56957661080"
    NOMBRE_BASE = "traslado_cyt"
    CATEGORY = "UTILITY"
    OPT_IN_IMG_URL = "https://devturistik.blob.core.windows.net/files/opt_in_image.png"
    HEADER_TYPE = "IMAGE"
    HEADER_TEXT = ""
    HEADER_URL = "https://automsg.blob.core.windows.net/files/horarios_cyt.png"
    BTN_DATA = [
        {"INTENT_ID": "turistik-wvmoaokrj0@b.m-1731438808238",
            "TEXT": {"ESP": "Confirmar", "POR": "Confirmar", "ENG": "Confirm"}},
        {"INTENT_ID": "turistik-ahiw81igiw@b.m-1731438834171",
            "TEXT": {"ESP": "Hablar con un agente", "POR": "Fale com agente", "ENG": "Speak with an agent"}}
    ]
    FOOTER = "Turistik Chile"
    VERSION = "2"
    LANGUAGES = ["ESP", "POR", "ENG"]
    BOT = "Notificaciones"
    for LANG in LANGUAGES:
        nombre_template = f"{NOMBRE_BASE}_{LANG}_{VERSION}".lower()
        texto_template = TEMP[LANG]
        BUTTONS = []
        for btn in BTN_DATA:
            this_button = {
                "type": "QUICK_REPLY",
                "text": btn["TEXT"][LANG],
                "intentIdOrName": btn["INTENT_ID"]
            }
            BUTTONS.append(this_button)
        auth = input(f"Va a crear el siguiente template: {nombre_template}. Confirmar Y/N:\n")
        if auth.upper() == "Y":
            print(create_template(tp_name=nombre_template, phone_number=WHATSAPP_LINE_NUMBER, bot_name=BOT,
                                    category=CATEGORY, opt_in_image=OPT_IN_IMG_URL, header_format="IMAGE",
                                    header_text="", header_url=HEADER_URL, template_text=texto_template, buttons=BUTTONS,
                                    footer_text=FOOTER))
            

def create_template_cross_selling():
    # Recordatorio de excursiones
    TEMP = {
        "POR": 
            """Olá! Falamos com você da agência TURISTIK 🇨🇱
Esperamos que você tenha aproveitado sua experiência conosco ontem! 🌟

Como forma de agradecimento, queremos oferecer a você um desconto especial e exclusivo para a sua próxima reserva pelo WhatsApp 🎁

Diga-nos: sobre qual destino você gostaria de receber informações?
Costa 🏝 - Cordilheira 🏔 - Vinhedos 🍇 - Safári 🦁 - Cidade 🌆 """
        ,
        "ENG": 
            """"Hello! We're reaching out to you from TURISTIK agency 🇨🇱
We hope you enjoyed your experience with us yesterday! 🌟

As a thank you, we'd like to offer you a special and exclusive discount for your next booking via WhatsApp 🎁

Tell us, which destination would you like to receive information about?
Coast 🏝 - Mountains 🏔 - Vineyards 🍇 - Safari 🦁 - City 🌆"""
        ,
        "ESP": 
            """¡Hola! Te hablamos desde de la agencia TURISTIK 🇨🇱
¡Esperamos que hayas disfrutado tu experiencia con nosotros ayer! 🌟

Como agradecimiento, queremos ofrecerte un descuento especial y exclusivo para tu próxima reserva por WhatsApp 🎁

Cuéntame, ¿sobre qué destino te gustaría recibir información?
Costa 🏝 - Cordillera 🏔 - Viñedos 🍇- Safari🦁- Ciudad🌆
"""
        
    }

    WHATSAPP_LINE_NUMBER = "56962606008"
    NOMBRE_BASE = "cross_selling"
    CATEGORY = "MARKETING"
    OPT_IN_IMG_URL = "https://devturistik.blob.core.windows.net/files/opt_in_image.png"
    HEADER_TYPE = "IMAGE"
    HEADER_TEXT = ""
    HEADER_URL = "https://automsg.blob.core.windows.net/files/icono_turistik.png"
    BTN_DATA = [
        {"INTENT_ID": "turistik-284tlnq01m@b.m-1765484384655",
            "TEXT": {"ESP": "¡Quiero mi descuento!", "POR": "Quero meu desconto!", "ENG": "I want my discount!"}},
        {"INTENT_ID": "turistik-39x6nq5n75@b.m-1765484860945",
            "TEXT": {"ESP": "No estoy interesado", "POR": "Não estou interessado", "ENG": "I'm not interested."}}
    ]
    FOOTER = "Turistik Chile"
    VERSION = "0_3"
    LANGUAGES = ["ESP", "POR", "ENG"]
    idiomas = {"ESP": "es_ES", "POR": "pt_BR", "ENG": "en_US"}
    BOT = "Notificaciones"
    for LANG in LANGUAGES:
        nombre_template = f"{NOMBRE_BASE}_{LANG}_{VERSION}".lower()
        texto_template = TEMP[LANG]
        locale = idiomas[LANG]
        BUTTONS = []
        for btn in BTN_DATA:
            this_button = {
                "type": "QUICK_REPLY",
                "text": btn["TEXT"][LANG],
                "intentIdOrName": btn["INTENT_ID"]
            }
            BUTTONS.append(this_button)
        auth = input(f"Va a crear el siguiente template: {nombre_template}. Confirmar Y/N:\n")
        if auth.upper() == "Y":
            print(create_template(tp_name=nombre_template, phone_number=WHATSAPP_LINE_NUMBER, bot_name=BOT,
                                    category=CATEGORY, opt_in_image=OPT_IN_IMG_URL, header_format="IMAGE",
                                    header_text="", header_url=HEADER_URL, template_text=texto_template, buttons=BUTTONS,
                                    footer_text=FOOTER, locale=locale))
            

if __name__ == "__main__":
    create_template_cross_selling()