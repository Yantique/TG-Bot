from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from datetime import datetime
from time import sleep
from bot import Userbot

import requests
import asyncio
import random
import os.path


# Global params
SPREADSHEET_ID = '1xzDikfjhqYAiElgAfK4o0Z8e6Wf0RxD_yirg72sFAp8'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
ROWS = 1000
START_CHATTING_DELAY = 30
JOINING_DELAY = 120  # (2 min) задержка между вступлениями в группы
MAILING_DELAY = 900  # (15 min) задержка между повторными отправками в секундах
DEFAULT_NAME = 'Artem'
REPEAT = False
ACTIVE_ACCOUNTS = {}


def setup(sheet):
    global ROWS, START_CHATTING_DELAY, JOINING_DELAY, MAILING_DELAY, DEFAULT_NAME, REPEAT
    cell_range = f'settings!A1:B{ROWS}'
    rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    params = {}
    for row in rows:
        param, value = row
        params[param] = value
    ROWS = int(params["ROWS"])
    START_CHATTING_DELAY = int(params["START_CHATTING_DELAY"])
    JOINING_DELAY = int(params["JOINING_DELAY"])
    MAILING_DELAY = int(params["MAILING_DELAY"])
    DEFAULT_NAME = params["DEFAULT_NAME"]
    REPEAT = params["REPEAT"]
    if REPEAT == 'True':
        REPEAT = True
    else:
        REPEAT = False


async def auth(sheet):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Logging into accounts")
    cell_range = f'accounts!A1:K{ROWS}'
    rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values']
    status = {'values': [['Validating']] * (len(rows) - 1)}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K2", valueInputOption="RAW",
                          body=status).execute()
    status = {'values': [['Status']]}
    for i, bot in enumerate(rows[1:]):
        phone_number, password, api_id, api_hash = bot[:4]
        proxy = bot[9]

        print(f"Account #{i + 1}")
        print(f'Phone number: +{phone_number}')
        ACTIVE_ACCOUNTS[phone_number] = Userbot(phone_number, api_id, api_hash, password, proxy)
        acc = ACTIVE_ACCOUNTS[phone_number]
        result = await acc.start_session()
        if result != 'Account is active':
            ACTIVE_ACCOUNTS.pop(phone_number)
        status['values'].append([str(result)])
        print(result, '\n')
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K1", valueInputOption="RAW",
                          body=status).execute()
    print('[auth] Done!')


def proxy_distribution(sheet):
    # Getting the proxy list
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Proxy distribution")
    cell_range = f'proxy!A1:B{ROWS}'
    proxies = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    proxies = list(map(lambda x: x[0], proxies))

    # Getting the list of bots
    cell_range = f'accounts!A1:A{ROWS}'
    bots = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    status = {'values': [['Proxy distribution']] * len(bots)}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K2", valueInputOption="RAW",
                          body=status).execute()
    # Setting up a proxy
    cell_range = f'accounts!J1:J{ROWS}'
    active_proxies = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    active_proxies += [[] for _ in range(len(bots) - len(active_proxies))]
    proxy_list = []
    for i in range(len(bots)):
        if active_proxies[i]:
            proxy_list.append(active_proxies[i])
        else:
            proxy_list.append([random.choice(proxies)])

    # Update sheet
    proxies = {'values': proxy_list}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!J2", valueInputOption="RAW",
                          body=proxies).execute()
    status = {'values': [['Proxy is set']] * len(bots)}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K2", valueInputOption="RAW",
                          body=status).execute()
    print(f"[proxy_distribution]: Done!")


async def acc_distribution(sheet):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting to join groups")
    cell_range = f'chats!A1:E{ROWS}'
    chats = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    status = {'values': [[f'Status ({datetime.date(datetime.now())})']]}
    chat_ids = {'values': [['Chat id']]}
    bots = {'values': [['Bot']]}
    prev = None
    for row_number, chat in enumerate(chats):
        link, chat_id, bot = chat[0:3]
        if len(chat) > 4:
            current_status = chat[4]
        else:
            current_status = 'default'
        if len(bot) == 0 or current_status == 'Manual select':
            if len(bot) == 0:
                bot = random.choice(list(ACTIVE_ACCOUNTS.keys()))
            acc = ACTIVE_ACCOUNTS[bot]
            if bot == prev:
                await asyncio.sleep(random.randint(0, JOINING_DELAY))
            else:
                await asyncio.sleep(30)
            result = await acc.join_chat(link)
            if type(result) == int:
                chat_ids['values'].append([result])
                bots['values'].append([bot])
                is_captcha = await acc.detect_captcha(link)
                if is_captcha == 'CAPTCHA':
                    status['values'].append(['CAPTCHA'])
                else:
                    status['values'].append(['Waiting for mailing'])
            else:
                status['values'].append([str(result)])
                chat_ids['values'].append([])
                bots['values'].append([])
        else:
            status['values'].append([current_status])
            chat_ids['values'].append([chat_id])
            bots['values'].append([bot])
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {bot}: {link} - {status['values'][-1][0]}")
        prev = bot
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="chats!B1", valueInputOption="RAW", body=chat_ids).execute()
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="chats!C1", valueInputOption="RAW", body=bots).execute()
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="chats!E1", valueInputOption="RAW", body=status).execute()
    print(f"[acc_distribution]: Done!")


async def setup_acc(sheet):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting to set up accounts")
    cell_range = f'accounts!A1:K{ROWS}'
    bots = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    photos = {'values': [['Photo']]}
    status = {'values': [['Status']]}
    for row_number, bot in enumerate(bots):
        phone_number = bot[0]
        username, first_name, last_name, bio, photo = bot[4:9]
        acc_status = bot[10]
        if acc_status == 'Account is active':
            acc = ACTIVE_ACCOUNTS[phone_number]
            try:
                await acc.set_user_profile(username=username, first_name=first_name, last_name=last_name, bio=bio)
                if len(photo) > 0:
                    result = get_photo(photo)
                    if result == 'Success':
                        await acc.set_user_profile(photo_path=f"temp/photo.{photo.split('.')[-1]}")
                    else:
                        status['values'].append([result])
            except Exception as e:
                status['values'].append([str(e)])
        else:
            status['values'].append([acc_status])
        photos['values'].append([''])
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!I1", valueInputOption="RAW",
                          body=photos).execute()
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K1", valueInputOption="RAW",
                          body=status).execute()
    print(f"[setup_acc]: Done!")


async def send_messages(sheet):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Starting mailing")
    prev = None
    cell_range = f'text!A1:B{ROWS}'
    messages = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    messages = array_to_dict(messages)
    cell_range = f'chats!A1:E{ROWS}'
    chats = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    for row_number, chat in enumerate(chats):
        chat_id, bot_number, message_type, status = chat[1:]
        link = chat[0]
        if (status == 'Waiting for mailing' or status == 'Message sent' or status == 'Skipped') and message_type != 'Skip':
            acc = ACTIVE_ACCOUNTS[bot_number]
            name = await acc.get_my_name()
            if name is None:
                name = ''
            if bot_number == prev:
                await asyncio.sleep(random.randint(0, START_CHATTING_DELAY))
            else:
                await asyncio.sleep(30)
            result = await acc.send_message(link, messages[message_type].replace(DEFAULT_NAME, name))
            status = {'values': [[str(result)]]}
            sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=f"chats!E{row_number + 2}", valueInputOption="RAW",
                                  body=status).execute()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {bot_number}: {link} - {str(result)}")
            prev = bot_number
        elif message_type == 'Skip':
            status = {'values': [['Skipped']]}
            sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=f"chats!E{row_number + 2}",
                                  valueInputOption="RAW",
                                  body=status).execute()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] {bot_number}: {link} - Skipped")
            await asyncio.sleep(1.1)
    print(f"[send_messages]: Done!")


def array_to_dict(messages):
    d = dict()
    for row in messages:
        d[row[0]] = row[1]
    return d


def get_photo(link):
    try:
        response = requests.get(link)
        if response.status_code == 200:
            file_name = f"photo.{link.split('.')[-1]}"
            with open(f"temp/{file_name}", 'wb') as file:
                file.write(response.content)
            return "Success"
        else:
            return "Download failed"
    except requests.exceptions.MissingSchema:
        return "Incorrect URL"
    except requests.exceptions.ConnectionError:
        return "Incorrect URL"


def main():
    creds = None
    if os.path.exists('Credentials/token.json'):
        creds = Credentials.from_authorized_user_file('Credentials/token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'Credentials/credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('Credentials/token.json', 'w') as token:
            token.write(creds.to_json())
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    setup(sheet)
    proxy_distribution(sheet)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(auth(sheet))
    loop.run_until_complete(setup_acc(sheet))
    loop.run_until_complete(acc_distribution(sheet))
    while True:
        loop.run_until_complete(send_messages(sheet))
        if REPEAT:
            sleep(MAILING_DELAY)
            continue
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] The mailing is over")
            break


if __name__ == '__main__':
    main()
