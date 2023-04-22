from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from pyrogram import errors
from pyrogram import Client

from datetime import datetime
from time import sleep

import requests
import asyncio
import random
import os.path


# Global params
START_TIME = '12:00'
SPREADSHEET_ID = '1xzDikfjhqYAiElgAfK4o0Z8e6Wf0RxD_yirg72sFAp8'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SESSIONS_FOLDER = 'Sessions/'
JOINING_DELAY = 10  # Test param (value = 600)
START_CHATTING_DELAY = 10  # Test param (value = 72000)
STOP_CHATTING_DELAY = 15
ROWS = 1000
DEFAULT_NAME = 'Artem'
PROXY_LIST = []


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

    while True:
        if datetime.now().strftime("%H:%M") == START_TIME or True:  # Remove true in release version
            get_proxies(sheet)
            auth(sheet)
            asyncio.run(acc_distribution(sheet))
            asyncio.run(start_chatting(sheet))
        else:
            sleep(30)
        break


def auth(sheet):
    cell_range = f'accounts!A1:K{ROWS}'
    rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values']
    status = {'values': [['Validating'] * (len(rows) - 1)]}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K2", valueInputOption="RAW",
                          body=status).execute()
    photos = {'values': [['Photo']]}
    status = {'values': [['Status']]}
    for i, row in enumerate(rows[1:]):
        phone_number, password, api_id, api_hash = row[:4]
        username, first_name, last_name, bio, photo = row[4:9]
        proxy = transform_proxy(random.choice(PROXY_LIST))
        acc = Client(f"{SESSIONS_FOLDER}{phone_number}", api_id=api_id, api_hash=api_hash, proxy=proxy)
        print("Account #1")
        print(f'Phone number: +{phone_number}')
        print(f'Password: {password if len(password) > 0 else None}')
        setup_errors = []
        with acc:
            print("Account is active")
            try:
                acc.set_username(username)
            except errors.exceptions.bad_request_400.UsernameOccupied:
                setup_errors.append('Username is taken')
            except errors.exceptions.bad_request_400.UsernameNotModified:
                pass
            acc.update_profile(first_name=first_name, last_name=last_name, bio=bio)
            if len(photo) > 0:
                result = get_photo(photo)
                if result == 'Success':
                    acc.set_profile_photo(photo='temp/photo.jpg')
                else:
                    setup_errors.append(result)
            if len(setup_errors) == 0:
                status['values'].append(['OK'])
            else:
                status['values'].append([' / '.join(setup_errors)])
            photos['values'].append([''])

    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!I1", valueInputOption="RAW",
                          body=photos).execute()
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="accounts!K1", valueInputOption="RAW",
                          body=status).execute()


async def acc_distribution(sheet):
    cell_range = f'accounts!A1:D{ROWS}'
    bots = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    cell_range = f'chats!A1:E{ROWS}'
    rows = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values']
    join_tasks = []
    status = {'values': [[f'Status ({datetime.date(datetime.now())})']]}
    for row_number, row in enumerate(rows[1:]):
        link, chat_id, bot = row[0:3]
        if len(bot) == 0:
            bot = random.choice(bots)
            status['values'].append(['Waiting to join'])
            join_tasks.append(asyncio.create_task(join_chat(sheet, bot, row_number + 2, link)))
        else:
            status['values'].append(['Waiting for mailing'])
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range="chats!E1", valueInputOption="RAW", body=status).execute()
    for task in join_tasks:
        await task


async def join_chat(sheet, bot, row_number, link):
    phone_number, api_id, api_hash = bot[0], bot[2], bot[3]
    proxy = transform_proxy(random.choice(PROXY_LIST))
    acc = Client(f"{SESSIONS_FOLDER}{phone_number}", api_id=api_id, api_hash=api_hash, proxy=proxy)
    await acc.start()
    await asyncio.sleep(random.randint(0, JOINING_DELAY))
    try:
        chat_info = await acc.join_chat(link)
        chat_id = chat_info.id
    except errors.exceptions.bad_request_400.InviteHashExpired:
        chat_id = 'Invalid link'
    except errors.exceptions.bad_request_400.UserAlreadyParticipant:
        chat_info = await acc.get_chat(link)
        chat_id = chat_info.id

    await acc.stop()
    if chat_id != 'Invalid link':
        status = {
            'valueInputOption': "RAW",
            'data': [
                {
                    'range': f"chats!B{row_number}:C{row_number}",
                    'values': [[chat_id, phone_number]]
                },
                {
                    'range': f"chats!E{row_number}",
                    'values': [['Waiting for mailing']]
                }]
        }
    else:
        status = {
            'valueInputOption': "RAW",
            'data': [
                {
                    'range': f"chats!E{row_number}",
                    'values': [['Invalid link']]
                }]
        }
    sheet.values().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=status).execute()


async def start_chatting(sheet):
    cell_range = f'text!A1:B{ROWS}'
    messages = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    messages = array_to_dict(messages)
    cell_range = f'accounts!A1:F{ROWS}'
    bots = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    cell_range = f'chats!A1:E{ROWS}'
    chats = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    sessions = []
    for row_number, row in enumerate(chats):
        chat_id, bot_number, chat_type, status = row[1:]
        if status == 'Waiting for mailing':
            bot = None
            for bot in bots:
                if bot[0] == bot_number:
                    break
            message = messages[chat_type]
            sessions.append(asyncio.create_task(bot_session(sheet, chat_id, bot, message, row_number + 2)))
    for session in sessions:
        await session


async def bot_session(sheet, chat_id, bot, message, row_number):
    phone_number, api_id, api_hash, name = bot[0], bot[2], bot[3], bot[5]
    message = message.replace(DEFAULT_NAME, name)
    proxy = transform_proxy(random.choice(PROXY_LIST))
    acc = Client(f"{SESSIONS_FOLDER}{phone_number}", api_id=api_id, api_hash=api_hash, proxy=proxy)
    try:
        await acc.start()
        await asyncio.sleep(random.randint(0, START_CHATTING_DELAY))
        await acc.send_message(chat_id, message)
        await asyncio.sleep(STOP_CHATTING_DELAY)
        await acc.stop()
        status = {'values': [['Message sent']]}
    except errors.exceptions.flood_420.SlowmodeWait:
        status = {'values': [['Slow mode']]}
    except errors.exceptions.forbidden_403.Forbidden:
        status = {'values': [['Muted']]}
    except errors.exceptions.not_acceptable_406.ChannelPrivate:
        status = {'values': [['Banned']]}
    sheet.values().update(spreadsheetId=SPREADSHEET_ID, range=f"chats!E{row_number}", valueInputOption="RAW",
                          body=status).execute()


def array_to_dict(messages):
    d = dict()
    for row in messages:
        d[row[0]] = row[1]
    return d


def transform_proxy(raw_proxy, scheme='http'):
    username, password, hostname, port = raw_proxy.split('@')[0].split(':') + raw_proxy.split('@')[1].split(':')
    proxy = {
        "scheme": scheme,  # "socks4", "socks5" and "http" are supported
        "hostname": hostname,
        "port": int(port),
        "username": username,
        "password": password
    }
    return proxy


def get_proxies(sheet):
    cell_range = f'proxy!A1:B{ROWS}'
    proxies = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=cell_range).execute()['values'][1:]
    for proxy in proxies:
        PROXY_LIST.append(proxy[0])


def get_photo(link):
    try:
        response = requests.get(link)
        if response.status_code == 200:
            file_name = "photo.jpg"
            with open(f"temp/{file_name}", 'wb') as file:
                file.write(response.content)
            return "Success"
        else:
            return "Download failed"
    except requests.exceptions.MissingSchema:
        return "Incorrect URL"
    except requests.exceptions.ConnectionError:
        return "Incorrect URL"


if __name__ == '__main__':
    main()
