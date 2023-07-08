from telethon.sync import TelegramClient
from telethon.tl.functions.account import UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.messages import ImportChatInviteRequest, GetHistoryRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest
from telethon.tl.functions.channels import JoinChannelRequest

import asyncio


class Userbot:
    sessions_folder = "Sessions/"
    keywords = ['captcha', 'solve']

    def __init__(self, phone_number, api_id, api_hash, password=None, proxy=None):
        self.phone_number = phone_number
        self.api_id = api_id
        self.api_hash = api_hash
        self.password = password
        self.proxy = proxy
        self.client = None

    async def start_session(self):
        try:
            if self.proxy is not None:
                username, password, hostname, port = self.proxy.split('@')[0].split(':') + self.proxy.split('@')[1].split(':')
                self.proxy = {
                    'proxy_type': 'http',
                    'addr': hostname,
                    'port': int(port),
                    'username': username,
                    'password': password
                }
            self.client = TelegramClient(f"{self.sessions_folder}{self.phone_number}", self.api_id, self.api_hash)
            self.client.set_proxy(self.proxy)
            await self.client.start(password=self.password)
            return "Account is active"
        except Exception as e:
            return e

    async def set_user_profile(self, username=None, first_name=None, last_name=None, bio=None, photo_path=None):
        try:
            if photo_path:
                await self.client(UploadProfilePhotoRequest(
                    file=await self.client.upload_file(photo_path)))
            await self.client(UpdateProfileRequest(first_name=first_name, last_name=last_name, about=bio))
            await self.client(UpdateUsernameRequest(username))
            return "Account is active"
        except Exception as e:
            return e

    async def join_chat(self, link):
        try:
            chat = await self.client(JoinChannelRequest(link))
            chat_id = chat.chats[0].id
            return chat_id
        except Exception as e:
            return e

    async def get_my_name(self):
        try:
            me = await self.client.get_me()
            my_name = me.first_name
            return my_name
        except Exception as e:
            print(f"Failed to get my name: {e}")

    async def send_message(self, chat_id, message):
        try:
            await self.client.send_message(chat_id, message)
            return "Message sent"
        except Exception as e:
            return e

    async def detect_captcha(self, link, limit=5):
        try:
            chat = await self.client.get_entity(link)
            result = await self.client(GetHistoryRequest(
                peer=chat,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=0,
                hash=0
            ))
            for message in result.messages:
                for word in self.keywords:
                    if word in message.message:
                        return 'CAPTCHA'
        except Exception as e:
            return e

    async def close_session(self):
        if self.client is not None:
            await self.client.disconnect()


# Example usage
async def main():
    api_id = 23068940  # Your API ID
    api_hash = '943103e7262949158fef9bcc5e928ea4'  # Your API hash
    phone_number = '77479902957'

    # Create instance of userbot
    client = Userbot(phone_number, api_id, api_hash, password='3008')

    await client.start_session()
    link = 'https://t.me/testGroupauctiom'
    chat_id = await client.join_chat(link)
    print(chat_id)
    await client.send_message(chat_id, 'Hello')
    await client.detect_captcha(link)
    await client.close_session()


if __name__ == '__main__':
    # Run the main function
    asyncio.run(main())
