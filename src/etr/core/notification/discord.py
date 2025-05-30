import aiohttp
import asyncio
from html import unescape

from etr.config import Config

async def send_discord_webhook(
    message: str, 
    webhook_url: str = Config.DISCORD_URL_TEST,
    username: str = "Notifier",
) -> int:
    """
    Discord Webhookに非同期でメッセージを送信する関数。

    Parameters:
    - webhook_url (str): DiscordのWebhook URL
    - message (str): 送信するメッセージ（HTML可だがDiscordはMarkdown推奨）
    - username (str): 表示されるユーザー名

    Returns:
    - int: HTTPステータスコード
    """
    plain_text = unescape(message)

    data = {
        "content": plain_text,
        "username": username
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(webhook_url, json=data) as response:
            return response.status
