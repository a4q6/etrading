import requests
import aiohttp
import asyncio
from html import unescape

from etr.config import Config

MAX_CONTENT_LENGTH = 2000


async def async_send_discord_webhook(
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
    chunks = [plain_text[i:i+MAX_CONTENT_LENGTH] for i in range(0, len(plain_text), MAX_CONTENT_LENGTH)]

    statuses = []
    async with aiohttp.ClientSession() as session:
        for chunk in chunks:
            payload = {
                "content": chunk,
                "username": username
            }
            async with session.post(webhook_url, json=payload) as response:
                statuses.append(response.status)
    return statuses


def send_discord_webhook(
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
    chunks = [plain_text[i:i+MAX_CONTENT_LENGTH] for i in range(0, len(plain_text), MAX_CONTENT_LENGTH)]

    statuses = []
    for chunk in chunks:
        data = {
            "content": chunk,
            "username": username
        }
        response = requests.post(webhook_url, json=data)
        statuses.append(response.status_code)
    return statuses
