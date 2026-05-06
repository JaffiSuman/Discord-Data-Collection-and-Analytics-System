import os
import logging
import asyncio
import aiohttp
import discord
import psycopg2
from datetime import datetime, UTC
from dotenv import load_dotenv

load_dotenv()

# ------------------------------------------------------------
# НАСТРОЙКИ
# ------------------------------------------------------------

now = datetime.now(UTC).strftime("%d-%m-%Y %H:%M")

logging.basicConfig(
    level=logging.INFO,
    filename="discord_bot.log",
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# ------------------------------------------------------------
# УТИЛИТЫ
# ------------------------------------------------------------

def executemany_postgresql(table_name, data, conn):
    """Вставка нескольких записей в таблицу PostgreSQL."""
    if not data:
        return
    try:
        with conn.cursor() as cursor:
            columns = ', '.join(data[0].keys())
            placeholders = ', '.join(['%s'] * len(data[0]))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            values = [tuple(item.values()) for item in data]
            cursor.executemany(query, values)
            conn.commit()
            logging.info(f"Добавлено {len(data)} записей в таблицу {table_name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка вставки в {table_name}: {e}")

# ------------------------------------------------------------
# DISCORD API ЗАПРОСЫ
# ------------------------------------------------------------

async def fetch_with_retry(session, url, headers, guild_id, retries=3):
    """Простая версия запроса к Discord API с повтором при сетевых ошибках."""
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    await asyncio.sleep(0.5)
                    return data

                elif response.status == 429:
                    data = await response.json()
                    retry_after = data.get("retry_after", 1.0)
                    logging.warning(f"Rate limit (429) для {guild_id}, ждем {retry_after:.2f}s")
                    await asyncio.sleep(retry_after + 0.2)

                else:
                    logging.error(f"Ошибка {response.status} при запросе к {guild_id}")
                    await asyncio.sleep(0.5)

        except aiohttp.ClientError as e:
            logging.error(f"Сетевая ошибка при запросе к {guild_id}: {e}")
            await asyncio.sleep(0.5)

    logging.error(f"❌ Сервер {guild_id} не ответил после {retries} попыток — пропускаем.")
    return None


async def myRequests_async(guild_ids, token):
    """
    Асинхронный сбор информации о голосовых каналах.
    Безопасный интервал — 1 запрос в секунду.
    """
    headers = {"Authorization": f"{token}", "Content-Type": "application/json"}
    voice_channels_local = []

    async with aiohttp.ClientSession() as session:
        for gid in guild_ids:
            url = f"https://discord.com/api/v10/guilds/{gid}/channels"
            result = await fetch_with_retry(session, url, headers, gid)

            if result:
                for channel in result:
                    if channel.get("type") == 2:  # Voice channel
                        voice_channels_local.append({
                            "id": channel["id"],
                            "voice_name": channel["name"],
                            "server_id": gid
                        })
                        # logging.info(f"🎧 {channel['name']} (guild {gid})")
                        
    return voice_channels_local

# ------------------------------------------------------------
# DISCORD КЛИЕНТ
# ------------------------------------------------------------

class MyClient(discord.Client):
    def __init__(self, token, guild_ids, db_conn, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.guild_ids = guild_ids
        self.conn = db_conn
        self.voice_channels_local = []
        self.voice_users_local = []
        self.server_info_local = []
        self.user_activity_local = []

    async def on_ready(self):
        try:
            self.voice_channels_local = await myRequests_async(self.guild_ids, self.token)
            await self.collect_voice_users()
            await self.collect_server_info()
            # await self.collect_server_full_info()
            self.save_to_db()
            logging.info(f"Данные для гильдий {self.guild_ids} записаны.")
        except Exception as e:
            logging.error(f"Ошибка on_ready: {e}")
        finally:
            await asyncio.sleep(2)
            await self.close()

    async def collect_voice_users(self):
        """Собирает информацию о пользователях в голосовых каналах."""
        for item in self.voice_channels_local:
            channel = self.get_channel(int(item['id']))
            if not (channel and isinstance(channel, discord.VoiceChannel)):
                continue
            for member in channel.members:
                self.voice_users_local.append({
                    "datetime": now,
                    "user_id": member.id,
                    "server_id": item['server_id'],
                    "voice_name": item['voice_name'],
                    "voice_id": item['id']
                })

    async def collect_server_info(self):
        """Собирает информацию о серверах и активности пользователей."""
        
        for server in self.guilds:
            self.server_info_local.append({
                "datetime": now,
                "server_id": server.id,
                "member_count": server.member_count
            })

            for member in server.members:
                games = [
                    a.name.strip()
                    for a in member.activities
                    if a.type == discord.ActivityType.playing and a.name and a.name.strip()
                ]
                if games:
                    self.user_activity_local.append({
                        "datetime": now,
                        "user_id": member.id,
                        "user_activity_name": ', '.join(games)
                    })

    async def collect_server_full_info(self):
        """Собирает полную информацию о каждом сервере."""
        self.server_full_info_local = []
        for server in self.guilds:
            self.server_full_info_local.append({
                "datetime": now,
                "server_id": server.id,
                "server_name": server.name,
                "language": server.preferred_locale[1],
                "created_at": server.created_at,
                "vanity_url": server.vanity_url_code,
                "owner_id": server.owner_id
            })
        executemany_postgresql('server_full_info', self.server_full_info_local, self.conn)

    def save_to_db(self):
        """Сохраняет собранные данные в базу."""
        executemany_postgresql('user_activity', self.user_activity_local, self.conn)
        executemany_postgresql('server_info', self.server_info_local, self.conn)
        executemany_postgresql('voice_users', self.voice_users_local, self.conn)
        self.conn.close()

# ------------------------------------------------------------
# ЗАПУСК БОТОВ
# ------------------------------------------------------------

def run_bot(token_env_var, guild_ids_env_var):
    token = os.getenv(token_env_var)
    guild_ids_str = os.getenv(guild_ids_env_var)
    guild_ids = [i.strip() for i in guild_ids_str.split(',')] if guild_ids_str else []

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    client = MyClient(token=token, guild_ids=guild_ids, db_conn=conn)
    try:
        client.run(token)
    except KeyboardInterrupt:
        print("Скрипт прерван пользователем.")
    except Exception as e:
        logging.error(f"Критическая ошибка при запуске {token_env_var}: {e}")

run_bot("TOKEN", "GUILD_ID")
run_bot("TOKEN2", "GUILD_ID2")
 
