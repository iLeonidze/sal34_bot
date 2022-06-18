from __future__ import print_function, annotations

import asyncio
import datetime
import json
import math
import os.path
import signal
import sys
import time
import threading
import traceback
from typing import Dict, List, Any

from emoji import UNICODE_EMOJI_ENGLISH

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import pandas as pd
from pandas import DataFrame

from telegram.ext import Updater, MessageHandler, Filters, CallbackContext, CommandHandler, \
    CallbackQueryHandler
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, \
    InlineKeyboardButton, Bot, ForceReply, TelegramError, ChatMember

import logging

from telethon.errors import UserPrivacyRestrictedError
from telethon.sync import TelegramClient
from telethon.tl.functions.channels import InviteToChannelRequest
from telethon.tl.functions.contacts import DeleteContactsRequest, ImportContactsRequest, \
    AddContactRequest
from telethon.tl.functions.messages import ExportChatInviteRequest
from telethon.tl.types import InputPhoneContact

from assistant import HelpAssistant, is_bot_assistant_request

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

TABLES_SYNC_TIMER = None
CACHES_STALE_TIMER = None
ACTIONS_QUEUE_TIMER = None
USERS_CONTEXT_SAVE_TIMER = None
GOOGLE_CREDENTIALS = None
TG_UPDATER = None
TG_BOT: Bot or None = None
TG_CLIENT: TelegramClient

CONFIGS = {
    "buildings": {},
    "service": {}
}

STATS = {}

DB = {}

QUEUED_ACTIONS = []

DF_COLUMNS = [
    'entrance',
    'floor',
    'floor_position',
    'object_type',
    'number',
    'area',
    'rooms',
    'surname',
    'name',
    'patronymic',
    'user_type',
    'telegram',
    'phone',
    'added_to_group',
    'show_phone',
    'updated',
    'comments',
    'has_other_objects',
    'contract_id',
    'contract_date',
    'contract_reg_id',
    'contract_reg_date']

OBJECT_TYPES_NAMES = {
    'кв': 'Квартира',
    'кл': 'Кладовка',
    'мм': 'Парковка',
    'нж': 'Помещение',
}

TABLES_RELOADED_TIME = 0
LAST_STALED_USER_CACHE = time.time()
QUEUED_ACTIONS_LAST_EXECUTED_TIME = time.time()

HELP_ASSISTANT = None


def get_default_context():
    return {
        'private_chat': {
            'bot_started': None,
            'current_keyboard': 'main',
            'current_dialog_stage': None,
            'is_access_granted': False,
            'is_waiting_initial_access_approve': False,
            'is_waiting_data_update_approve': False,
            'initial_access_details': {
                'name': None,
                'surname': None,
                'patronymic': None,
                'user_type': None,
                'phone': None,
                'object_type': None,
                'object_number': None,
                'uploaded_photo_id': None
            },
            'data_update_details': {}
        },
        # chat IDs and join dates
        'joined_chats': {},
        'leaved_chats': {},
        'last_activity_in_chats': {},
        'stats': {
            'sended_private_messages_total': 0,
            'sended_public_messages_total': 0,
            'sended_public_messages_per_chat': {},
            'total_garbage_detected_for_user': 0
        }
    }


def is_emoji(s):
    for symbol in set(s.lower()):
        if symbol not in UNICODE_EMOJI_ENGLISH:
            return False

    return True


def is_repeated_symbol(s):
    return len(set(s.lower())) == 1


class SetInterval:
    def __init__(self, action, interval):
        self.interval = interval
        self.action = action
        self.stopEvent = threading.Event()
        thread = threading.Thread(target=self.__set_interval)
        thread.start()

    def __set_interval(self):
        next_time = time.time() + self.interval
        while not self.stopEvent.wait(next_time - time.time()):
            next_time += self.interval
            self.action()

    def cancel(self):
        self.stopEvent.set()


class User:
    def __init__(self, telegram_id: int, cache: UsersCache):
        self.load_time = time.time()
        self.cache = cache
        self.telegram_id = telegram_id
        self.db_entries = pd.DataFrame()
        self.person = None
        self.phone = None
        self.add_to_group = None
        self.own_object_types = []
        self.from_sections = []
        self.related_users_objects = []
        self.building = None
        self.objects = []

        self.context = get_default_context()

        for building, table in DB.items():
            rows = table.loc[table['telegram'] == str(self.telegram_id)]
            rows['building'] = building
            self.building = building
            if self.db_entries.empty:
                self.db_entries = rows
            else:
                # TODO: fix this, this merge will fail
                self.db_entries = self.db_entries.merge(rows)

        if self.has_any_object():
            effective_index = 0
            self.person = {
                'name': self.db_entries['name'].iloc[effective_index],
                'surname': self.db_entries['surname'].iloc[effective_index],
                'patronymic': self.db_entries['patronymic'].iloc[effective_index]
            }

            self.add_to_group = self.db_entries['added_to_group'].iloc[effective_index] == 'YES'

            phone_number = self.db_entries['phone'].iloc[effective_index]
            if phone_number:
                self.phone = {
                    'number': phone_number,
                    'visible': self.db_entries['show_phone'].iloc[effective_index] == 'YES'
                }

            self.own_object_types = self.db_entries['object_type'].unique()

            related_users_df = pd.DataFrame(columns=DF_COLUMNS)
            related_users_df['building'] = None
            for index, row in self.db_entries.iterrows():
                table = DB[row['building']]

                if row['object_type'] == 'мм':
                    obj_type = 'p'
                    section_id = obj_type
                elif row['object_type'] == 'кл':
                    obj_type = 's'
                    section_id = obj_type
                else:
                    section_id = row['entrance']
                    obj_type = 'f'

                self.from_sections.append({
                    'type': obj_type,
                    'number': int(row['entrance']),
                    'id': section_id
                })

                self.objects.append({
                    'building': row['building'],
                    'floor': int(row['floor']),
                    'section': section_id,
                    'section_raw': row['entrance'],
                    'type': row['object_type'],
                    'number': row['number'],
                    'floor_position': int(row['floor_position']),
                })

                related_users_found_df = table[
                    (table['object_type'] == row['object_type']) &
                    (table['number'] == row['number']) &
                    ((table['name'] != row['name']) |
                     (table['surname'] != row['surname']) |
                     (table['patronymic'] != row['patronymic']))
                ]
                if not related_users_found_df.empty:
                    related_users_df = related_users_df.append(related_users_found_df)
                # TODO: building correct objects from rows

            self.from_sections = self.from_sections
            self.related_users_objects = related_users_df

            self.load_context()

    def __hash__(self):
        return hash(self.telegram_id) + hash(self.load_time)

    def __eq__(self, other) -> bool:
        if self is other:
            return True

        if not isinstance(other, User):
            return False

        if self.telegram_id != other.telegram_id:
            return False

        return True

    def is_identified(self):
        return self.person is not None

    def has_any_object(self):
        return not self.db_entries.empty

    def get_related_users(self):
        return self.related_users_objects[['name', 'surname', 'patronymic', 'telegram', 'phone', 'added_to_group', 'show_phone']].drop_duplicates()

    def get_user_filepath(self):
        return f'./users/{self.telegram_id}.json'

    def load_context(self):
        user_filepath = self.get_user_filepath()
        if os.path.isfile(user_filepath):
            try:
                with open(user_filepath, 'r', encoding='utf8') as stream:
                    self.context = json.load(stream)
            except Exception:
                print(f'!!! Failed to read user data {self.telegram_id} !!!')

    def delayed_context_save(self):
        if USERS_CONTEXT_SAVE_TIMER is not None:
            self.cache.schedule_user_context_save(self)
        else:
            print(f'Autosave disabled, saving {self.telegram_id} synchronously...')
            self.save_context()

    def save_context(self):
        user_filepath = self.get_user_filepath()
        with open(user_filepath, 'w', encoding='utf8') as stream:
            json.dump(self.context, stream, ensure_ascii=False)

        if self in self.cache.scheduled_saves:
            self.cache.scheduled_saves.remove(self)

    def get_fullname(self) -> str:
        if not self.is_identified():
            return ''

        fullname = f'{self.person["surname"]} {self.person["name"]}'
        if self.person.get("patronymic"):
            fullname += f' {self.person["patronymic"]}'

        return fullname

    def get_linked_fullname(self) -> str:
        if not self.is_identified():
            return ''

        fullname = '[' + encode_markdown(self.get_fullname()) + '](tg://user?id=' + str(self.telegram_id) + ')'
        return fullname

    def get_shortname(self) -> str:
        if not self.is_identified():
            return ''

        shortname = f'{self.person["name"]}'
        if self.person.get("surname"):
            shortname += f' {self.person["surname"][0]}.'

        return shortname

    def get_linked_shortname(self) -> str:
        if not self.is_identified():
            return ''

        shortname = '[' + encode_markdown(self.get_shortname()) + '](tg://user?id=' + str(self.telegram_id) + ')'
        return shortname

    def get_seminame(self) -> str:
        if not self.is_identified():
            return ''

        seminame = f'{self.person["name"]} {self.person["surname"]}'

        return seminame

    def get_linked_seminame(self) -> str:
        if not self.is_identified():
            return ''

        seminame = '[' + encode_markdown(self.get_seminame()) + '](tg://user?id=' + str(self.telegram_id) + ')'
        return seminame

    def change_fullname(self, name, surname, patronymic=None):
        self.update_table_values([['name', name], ['surname', surname], ['patronymic', patronymic]])

    def change_user_type(self, user_type: int):
        user_types = ['собственник', 'пользователь']
        user_type_str = user_types[user_type]
        self.update_table_value('user_type', user_type_str)

    def get_public_phone(self):
        if not self.phone:
            return 'телефон не указан'
        if self.phone.get('visible', False):
            return '+' + self.phone['number']
        else:
            return 'телефон скрыт'

    def change_phone(self, phone):
        self.update_table_value('phone', phone)

    def change_phone_visibility(self, visibility_bool):
        self.update_table_value('phone_visibility', visibility_bool)

    def lock_bot_access(self):
        self.context['private_chat']['is_access_granted'] = False
        self.evict()

    def get_related_chats(self) -> List[Dict]:
        chats = []

        for section in self.from_sections:
            chat = get_chat_for_section_building(self.building, section['id'])
            chats.append(chat)

        for chat in CONFIGS['buildings'][self.building]['groups']:
            if chat['name'] in ['private_common_group', 'public_info_channel', 'guards_group']:
                chats.append(chat)

        return list({v['id']:v for v in chats}.values())

    def get_related_chats_ids(self) -> List[int]:
        chats_ids = []
        chats = self.get_related_chats()
        for chat in chats:
            chats_ids.append(chat['id'])
        return chats_ids

    def is_chat_related(self, requested_chat_id: int) -> bool:
        is_chat_related = False
        for chat_id in self.get_related_chats_ids():
            if chat_id == requested_chat_id:
                is_chat_related = True
                break

        return is_chat_related

    def add_to_chat(self, chat_id: int, save=True):
        if self.telegram_id == CONFIGS['service']['identity']['telegram']['superuser_id']:
            return

        if not self.is_added_to_group(chat_id):

            chats = self.get_related_chats()
            for chat in chats:

                if chat['id'] != chat_id:
                    continue

                if chat['name'] == 'public_info_channel':
                    tg_client_send_invite_to_public_channel(chat['invite_address'], self)
                else:
                    tg_client_add_user_to_channel(chat_id, self)

                break

        if save and is_common_group_chat(self.building, chat_id):
            self.update_table_value('added_to_group', 'YES')

    def add_to_all_chats(self):
        chats = self.get_related_chats()
        for chat in chats:
            self.add_to_chat(chat['id'], save=False)

        self.update_table_value('added_to_group', 'YES')

    def remove_from_chat(self, chat_id: int, save=True):
        if self.telegram_id == CONFIGS['service']['identity']['telegram']['superuser_id']:
            return

        if self.is_added_to_group(chat_id):
            tg_bot_delete_user_from_channel(chat_id, self.telegram_id)

        if save and is_common_group_chat(self.building, chat_id):
            self.update_table_value('added_to_group', 'NO')

    def remove_from_all_chats(self):
        chats = self.get_related_chats()
        for chat in chats:
            self.remove_from_chat(chat['id'], save=False)

        self.update_table_value('added_to_group', 'NO')

    def evict(self) -> None:
        self.save_context()
        if self.cache.users.get(self.telegram_id):
            del self.cache.users[self.telegram_id]

    def deactivate(self) -> None:
        self.lock_bot_access()
        self.remove_from_all_chats()
        self.evict()

    def update_table_values(self, values: List[List[str, str or int]]):
        update_table(self.building, values)
        self.evict()

    def update_table_value(self, column_name: str, value: str or int):
        update_table(self.building, [[column_name, value]])
        self.evict()

    def get_floors(self, building: int, section: str) -> List[int]:
        result = []
        for obj in self.objects:
            if obj['building'] == building and obj['section'] == section:
                result.append(obj['floor'])
        return result

    def get_object_numbers(self, building: int, section: str, floor: int) -> List[int]:
        result = []
        for obj in self.objects:
            if obj['building'] == building and obj['section'] == section and obj['floor'] == floor:
                result.append(obj['number'])
        return result

    def _get_neighbours(self, building=None, section: str = None, number: str or int = None, object_type: str = None) -> DataFrame:
        if not building:
            building = self.building

        filtered_user_objects = []
        for obj in self.objects:
            if section and obj['section'] != section:
                continue
            if number and int(obj['number']) != int(number):
                continue
            if object_type and obj['type'] != object_type:
                continue
            filtered_user_objects.append(obj)

        all_neighbours = None

        for obj in filtered_user_objects:
            table = DB[building]
            section_table = table[(table['entrance'] == obj['section_raw']) & (table['object_type'] == obj['type'])]

            # neighbours from same floor
            neighbours_from_floor = section_table[section_table['floor'] == str(obj['floor'])]
            neighbours_bottom = section_table[(section_table['floor'] == str(obj['floor']-1)) & (section_table['floor_position'] == str(obj['floor_position']))]
            neighbours_top = section_table[(section_table['floor'] == str(obj['floor']+1)) & (section_table['floor_position'] == str(obj['floor_position']))]

            obj_neighbours = neighbours_from_floor.append(neighbours_bottom).append(neighbours_top)

            if all_neighbours is None:
                all_neighbours = obj_neighbours
            else:
                all_neighbours = all_neighbours.append(obj_neighbours)

        all_neighbours.number = all_neighbours.number.astype(int)

        return all_neighbours

    def get_neighbours(self, building=None, section: str = None, number: str or int = None, object_type: str = None) -> Dict[str, Dict[str, Dict[str, Any[str, List[Any[User, List[str]]]]]]]:
        neighbours_table = self._get_neighbours(building, section, number, object_type)
        return rebuild_neighbours_dict_from_table(neighbours_table)

    def is_added_to_group(self, group_id: int) -> bool:
        return is_user_added_to_groups(self.telegram_id, [group_id])

    def is_added_to_all_groups(self) -> bool:
        groups_ids = self.get_related_chats_ids()
        return is_user_added_to_groups(self.telegram_id, groups_ids)

    def get_str_user_related_groups_status(self):
        text = ''
        added_everywhere = True
        for chat in self.get_related_chats():
            text += '\\- '
            chat_name = get_chat_name_by_chat(chat)

            if self.is_added_to_group(chat['id']):
                text += '✅ '
            else:
                text += '❌ '
                added_everywhere = False

            text += chat_name + '\n'

        return text.strip(), added_everywhere


def is_user_added_to_groups(telegram_id: int, groups_ids: List[int]) -> bool:
    for group_id in groups_ids:
        time.sleep(2)
        try:
            result = TG_BOT.get_chat_member(group_id, telegram_id)
            if not isinstance(result, ChatMember) or result.status not in ['member', 'administrator', 'creator']:
                return False
        except TelegramError as e:
            return False
    return True


def rebuild_neighbours_dict_from_table(table: DataFrame) -> Dict[str, Dict[str, Dict[str, Any[str, List[Any[User, List[str]]]]]]]:
    table.number = table.number.astype(int)
    table = table.sort_values(by=['number'], ascending=True)

    neighbours = {}

    # этаж -> объект -> {"type", "users": [["",""] или User]}

    for index, row in table.iterrows():
        floor = row['floor']
        if not neighbours.get(floor):
            neighbours[floor] = {}

        obj_number = int(row['number'])
        if not neighbours[floor].get(obj_number):
            neighbours[floor][obj_number] = {
                'type': row['object_type'],
                'users': [],
                'position': int(row['floor_position'])
            }

        if row['telegram'] != '':
            user = USERS_CACHE.get_user(int(row['telegram']))
        else:
            user = [encode_markdown(row['name']), encode_markdown(row['surname'])]
        neighbours[floor][obj_number]['users'].append(user)

    # Do not show absent users in results
    for floor_number, floor_objs in neighbours.items():
        for obj_number, obj in floor_objs.items():
            only_users = []
            for user in obj['users']:
                if isinstance(user, User):
                    only_users.append(user)
            if only_users:
                neighbours[floor_number][obj_number]['users'] = only_users

    return neighbours


def encode_markdown(string: str):
    # TODO: this is slow
    return string.replace('-', '\\-') \
                 .replace('_', '\\_') \
                 .replace('.', '\\.') \
                 .replace('(', '\\(') \
                 .replace(')', '\\)') \
                 .replace('[', '\\[') \
                 .replace(']', '\\]')


def get_all_users(building) -> List:
    users = []
    table = DB[building]
    tg_ids_ndarray = table['telegram'].unique()
    for tg_id in tg_ids_ndarray:
        if tg_id != '':
            user = USERS_CACHE.get_user(tg_id)
            users.append(user)
    return users


class UsersCache:
    def __init__(self):
        self.users: Dict[int, User] = {}
        self.last_save_time = time.time()
        self.scheduled_saves: set[User] = set()

    def get_user(self, incoming_user_update: Update or int) -> User:
        if isinstance(incoming_user_update, Update):
            incoming_user_id = int(incoming_user_update.effective_user.id)
        else:
            incoming_user_id = int(incoming_user_update)

        # TODO: temporary solution - disabled caches and force reloading
        # if self.users.get(incoming_user_id):
        #     return self.users[incoming_user_id]
        USERS_CACHE.evict()
        reload_tables()

        user = User(incoming_user_id, self)
        if user.is_identified():
            self.users[incoming_user_id] = user
            return self.users[incoming_user_id]

        return user

    def save_users(self):
        for user in list(self.scheduled_saves):
            user.save_context()
        self.scheduled_saves = set()
        self.last_save_time = time.time()

    def save_all_users(self):
        for tg_id, user in self.users.items():
            user.save_context()
        self.scheduled_saves = set()
        self.last_save_time = time.time()

    def get_stats(self):
        cached_users = len(self.users)
        waiting_for_saving_users = len(self.scheduled_saves)
        return {
            "cached_users": cached_users,
            "users_save_queue": waiting_for_saving_users,
            "time_since_last_save": time.time() - self.last_save_time
        }

    def schedule_user_context_save(self, user: User):
        self.scheduled_saves.add(user)

    def evict(self):
        for user_tg_id in list(self.users.keys()):
            self.users[user_tg_id].evict()

    def stale(self):
        global LAST_STALED_USER_CACHE

        stale_interval = CONFIGS['service']['scheduler']['caches_stale_interval']

        current_time = time.time()

        for user_tg_id in list(self.users.keys()):
            cached_user = self.users[user_tg_id]

            if current_time-cached_user.load_time > stale_interval:

                print(f'Staling cache for user {user_tg_id}')

                if cached_user in self.scheduled_saves:
                    cached_user.save_context()

                del self.users[user_tg_id]

                LAST_STALED_USER_CACHE = time.time()

    def _get_neighbours_from_section(self, building: str, section: str = None) -> DataFrame:
        table = DB[building]

        if not section:
            return table

        obj_type = 'кв'
        if section == 's':
            obj_type = 'кл'
        elif section == 'p':
            obj_type = 'мм'

        if obj_type == 'кв':
            neighbours_table = table[(table['object_type'] == obj_type) & (table['entrance'] == section)]
        else:
            neighbours_table = table[(table['object_type'] == obj_type)]

        return neighbours_table

    def get_neighbours_from_section(self, building: str, section: str = None) -> Dict[str, Dict[str, Dict[str, Any[str, List[Any[User, List[str]]]]]]]:
        neighbours_table = self._get_neighbours_from_section(building, section)
        return rebuild_neighbours_dict_from_table(neighbours_table)


USERS_CACHE = UsersCache()


async def _tg_client_add_user_to_contacts(client: TelegramClient,
                                          user: User = None,
                                          phone: str or int = None,
                                          name: str = None,
                                          surname: str = None):
    if user is None and phone is None:
        raise Exception('Phone or User ID should be specified')

    print('Adding contact...')

    if isinstance(phone, int):
        phone = "+" + str(phone)

    if not name:
        name = phone

    if not surname:
        surname = phone

    if user is None:
        try:
            print('Adding via Phone...')
            contact = InputPhoneContact(client_id=0, phone=phone, first_name=name, last_name=surname)
            return await client(ImportContactsRequest([contact]))
        except Exception as e:
            if user is None:
                raise e from None

    print('Observing user via ID...')
    await _tg_client_observe_groups_for_user(client, user)
    print('Adding via ID...')
    return await client(AddContactRequest(user.telegram_id, name, surname, phone=phone))


async def _tg_client_observe_groups_for_user(client: TelegramClient, user: User):
    await client.get_dialogs()
    for chat in CONFIGS['buildings']['area_chats'][user.building()]:
        await client.get_participants(chat['id'])


async def _tg_client_remove_user_from_contacts(client: TelegramClient, tg_id):
    return await client(DeleteContactsRequest(id=tg_id))


async def _tg_client_get_guest_id_via_phone(phone: str) -> int or None:
    return None

    # loop = asyncio.new_event_loop()
    #
    # client_api_id = CONFIGS['service']['identity']['telegram']['client_api_id']
    # client_api_hash = CONFIGS['service']['identity']['telegram']['client_api_hash']
    #
    # client: TelegramClient = TelegramClient('sal34_bot_client',
    #                                         client_api_id,
    #                                         client_api_hash,
    #                                         loop=loop)
    #
    # async with client:
    #     import_result = await _tg_client_add_user_to_contacts(client, phone=phone)
    #
    #     if len(import_result.imported) == 0:
    #         print('Nothing imported!')
    #         return None
    #
    #     print('Loading user info...')
    #     user_id = await _tg_client_get_entity_id(phone)
    #
    #     if not USERS_CACHE.get_user(user_id).has_any_object():
    #         print('Removing contact...')
    #         await _tg_client_remove_user_from_contacts(client, import_result.users[0])
    #
    #     return user_id


async def _tg_client_add_user_to_channel(channel_id: int, user: User) -> None:
    loop = asyncio.new_event_loop()

    client_api_id = CONFIGS['service']['identity']['telegram']['client_api_id']
    client_api_hash = CONFIGS['service']['identity']['telegram']['client_api_hash']

    client: TelegramClient = TelegramClient('sal34_bot_client',
                                            client_api_id,
                                            client_api_hash,
                                            loop=loop)

    async with client:

        try:
            await _tg_client_add_user_to_contacts(client,
                                                  phone=user.phone['number'],
                                                  user=user,
                                                  name=user.person['name'] + ' 34',
                                                  surname=user.person.get('surname', ''))
        except Exception as e:
            print('Failed to add user to contacts')
            print(e)

        await client(InviteToChannelRequest(
            channel_id,
            [user.telegram_id]
        ))


async def _tg_client_send_message_to_user(message: str, user: User) -> None:
    loop = asyncio.new_event_loop()

    client_api_id = CONFIGS['service']['identity']['telegram']['client_api_id']
    client_api_hash = CONFIGS['service']['identity']['telegram']['client_api_hash']

    client: TelegramClient = TelegramClient('sal34_bot_client',
                                            client_api_id,
                                            client_api_hash,
                                            loop=loop)

    async with client:
        await client.send_message(user.telegram_id, message)


def tg_client_add_user_to_channel(channel_id: int, user: User) -> None:
    asyncio.run(_tg_client_add_user_to_channel(channel_id, user))


def tg_client_send_invite_to_public_channel(invite_address, user: User) -> None:
    message = 'Обязательно подписывайтесь на инфо канал с важными новостями дома:\n' + invite_address

    asyncio.run(_tg_client_send_message_to_user(message, user))


def tg_client_get_invites_for_chats(chats_ids: List[int]) -> List[str]:
    results = []
    for chat_id in chats_ids:
        results.append(tg_client_get_invite_for_chat(chat_id))
    return results


def tg_client_get_invite_for_chat(chat_id: int) -> str:
    return asyncio.run(_tg_client_get_invite_for_chat(chat_id))


async def _tg_client_get_invite_for_chat(chat_id: int) -> str:
    loop = asyncio.new_event_loop()

    client_api_id = CONFIGS['service']['identity']['telegram']['client_api_id']
    client_api_hash = CONFIGS['service']['identity']['telegram']['client_api_hash']

    client: TelegramClient = TelegramClient('sal34_bot_client',
                                            client_api_id,
                                            client_api_hash,
                                            loop=loop)

    async with client:
        result = await client(ExportChatInviteRequest(
            peer=chat_id,
            expire_date=datetime.datetime.utcnow() + datetime.timedelta(days=1),
            usage_limit=1
        ))
        return result.link


def tg_bot_delete_user_from_channel(channel_id: int, user_id: int) -> None:
    TG_BOT.kick_chat_member(chat_id=channel_id, user_id=user_id)
    TG_BOT.unban_chat_member(chat_id=channel_id, user_id=user_id)


async def _tg_client_get_entity_id(entity_query: int or str) -> int or None:
    loop = asyncio.new_event_loop()

    client_api_id = CONFIGS['service']['identity']['telegram']['client_api_id']
    client_api_hash = CONFIGS['service']['identity']['telegram']['client_api_hash']

    client: TelegramClient = TelegramClient('sal34_bot_client',
                                            client_api_id,
                                            client_api_hash,
                                            loop=loop)

    async with client:
        try:
            entity = await client.get_entity(entity_query)
            if entity and entity.id:
                return entity.id
        except Exception:
            return None


def tg_client_get_user_by_username(username: str) -> User or None:
    user_id = asyncio.run(_tg_client_get_entity_id(username))
    if not user_id:
        return None
    return USERS_CACHE.get_user(user_id)


def tg_client_get_user_id_by_phone(phone: str) -> int or None:
    result = asyncio.run(_tg_client_get_entity_id(phone))
    if result is not None:
        return result

    return asyncio.run(_tg_client_get_guest_id_via_phone(phone))


def get_chat_for_section_building(building, section):
    for chat in CONFIGS['buildings'][str(building)]['groups']:
        if chat.get('section') == str(section):
            return chat
    return None


def is_common_group_chat(building, chat_id):
    for chat in CONFIGS['buildings'][str(building)]['groups']:
        if chat['id'] == chat_id and chat['name'] == 'private_section_group':
            return True
    return False


def reload_configs():
    global CONFIGS
    global DB

    with open('service.json', 'r') as s:
        CONFIGS['service'] = json.load(s)

    for building_file in os.listdir('./buildings'):
        building_name = building_file.split('.')[0]
        DB[building_name] = None
        with open('./buildings/' + building_file, 'r') as s:
            CONFIGS['buildings'][building_name] = json.load(s)

    for stats_file in os.listdir('./stats'):
        stats_name = stats_file.split('.')[0]
        with open('./stats/' + stats_file, 'r') as s:
            STATS[stats_name] = json.load(s)


def connect_google_service():
    global GOOGLE_CREDENTIALS

    filename = CONFIGS['service']['identity']['google']['filename']
    scopes = CONFIGS['service']['identity']['google']['scopes']

    GOOGLE_CREDENTIALS = \
        service_account.Credentials.from_service_account_file(filename, scopes=scopes)


def reload_tables():
    global TABLES_RELOADED_TIME

    if time.time()-TABLES_RELOADED_TIME < 10:
        return

    print('Reloading tables...')

    try:
        service = build('sheets', 'v4', credentials=GOOGLE_CREDENTIALS)

        for building_number, building_table in DB.items():

            # PEOPLE
            spreadsheet_id = CONFIGS['buildings'][building_number]['spreadsheet']['people']['id']
            spreadsheet_range = CONFIGS['buildings'][building_number]['spreadsheet']['people']['range']

            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                        range=spreadsheet_range).execute()
            rows = result.get('values', [])

            if not rows:
                print('Syncing tables error PEOPLE: No data')
                return

            DB[building_number] = pd.DataFrame(rows, columns=DF_COLUMNS)

            # ASSISTANT
            spreadsheet_id = CONFIGS['buildings'][building_number]['spreadsheet']['assistant']['id']
            spreadsheet_range = CONFIGS['buildings'][building_number]['spreadsheet']['assistant']['range']

            sheet = service.spreadsheets()
            result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                        range=spreadsheet_range).execute()
            rows = result.get('values', [])

            if not rows:
                print('Syncing tables error ASSISTANT: No data')
                return

            global HELP_ASSISTANT
            HELP_ASSISTANT = HelpAssistant(rows)

            print(f'  {building_number} synced')

        TABLES_RELOADED_TIME = time.time()

    except HttpError as err:
        print(err)


def update_table(building: str or int, values: List[List[str, str or int]]):
    pass


def identify_chat_by_tg_update(update: Update) -> (bool, str, bool, str, list or None):
    incoming_chat_id = update.effective_chat.id

    is_found = False
    found_building_number = None
    is_admin_chat = False
    chat_name = None
    chat_section = None
    building_chats = None

    for building_number, building_config in CONFIGS['buildings'].items():
        if is_found:
            break

        for group in building_config['groups']:
            if group['id'] == incoming_chat_id:
                is_found = True
                found_building_number = building_number
                building_chats = building_config['groups']

                chat_name = group['name']
                
                if group.get('section'):
                    chat_section = group['section']

                if group['name'] == 'admin':
                    is_admin_chat = True

                break

    return is_found, found_building_number, is_admin_chat, chat_name, chat_section, building_chats


def start_tables_sync():
    reload_tables()
    global TABLES_SYNC_TIMER
    if not TABLES_SYNC_TIMER or not isinstance(TABLES_SYNC_TIMER, SetInterval):
        TABLES_SYNC_TIMER = SetInterval(reload_tables, CONFIGS['service']['scheduler']['sync_interval'])


def stop_tables_sync():
    global TABLES_SYNC_TIMER
    if TABLES_SYNC_TIMER and isinstance(TABLES_SYNC_TIMER, SetInterval):
        TABLES_SYNC_TIMER.cancel()
        TABLES_SYNC_TIMER = None


def start_caches_stale():
    global CACHES_STALE_TIMER
    if not CACHES_STALE_TIMER or not isinstance(CACHES_STALE_TIMER, SetInterval):
        CACHES_STALE_TIMER = SetInterval(USERS_CACHE.stale, 1)


def stop_caches_stale():
    global CACHES_STALE_TIMER
    if CACHES_STALE_TIMER and isinstance(CACHES_STALE_TIMER, SetInterval):
        CACHES_STALE_TIMER.cancel()
        CACHES_STALE_TIMER = None


def reset_actions_queue():
    global QUEUED_ACTIONS
    QUEUED_ACTIONS = []


def start_actions_queue():
    global ACTIONS_QUEUE_TIMER
    if not ACTIONS_QUEUE_TIMER or not isinstance(ACTIONS_QUEUE_TIMER, SetInterval):
        ACTIONS_QUEUE_TIMER = SetInterval(proceed_actions_queue, 1)


def stop_actions_queue():
    global ACTIONS_QUEUE_TIMER
    if ACTIONS_QUEUE_TIMER and isinstance(ACTIONS_QUEUE_TIMER, SetInterval):
        ACTIONS_QUEUE_TIMER.cancel()
        ACTIONS_QUEUE_TIMER = None


def start_users_context_save():
    global USERS_CONTEXT_SAVE_TIMER
    if not USERS_CONTEXT_SAVE_TIMER or not isinstance(USERS_CONTEXT_SAVE_TIMER, SetInterval):
        USERS_CONTEXT_SAVE_TIMER = SetInterval(proceed_users_context_save, 1)


def stop_users_context_save():
    global USERS_CONTEXT_SAVE_TIMER
    if USERS_CONTEXT_SAVE_TIMER and isinstance(USERS_CONTEXT_SAVE_TIMER, SetInterval):
        USERS_CONTEXT_SAVE_TIMER.cancel()
        USERS_CONTEXT_SAVE_TIMER = None


def proceed_actions_queue():
    global QUEUED_ACTIONS
    global QUEUED_ACTIONS_LAST_EXECUTED_TIME

    # wait until telegram started
    if TG_BOT is None:
        # print('TG bot is not ready yet')
        return
    else:
        for action in QUEUED_ACTIONS:
            if not action.get('executed', False):
                if action['time'] < time.time():

                    # TODO: support more types

                    if action['type'] == 'delete':
                        print(f"Deleting message {action['message_id']} from {action['chat_id']}...")
                        TG_BOT.delete_message(chat_id=action['chat_id'],
                                              message_id=action['message_id'])

                    action['executed'] = True

        non_executed_actions = []
        for action in QUEUED_ACTIONS:
            if not action.get('executed', False):
                non_executed_actions.append(action)
        QUEUED_ACTIONS = non_executed_actions

        QUEUED_ACTIONS_LAST_EXECUTED_TIME = time.time()


def proceed_users_context_save():
    interval = CONFIGS['service']['scheduler']['context_save_interval']
    if time.time()-USERS_CACHE.last_save_time > interval:
        print('Context save started...')
        USERS_CACHE.save_users()
        print('Context save finished...')


def bot_send_message_user_not_authorized(update: Update, context: CallbackContext):
    text = f'{update.effective_user.name}, Вы должны быть зарегистрированы чтобы воспользоваться мною. Напишите администраторам @iLeonidze или @Foeniculum'
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=text,
                             reply_to_message_id=update.message.message_id)


def bot_send_message_this_command_bot_allowed_here(update: Update, context: CallbackContext):
    text = f'Эта команда недопустима здесь'
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=text,
                             reply_to_message_id=update.message.message_id)


def start_identification(update: Update, context: CallbackContext):
    text = f'Привет\\!\nЯ бот из дома 2к6 \\(бывший 34\\)\\. Чтобы воспользоваться мной и попасть в закрытый чат, необходимо ' \
           f'пройти идентификацию по [этой инструкции](https://sal34.notion.site/sal34/28-2-6-FAQ-39a269ac25924dacbb3dc589b1579d5b#5b071d6efb30417ebf5cf311645f41b1)\\. ' \
           f'\n\nПомимо этого, у дома есть [открытый чат](https://t.me/salarevo34)\\.'
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=text, parse_mode='MarkdownV2')
    return


def proceed_private_dialog_send_profile(update: Update, context: CallbackContext):
    pass


def proceed_private_dialog_send_objects(update: Update, context: CallbackContext):
    pass


def proceed_private_dialog_get_neighbours(update: Update, context: CallbackContext):
    pass


PRIVATE_KEYBOARD_LAYOUTS = {
    'main': [
        [
            {'name': 'Мой профиль', 'action': proceed_private_dialog_send_profile},
            {'name': 'Мои объекты', 'action': proceed_private_dialog_send_objects}
        ],
        [
            {'name': 'Узнать моих соседей', 'action': proceed_private_dialog_get_neighbours}
        ]
    ]
}


def set_keyboard_context(name: str):
    keyboard_raw = PRIVATE_KEYBOARD_LAYOUTS[name]
    buttons_list = []
    for key_list in keyboard_raw:
        key_row = []
        for key_props in key_list:
            key_row.append(KeyboardButton(key_props['name']))
        buttons_list.append(key_row)

    return ReplyKeyboardMarkup(buttons_list, resize_keyboard=False)


def bot_command_start(update: Update, context: CallbackContext):
    if update.message.chat.type != 'private':
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    this_user = USERS_CACHE.get_user(update)
    if not this_user.is_identified():
        return start_identification(update, context)

    text = 'Привет!\nКажется, Вы уже прошли идентификацию и для Вас уже всё доступно.\n' \
           'Выберите из меню что Вы хотите узнать.'

    reply_markup = set_keyboard_context('main')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=text, reply_markup=reply_markup)


def proceed_private_dialog(update: Update, context: CallbackContext):
    pass
    # context.bot.send_message(chat_id=update.effective_chat.id,
    #                          text='?')


def form_objects_list_string(user: User) -> str:
    text = 'Объекты:\n'

    for object_type in ['кв', 'кл', 'мм', 'нж']:
        if object_type in user.own_object_types:
            # text += '\n' + OBJECT_TYPES_NAMES[object_type] + '\n'
            for index, object_entry in user.db_entries.loc[user.db_entries['object_type'] == object_type].iterrows():

                text += '• ' + OBJECT_TYPES_NAMES[object_type].lower() + ' '

                floor_str = ''
                if object_type == 'кв':
                    floor_str = f' на {object_entry["floor"]} этаже'

                entrance_str = ''
                if object_type != 'мм':
                    in_letter = 'в'
                    if str(object_entry["entrance"]) == '2':
                        in_letter = 'во'
                    entrance_str = f' {in_letter} {object_entry["entrance"]}\\-й секции'

                rooms_str = ''
                if object_type != 'мм' and object_type != 'кв':
                    rooms_str += f' {object_entry["area"]} кв\\.м'

                area_str = ''
                if object_type == 'кв':
                    if int(object_entry["rooms"]) == 1:
                        area_str += f' однокомнатная'
                    else:
                        area_str += f' {object_entry["rooms"]}\\-х комнатная'

                appendix_str = f'{rooms_str}{area_str}'

                if appendix_str != '':
                    appendix_str += ', '

                appendix_str += object_entry["user_type"]

                if appendix_str != '':
                    appendix_str = ' \\(' + appendix_str.strip() + '\\)'

                text += f'{str(object_entry["number"])}{floor_str}{entrance_str}{appendix_str}\n'

    return text.strip()


def form_related_users_list_sting(user: User):
    if user.related_users_objects is None or user.related_users_objects.empty:
        return ''

    text = 'Coжители:'
    for index, row in user.get_related_users().iterrows():

        fullname = f'{row["surname"]} {row["name"]}'
        if row["patronymic"]:
            fullname += f' {row["patronymic"]}'

        if row["telegram"]:
            text += '\n• [' + fullname + '](tg://user?id=' + str(row["telegram"]) + ')'
        else:
            text += '\n• ' + fullname

        if row["phone"]:
            text += f', `\\+{row["phone"]}`'
        else:
            text += ', телефон не указан'

        # if row['added_to_group'] == 'YES':
        #     text += ' \\(в группе\\)'
        # else:
        #     text += ' \\(не в группе\\)'

    return text.strip()


def get_short_object_type_str_by_id(object_type_id: str):
    if object_type_id == 'p':
        return 'мм'
    elif object_type_id == 's':
        return 'кл'
    else:
        return 'кв'


def get_neighbours_list_str(neighbours: Dict[str, Dict[str, Dict[str, Any[str, List[Any[User, List[str]]]]]]],
                           private: bool = False,
                           show_objects: bool = False,
                           split_floors: bool = False) -> str:
    text = ''
    lines = 0

    for floor_number, objects in neighbours.items():

        if split_floors:
            if floor_number != '-1' or len(neighbours) != 1:
                text += f'\n\n*{encode_markdown(str(floor_number))} этаж*'

        for object_number, object_description in objects.items():
            users_strs = []
            for user in object_description['users']:
                if isinstance(user, User):
                    if not private:
                        user_str = user.get_linked_shortname()
                    else:
                        user_str = user.get_linked_seminame() + ' тел\\. \\' + user.get_public_phone()
                else:
                    user_str = 'нет '
                    if not private:
                        if len(user) > 1 and len(user[1]) > 0:
                            user_str += user[0] + ' ' + user[1][0] + '\\.'
                        else:
                            user_str += user[0]
                    else:
                        if len(user) > 1 and len(user[1]) > 0:
                            user_str += user[0] + ' ' + user[1]
                        else:
                            user_str += user[0]

                users_strs.append(user_str)

            text += '\n\\- '
            if not split_floors:
                if str(floor_number)[0] == '-':
                    text += '\\'
                text += f'{floor_number} этаж, '

            if show_objects:
                text += f'{object_number} '

                if floor_number != '-1':
                    text += f'\\({object_description["position"]}\\) '

                text += f'{object_description["type"]}: '

            text += "; ".join(users_strs)

            lines += 1
            if lines > 100:
                text += 'XXX_SPLITTER_XXX'
                lines = 0

    return text.strip()


def bot_command_neighbours(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if update.effective_chat.type != 'private' and not is_found_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    if is_admin_chat:
        if update.message.reply_to_message:
            requested_user = USERS_CACHE.get_user(update.message.reply_to_message.forward_from.id)
            neighbours = requested_user.get_neighbours()
        else:
            neighbours = USERS_CACHE.get_neighbours_from_section(chat_building)

        text = get_neighbours_list_str(neighbours,
                                      private=True,
                                      show_objects=True,
                                      split_floors=True)
    elif not chat_section:
        is_private = update.effective_chat.type == 'private'
        if update.message.reply_to_message:
            requested_user = USERS_CACHE.get_user(update.message.reply_to_message.forward_from.id)
            neighbours = requested_user.get_neighbours()
            if neighbours:
                text = f'{requested_user.get_linked_shortname()} имеет ближайших соседей:\n' \
                       + get_neighbours_list_str(neighbours, private=is_private)
            else:
                text = f'{requested_user.get_linked_shortname()} не имеет соседей рядом'
        else:
            if not is_private:
                text = 'Используйте эту команду в чате секции или в приватной беседе, ' \
                       'здесь её использовать нельзя'
            else:
                neighbours = this_user.get_neighbours()
                if neighbours:
                    text = 'Ваши ближайшие соседи:\n' \
                           + get_neighbours_list_str(neighbours, private=is_private, show_objects=True)
                else:
                    text = 'К сожалению, у Вас еще нет соседей рядом'
    else:
        if update.message.reply_to_message:
            requested_user = USERS_CACHE.get_user(update.message.reply_to_message.forward_from.id)
            neighbours = requested_user.get_neighbours(section=chat_section)
            if neighbours:
                text = f'{requested_user.get_linked_shortname()} имеет ближайших соседей:\n' \
                       + get_neighbours_list_str(neighbours, private=False, show_objects=True)
            else:
                text = f'{requested_user.get_linked_shortname()} не имеет соседей рядом'
        else:
            neighbours = USERS_CACHE.get_neighbours_from_section(chat_building, chat_section)
            text = get_neighbours_list_str(neighbours,
                                          private=False,
                                          show_objects=True,
                                          split_floors=True)

    for text_part in text.split('XXX_SPLITTER_XXX'):
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text_part,
                                 reply_to_message_id=update.message.message_id,
                                 disable_notification=True,
                                 parse_mode='MarkdownV2')


def bot_command_who_is_this(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if update.effective_chat.type != 'private' and not is_found_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    requested_user: User or int or None = None
    reply_to_message_id = None

    if is_admin_chat:
        requested_user = raw_try_send_user_link(update, context)

    if isinstance(requested_user, int):
        requested_user_id = requested_user
        requested_user = None
    elif not update.message.reply_to_message:
        requested_user_id = update.message.from_user.id
        reply_to_message_id = update.message.message_id
    elif update.message.reply_to_message.contact:
        requested_user_id = update.effective_user.id
    elif update.message.reply_to_message.forward_from:
        requested_user_id = update.message.reply_to_message.forward_from.id
    else:
        requested_user_id = update.message.reply_to_message.from_user.id

    if not reply_to_message_id:
        reply_to_message_id = update.message.reply_to_message.message_id

    if not requested_user:
        requested_user = USERS_CACHE.get_user(requested_user_id)
    else:
        requested_user_id = requested_user.telegram_id

    reply_markup = None

    if not requested_user.is_identified():
        text = 'Я не знаю кто это'

        if is_admin_chat and requested_user_id != -1:
            text = f'{text}\nID пользователя: `{str(requested_user_id)}`'
            if update.message.reply_to_message.contact:
                text = f'{text}\nТелефон: `{str(update.message.reply_to_message.contact.phone_number)}`'

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text,
                                 reply_to_message_id=update.message.reply_to_message.message_id,
                                 parse_mode='MarkdownV2')
        return

    if update.effective_chat.type == 'private':
        return proceed_private_dialog_send_profile(update, context)

    if not is_admin_chat:
        if requested_user == this_user:
            who_form = 'Вы'
        else:
            who_form = 'Это'
        text = f'{who_form} ' + requested_user.get_linked_shortname()
    else:
        text = 'Это ' + requested_user.get_linked_fullname()

        if requested_user.phone is not None:
            text += f', `\\+{requested_user.phone["number"]}`'
            if requested_user.phone['visible']:
                text += ' \\(виден\\)'
            else:
                text += ' \\(скрыт\\)'
        else:
            text += f' без телефона'

        if requested_user.add_to_group:
            text += '\nДобавить в группу: Да'
        else:
            text += '\nДобавить в группу: Нет'

        status_str, added_everywhere = requested_user.get_str_user_related_groups_status()
        text += '\n\n' + status_str

        text += '\n\n' + form_objects_list_string(requested_user)

        related_users_str = form_related_users_list_sting(requested_user)
        if related_users_str != '':
            text += '\n\n' + related_users_str

        reply_markup = InlineKeyboardMarkup([
            [
                InlineKeyboardButton("Изменить ФИО", callback_data=f'change_fullname|{requested_user_id}'),
                InlineKeyboardButton("Изменить тип жителя", callback_data=f'change_user_type|{requested_user_id}')
            ],
            [
                InlineKeyboardButton("Изменить телефон", callback_data=f'change_phone|{requested_user_id}'),
                InlineKeyboardButton("Изменить согласие", callback_data=f'change_phone_visibility|{requested_user_id}')
            ],
            [
                InlineKeyboardButton("Добавить в чаты", callback_data=f'add_to_chats|{requested_user_id}'),
                InlineKeyboardButton("Удалить из чатов", callback_data=f'remove_from_chats|{requested_user_id}')
            ],
            [
                InlineKeyboardButton("Отозвать доступ к боту", callback_data=f'lock_bot_access|{requested_user_id}'),
                InlineKeyboardButton("Деактивировать пользователя", callback_data=f'deactivate_user|{requested_user_id}')
            ]
        ], resize_keyboard=False)

    if not is_admin_chat:

        if chat_section is None:

            if len(requested_user.from_sections) == 1:
                if requested_user.from_sections[0]['type'] == 'p':
                    text = f'{text} с паркинга'
                elif requested_user.from_sections[0]['type'] == 's':
                    text = f'{text} из кладовок {requested_user.from_sections[0]["number"]}\\-й секции'
                else:
                    text = f'{text} из {requested_user.from_sections[0]["number"]}\\-й секции'

            else:
                is_parking_found = False
                is_storage_found = False
                is_flat_found = False
                for section in requested_user.from_sections:
                    if section['type'] == 'p':
                        is_parking_found = True
                    if section['type'] == 's':
                        is_storage_found = True
                    if section['type'] == 'f':
                        is_flat_found = True

                if not is_flat_found:
                    if is_parking_found and is_storage_found:
                        # TODO: show storages number
                        text = f'{text} из паркинга и кладовок'
                    elif is_parking_found:
                        text = f'{text} из паркинга'
                    else:
                        # TODO: show storages number
                        text = f'{text} из кладовок'
                else:
                    sections_strs = []
                    for section in requested_user.from_sections:
                        if section['id'] == 'p':
                            sections_strs.append('паркинга')
                        elif section['id'] == 's':
                            sections_strs.append(f'кладовок в {section["number"]}\\-й секции')
                        else:
                            sections_strs.append(f'{section["number"]}\\-й секции')
                    sections_str_joined = "\\, ".join(list(set(sections_strs)))
                    text = f'{text} из {sections_str_joined}'

        else:
            object_type_str = 'Квартира'
            if chat_section == 'p':
                object_type_str = 'Парковочное место'
            elif chat_section == 's':
                object_type_str = 'Кладовка'
            for floor_number in requested_user.get_floors(chat_building, chat_section):
                for object_number in requested_user.get_object_numbers(chat_building, chat_section, floor_number):
                    text += f'\n{object_type_str} {object_number}'
                    if chat_section not in ['p', 's']:
                        text += f' на {floor_number}\\-м этаже'

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=str(text),
                             reply_to_message_id=reply_to_message_id,
                             parse_mode='MarkdownV2',
                             reply_markup=reply_markup)


def bot_command_stats(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    # TODO: allow users for asking stats in private messages
    if not is_found_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    text = ''
    table = DB[chat_building]

    if is_found_chat and chat_section is None:
        # print stats for entire building or from user private chat
        objects = table[table['added_to_group'] == 'YES'][['object_type', 'number', 'entrance']].drop_duplicates()
        text += 'Сейчас в чате дома представители:'

        for object_type in ['кв', 'кл', 'мм', 'нж']:
            amount = len(objects[objects['object_type'] == object_type].index)
            object_type_max = CONFIGS['buildings'][chat_building]['objects_amount'][object_type]
            percent = math.floor(amount / object_type_max * 100)
            text += f'\n• {object_type}: {str(amount)} / {str(object_type_max)} ({str(percent)}%)'

        text += '\n\nКоличество добавленных квартир по секциям:'
        for number, value in objects[objects['object_type'] == 'кв'].groupby(by="entrance").size().iteritems():
            tb_flats = table[table['object_type'] == 'кв']
            section_max = len(tb_flats[tb_flats['entrance'] == number][['object_type', 'number', 'entrance']].drop_duplicates().index)
            section_percent = math.floor(value / section_max * 100)
            text += f'\n{number} секция: {value} / {str(section_max)} ({str(section_percent)}%)'

        if is_admin_chat:
            text += f'\n\nАдминская статистика\n\n'

            text += f'Таблицы:' \
                    f'\n- Таблиц в памяти: {len(DB)}' \
                    f'\n- Последняя синхронизация: {int(time.time()-TABLES_RELOADED_TIME)} сек. назад'

            cache_stats = USERS_CACHE.get_stats()
            text += f'\n\nКэш:' \
                    f'\n- Пользователей в кэше: {cache_stats["cached_users"]}' \
                    f'\n- Ожидающие сохранения: {cache_stats["users_save_queue"]}' \
                    f'\n- Последний флаш: {int(cache_stats["time_since_last_save"])} сек. назад' \
                    f'\n- Устаревание последнего закэшированного: {int(time.time() - LAST_STALED_USER_CACHE)} сек. назад'

            text += f'\n\nОчередь действий:' \
                    f'\n- Запланировано в очереди: {len(QUEUED_ACTIONS)}' \
                    f'\n- Последнее исполнение очереди: {int(time.time() - QUEUED_ACTIONS_LAST_EXECUTED_TIME)} сек. назад'

    else:
        neighbours_table = table[(table['entrance'] == chat_section) & (table['added_to_group'] == 'YES') & (table['object_type'] == 'кв')][['number', 'floor']].drop_duplicates()
        text += f'Всего квартир {chat_section}-й секции в этом чате: {len(neighbours_table.index)}'

        text += f'\n\nКвартир в чате по каждому этажу:'
        size_columns = neighbours_table.groupby(by="floor").size()
        size_columns.index = size_columns.index.astype(int)
        for floor_number, value in size_columns.sort_index().iteritems():
            if floor_number != -1:
                text += f'\n{floor_number} этаж: {value}'

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=text,
                             reply_to_message_id=update.message.message_id)


def raw_try_send_user_link(update: Update, context: CallbackContext) -> User or int or None:
    text = update.message.reply_to_message.text

    if not text:
        return None

    if text[0] == '@':
        user = tg_client_get_user_by_username(text)
        if user:
            return user

    user_id = text.replace('+', '')
    if not user_id.isdigit():
        return None
    else:
        for number, building_table in DB.items():
            user_rows = building_table[((building_table['telegram'] == user_id) | (building_table['phone'] == user_id))]
            if not user_rows.empty:
                user_row = user_rows.iloc[0]
                telegram_id = user_row['telegram']
                if telegram_id:
                    user = USERS_CACHE.get_user(telegram_id)
                    return user

        # if not detected
        user_id = tg_client_get_user_id_by_phone(text)
        if user_id:
            return user_id

    return -1


def bot_command_help(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    commands = [
        ['help', 'Выводит это сообщение о том как пользоваться ботом'],
        ['neighbours', 'Список соседей\nВызов в общем чате покажет Ваших ближайших соседей, вызов в секции покажет имена и ссылки всех соседей секции поэтажно. Если в чате секции ответить этой командой на сообщение, то Вы сможете увидеть ближайших соседей человека, информацию о котором Вы ищете (в общем чате это не работает)'],
        ['who', 'Узнать информацию о соседе или о себе\nНеобходимо вызывать эту команду ответив на чье-то сообщение, информацию о котором Вы хотите узнать. Вызов в общем чате покажет из какой секции, вызов в секции покажет с какого этажа и номер объекта недвижимости. Если вызвать команду без реплая, то будет выведена информация о Вас.'],
        ['stats', 'Общая статистика по дому или секции\nВ общем чате показывает сколько соседей добавлено в базу, в чате секции показывает количество жильцов на каждом этаже'],
    ]

    admin_commands = [
        ['who', 'Выводит всю информацию о человеке по одному из заданных параметров:\n- Сообщение\n- Форвард сообщения\n- Контакт\n- Username\n- Номер телефона\n- Номер телефона вне нашей базы\n- ID телеграма'],
        ['reload', 'Сохраняет контекстные данные, сбрасывает все кэши и заново синхронизирует таблицы (это действие высвободит память, но может привести к снижению производительности бота)'],
        # ['reload_db', 'Вызывает принудительную синхронизацию всех таблиц БД'],
        ['start_tables_sync', 'Начинает синхронизацию таблиц БД'],
        ['stop_tables_sync', 'Останавливает синхронизацию таблиц БД'],
        ['flush_users_context', 'Вызывает принудительное сохранение контекстных данных пользователей, ожидающих сохранения'],
        ['flush_all_users_context', 'Вызывает принудительное сохранение контекстных данных ВСЕХ пользователей, находящихся в кэше'],
        ['start_users_context_autosave', 'Запускает автосохранение контекстных данных пользователей'],
        ['stop_users_context_autosave', 'Останавливает автосохранение контекстных данных пользователей'],
        ['start_cached_users_stale', 'Запускает устаревание и автоматическое извлечение старых пользователей из кэша'],
        ['stop_cached_users_stale', 'Прекращает устаревание и автоматическое извлечение старых пользователей из кэша'],
        ['recalculate_stats', 'Вызывает перерасчет всей статистики'],
        ['reset_actions_queue', 'Сбросить очередь запланированных действий (в т.ч. сбрасывает очередь удаления мусора)'],
        ['start_actions_queue', 'Запустить исполнение накопленной очереди действий'],
        ['stop_actions_queue', 'Остановить исполнение накопленной очереди действий'],
        ['add_all_users_to_chats', 'Принудительно добавляет всех пользователей в соответствующие им чаты'],
        ['add_all_users_to_chat', 'Принудительно добавляет всех пользователей в заданный чат'],
        ['revalidate_users_groups', 'Ревалидирует наличие пользователя в группах'],
    ]

    message = encode_markdown('В чатах дома есть бот-ассистент, который помогает соседям. Также '
                              'боту можно написать в личные сообщения, нажав на его аватарку '
                              'слева. Боту можно задать интересующий вас вопрос: напишите обычное '
                              'сообщение со своим вопросом, а в начале сообщение не забудьте '
                              'позвать бота написав "Бот,"\n\nВот на что бот умеет отвечать:')

    global HELP_ASSISTANT
    for entry in HELP_ASSISTANT.db:
        message += f'\n\n*{encode_markdown(entry["name"])}*\n`Бот, {encode_markdown(entry["test_queries"][0].lower())}`'

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=message,
                             reply_to_message_id=update.message.message_id,
                             parse_mode='MarkdownV2')



    # for command in commands:
    #     message += f'\n\n/{command[0]}\n{command[1]}'

    if is_admin_chat:
        message = '*Админские команды*'
        for admin_command in admin_commands:
            message += f'\n\n/{encode_markdown(admin_command[0])}\n{encode_markdown(admin_command[1])}'

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=message,
                                 reply_to_message_id=update.message.message_id,
                                 parse_mode='MarkdownV2')


def bot_command_reload_db(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested tables force reload!')

    reload_tables()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Таблицы синхронизированы',
                             reply_to_message_id=update.message.message_id)


def bot_command_reload(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested caches eviction!')

    USERS_CACHE.evict()
    reload_tables()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Кэши очищены и таблицы синхронизированы',
                             reply_to_message_id=update.message.message_id)


def bot_command_start_tables_sync(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested tables sync start!')

    start_tables_sync()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Синхронизация таблиц запущена',
                             reply_to_message_id=update.message.message_id)


def bot_command_stop_tables_sync(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested tables sync stop!')

    stop_tables_sync()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Синхронизация таблиц остановлена',
                             reply_to_message_id=update.message.message_id)


def bot_command_flush_users_context(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested users context flush!')

    USERS_CACHE.save_users()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Зафлашены все закэшированные пользователи, которые ожидали флаша',
                             reply_to_message_id=update.message.message_id)


def bot_command_flush_all_users_context(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested all users force context flush!')

    USERS_CACHE.save_all_users()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Принудительно зафлашены все закэшированные пользователи',
                             reply_to_message_id=update.message.message_id)


def bot_command_start_users_context_autosave(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested start users context autosave!')

    start_users_context_save()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Автоматическое отложенное сохранение запущено',
                             reply_to_message_id=update.message.message_id)


def bot_command_stop_users_context_autosave(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested stop users context autosave!')

    stop_users_context_save()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Автоматическое отложенное сохранение остновлено, все сохранения будут происходить синхронно',
                             reply_to_message_id=update.message.message_id)


def bot_command_start_cached_users_stale(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested start users staling!')

    start_caches_stale()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Устаревание кэшей запущено',
                             reply_to_message_id=update.message.message_id)


def bot_command_stop_cached_users_stale(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested stop users staling!')

    stop_caches_stale()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Устаревание кэшей остановлено',
                             reply_to_message_id=update.message.message_id)


def bot_command_recalculate_stats(update: Update, context: CallbackContext):
    # TODO
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Сейчас это недоступно',
                             reply_to_message_id=update.message.message_id)


def bot_command_reset_actions_queue(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested actions queue reset!')

    reset_actions_queue()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Запланированная очередь действий сброшена',
                             reply_to_message_id=update.message.message_id)


def bot_command_start_actions_queue(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested start actions queue!')

    start_actions_queue()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Запущено исполнение запланированной очереди действий',
                             reply_to_message_id=update.message.message_id)


def bot_command_stop_actions_queue(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested stop actions queue!')

    stop_actions_queue()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Исполнение запланированной очереди действий остановлено',
                             reply_to_message_id=update.message.message_id)


def bot_command_revalidate_users_groups(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat or not chat_building:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested to revalidate all users in groups!')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Ревалидирую всех соседей в группах...',
                             reply_to_message_id=update.message.message_id)

    added_everywhere_counter = 0
    not_added_everywhere_counter = 0
    users = get_all_users(chat_building)
    for i, user in enumerate(users):
        if not user.add_to_group:
            continue

        text = user.get_linked_fullname() + ' `' + str(user.telegram_id) + '`\n'

        status_str, added_everywhere = user.get_str_user_related_groups_status()
        text += status_str

        if not added_everywhere:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     parse_mode='MarkdownV2',
                                     text=text)
            not_added_everywhere_counter += 1
        else:
            added_everywhere_counter += 1

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Готово!\n\n'
                                  f'Находятся во всех группах: {added_everywhere_counter}\n'
                                  f'Отсутствуют в каких то группах: {not_added_everywhere_counter}',
                             reply_to_message_id=update.message.message_id)


def bot_command_add_all_users_to_chats(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat or not chat_building:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    print('Admin requested add users to all chats!')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Начинаю добавление в чаты всех пользователей...',
                             reply_to_message_id=update.message.message_id)

    users = get_all_users(chat_building)
    for i, user in enumerate(users):
        if user.is_added_to_all_groups():
            continue

        # TODO: remove this if statement and sub-block?
        if not user.add_to_group:
            # context.bot.send_message(chat_id=update.effective_chat.id,
            #                          text=f'{i+1}/{len(users)} ПРОПУЩЕН "{user.get_fullname()}"')
            continue

        try:
            user.add_to_all_chats()
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'{i+1}/{len(users)} добавлен "{user.get_fullname()}"')
            time.sleep(60)
        except Exception as e:
            print('An exception occurred')
            print(traceback.format_exc())
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'{i+1}/{len(users)} НЕ УДАЛОСЬ ДОБАВИТЬ "{user.get_fullname()}"\n\n{str(e)}')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Все пользователи добавлены!',
                             reply_to_message_id=update.message.message_id)


def bot_command_add_all_users_to_chat(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat or not chat_building:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    buttons = []

    for chat in CONFIGS['buildings'][str(chat_building)]['groups']:
        if chat['name'] == 'admin':
            continue

        chat_name = get_chat_name_by_chat(chat)
        buttons.append([InlineKeyboardButton(f'{chat_name}',
                                             callback_data=f'bulk_add_to_chats|{chat["id"]}')])

    reply_markup = InlineKeyboardMarkup(buttons, resize_keyboard=False)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Выберите куда необходимо добавить всех пользователей',
                             reply_markup=reply_markup)


def cb_bulk_add_to_chats(update: Update, context: CallbackContext, *input_args):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)
    this_user = USERS_CACHE.get_user(update)

    if not is_admin_chat or not chat_building:
        bot_send_message_this_command_bot_allowed_here(update, context)
        return

    if not this_user.is_identified():
        bot_send_message_user_not_authorized(update, context)
        return

    if len(input_args) == 1:
        requested_chat_id = input_args[0]
    else:
        return

    requested_chat = None
    for chat in CONFIGS['buildings'][str(chat_building)]['groups']:
        if str(chat['id']) == str(requested_chat_id):
            requested_chat = chat
            break

    if not requested_chat:
        return

    print(f'Admin requested add users to chat {requested_chat_id} "{get_chat_name_by_chat(requested_chat)}"!')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Начинаю добавление в чат "{get_chat_name_by_chat(requested_chat)}" всех пользователей...')

    users = get_all_users(chat_building)
    for i, user in enumerate(users):
        if user.is_added_to_group(requested_chat_id):
            print(f'{user.get_fullname()} skipped\nalready added to group')
            continue

        # TODO: remove this if statement and sub-block?
        if not user.add_to_group or not user.is_chat_related(int(requested_chat_id)):
            # context.bot.send_message(chat_id=update.effective_chat.id,
            #                          text=f'{i+1}/{len(users)} ПРОПУЩЕН "{user.get_fullname()}"')
            print(f'{user.get_fullname()} skipped\nadd_to_group: {user.add_to_group}\nchat_related: {user.is_chat_related(int(requested_chat_id))}')
            time.sleep(30)
            continue

        try:
            user.add_to_chat(int(requested_chat_id))
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'{i+1}/{len(users)} добавлен "{user.get_fullname()}"')
            time.sleep(60)
        except Exception as e:
            print('An exception occurred')
            print(traceback.format_exc())
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'{i+1}/{len(users)} НЕ УДАЛОСЬ ДОБАВИТЬ "{user.get_fullname()}"\n\n{str(e)}')

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text='Все пользователи добавлены!',
                             reply_to_message_id=update.message.message_id)


def schedule_garbage_message_deletion(update: Update, timeout: int):
    print('Scheduled message deletion as a garbage')
    QUEUED_ACTIONS.append({
        'time': round(time.time()) + timeout,
        'type': 'delete',
        'chat_id': update.effective_chat.id,
        'message_id': update.message.message_id
    })

    # set stats for user, who sended garbage
    user = USERS_CACHE.get_user(update)
    user.context['stats']['total_garbage_detected_for_user'] += 1
    user.delayed_context_save()


def raw_try_setup_garbage_deletion(update: Update, context: CallbackContext) -> bool:
    cleaner_timeouts = CONFIGS['service']['scheduler']['clean_garbage']

    if update.message.sticker:
        schedule_garbage_message_deletion(update, cleaner_timeouts['sticker'])
        return True

    if update.message.animation:
        schedule_garbage_message_deletion(update, cleaner_timeouts['gif'])
        return True

    message_text = update.message.text

    if is_repeated_symbol(message_text) or is_emoji(message_text):
        schedule_garbage_message_deletion(update, cleaner_timeouts['emoji'])
        return True

    return False


def stats_collector(update: Update, context: CallbackContext):
    user = USERS_CACHE.get_user(update)
    chat_id = update.effective_chat.id

    current_time = int(time.time())

    if update.effective_chat.type != 'private':
        # TODO: support chat join
        if not user.context['joined_chats'].get(chat_id):
            user.context['joined_chats'][chat_id] = current_time

        # TODO: support chat leave
        # if not user.context['left_chats'].get(chat_id):
        #     user.context['left_chats'][chat_id] = int(time.time())

        if not user.context['last_activity_in_chats'].get(chat_id):
            user.context['last_activity_in_chats'][chat_id] = {}
        user.context['last_activity_in_chats'][chat_id]['date'] = int(time.time())
        user.context['last_activity_in_chats'][chat_id]['update_id'] = update.update_id
        user.context['last_activity_in_chats'][chat_id]['message_id'] = update.effective_message.message_id
        user.context['last_activity_in_chats'][chat_id]['message_text'] = update.effective_message.text

        user.context['stats']['sended_public_messages_total'] += 1

        if not user.context['stats']['sended_public_messages_per_chat'].get(chat_id):
            user.context['stats']['sended_public_messages_per_chat'][chat_id] = 0
        user.context['stats']['sended_public_messages_per_chat'][chat_id] += 1
    else:
        if user.context['private_chat'].get('bot_started') is None:
            user.context['private_chat']['bot_started'] = current_time

        user.context['stats']['sended_private_messages_total'] += 1

    user.delayed_context_save()

    return False


def bot_assistant_call(update: Update, context: CallbackContext):
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    user = USERS_CACHE.get_user(update)

    if not user.is_identified():
        return

    if is_bot_assistant_request(update):
        HELP_ASSISTANT.proceed_request(update, context, user, building_chats)


def no_command_handler(update: Update, context: CallbackContext) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_found_chat and update.message.chat.type == 'private':
        proceed_private_dialog(update, context)
        return

    if is_admin_chat and raw_try_respond_to_cb_action_message(update, context):
        return

    if is_found_chat and raw_try_setup_garbage_deletion(update, context):
        return


def cb_change_fullname(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) == 2:
        new_name = input_args[1]
        new_name_parts = new_name.split(' ')

        if len(new_name_parts) < 2 or len(new_name_parts) > 3:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text='Неверно введено ФИО, попробуйте снова.',
                                     reply_markup=ForceReply(force_reply=False),
                                     reply_to_message_id=update.message.message_id)
            return

        text = f'Житель [{user.get_fullname()}](https://t.me/{user.telegram_id}) успешно переименован, новое имя "{new_name}"'

        user.change_fullname(*new_name_parts)

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text,
                                 parse_mode='MarkdownV2',
                                 reply_markup=ForceReply(force_reply=False),
                                 reply_to_message_id=update.message.message_id)
        return

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Чтобы сменить имя жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) отправьте в ответ новое полное имя',
                             parse_mode='MarkdownV2',
                             reply_markup=ForceReply(force_reply=True))


def cb_change_user_type(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) == 2:
        new_type = input_args[1]
        new_type_i = -1

        if new_type == 'собственник':
            new_type_i = 0
        elif new_type == 'пользователь':
            new_type_i = 1

        if new_type_i == -1:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text='Неверно выбран тип жителя, попробуйте снова.',
                                     reply_to_message_id=update.message.message_id)
            return

        text = f'Тип жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) успешно сменен на "{new_type}"'

        user.change_user_type(new_type_i)

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text,
                                 parse_mode='MarkdownV2',
                                 reply_to_message_id=update.message.message_id)
        return

    buttons_list = [[KeyboardButton('собственник')], [KeyboardButton('пользователь')]]
    keyboard = ReplyKeyboardMarkup(buttons_list, resize_keyboard=False, one_time_keyboard=True)

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Чтобы сменить тип жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) выберите на клавиатуре подходящий тип и отправьте в ответ',
                             parse_mode='MarkdownV2',
                             reply_markup=keyboard)


def cb_change_phone(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) == 2:
        new_phone = input_args[1]

        if new_phone[0] != '+' or len(new_phone) != 12:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text='Неверно введен номер телефона, попробуйте снова.',
                                     reply_markup=ForceReply(force_reply=False),
                                     reply_to_message_id=update.message.message_id)
            return

        text = f'Номер телефона жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) успешно сменен, новый номер "\\{new_phone}"'

        user.change_phone(new_phone)

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text,
                                 parse_mode='MarkdownV2',
                                 reply_markup=ForceReply(force_reply=False),
                                 reply_to_message_id=update.message.message_id)
        return

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Чтобы сменить номер телефона жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) отправьте в ответ новый номер телефона, начинающийся с \\+7 и полностью состоящий из цифр, без пробелов и других символов',
                             parse_mode='MarkdownV2',
                             reply_markup=ForceReply(force_reply=True))


def cb_change_phone_visibility(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) == 2:
        new_visibility = input_args[1]
        new_visibility_bool: bool or None = None

        if new_visibility == 'виден':
            new_visibility_bool = True
        elif new_visibility == 'скрыт':
            new_visibility_bool = False

        if new_visibility_bool is None:
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text='Неверно выбран тип видимости телефона жителя, попробуйте снова.',
                                     reply_to_message_id=update.message.message_id)
            return

        text = f'Видимость телефона жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) успешно сменена на "{new_visibility}"'

        user.change_phone_visibility(new_visibility_bool)

        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=text,
                                 parse_mode='MarkdownV2',
                                 reply_to_message_id=update.message.message_id)
        return

    buttons_list = [[KeyboardButton('виден'), KeyboardButton('скрыт')]]
    keyboard = ReplyKeyboardMarkup(buttons_list, resize_keyboard=False, one_time_keyboard=True)

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Чтобы сменить видимость телефона жителя [{user.get_fullname()}](https://t.me/{user.telegram_id}) выберите на клавиатуре подходящий тип и отправьте в ответ',
                             parse_mode='MarkdownV2',
                             reply_markup=keyboard)


def get_chat_name_by_chat(chat) -> str:
    if chat["name"] == 'private_common_group':
        chat_name = 'Общая группа'
    elif chat["name"] == 'public_info_channel':
        chat_name = 'Канал'
    elif chat["name"] == 'guards_group':
        chat_name = 'Охрана'
    elif chat["name"] == 'private_section_group':
        if chat['section'] == 'p':
            chat_name = 'Паркинг'
        elif chat['section'] == 's':
            chat_name = 'Кладовки'
        else:
            chat_name = 'Секция ' + str(chat["section"])
    else:
        chat_name = chat["name"]
    return chat_name


def cb_add_to_chats(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) > 1:
        target_chat_request = input_args[1]
        if target_chat_request == 'links':
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'Запрошен список ссылок для пользователя "{user.get_fullname()}". Перешлите ему следующее сообщение с приглашением:')
            invite_links = tg_client_get_invites_for_chats(user.get_related_chats_ids())
            invite_links_str = "\n".join(invite_links)
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'Добро пожаловать! Заходите в чаты по ссылкам:\n{invite_links_str}\n\nСсылками можно воспользоваться один раз и они действительны 24 часа')
        elif target_chat_request == 'all':
            try:
                user.add_to_all_chats()
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Пользователь "{user.get_fullname()}" добавлен во все чаты')
            except UserPrivacyRestrictedError as e:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Пользователь "{user.get_fullname()}" запретил приглашать его в группы. Перешлите ему следующее сообщение с приглашением:')
                invite_links = tg_client_get_invites_for_chats(user.get_related_chats_ids())
                invite_links_str = "\n".join(invite_links)
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Добро пожаловать! Заходите в чаты по ссылкам:\n{invite_links_str}\n\nСсылками можно воспользоваться один раз и они действительны 24 часа')
            except Exception as e:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Не удалось добавить пользователя "{user.get_fullname()}" во все чаты\n\n{str(e)}')
        else:
            found_chat = None
            for chat in user.get_related_chats():
                if chat['id'] == int(target_chat_request):
                    found_chat = chat
                    break

            if not found_chat:
                return

            chat_name = get_chat_name_by_chat(found_chat)

            try:
                user.add_to_chat(int(target_chat_request))
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Пользователь "{user.get_fullname()}" добавлен в чат "{chat_name}"')
            except UserPrivacyRestrictedError as e:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Пользователь "{user.get_fullname()}" запретил приглашать его в группы. Перешлите ему следующее сообщение с приглашением:')
                invite_link = tg_client_get_invite_for_chat(int(target_chat_request))
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Заходите в чат по ссылке:\n{invite_link}\n\nСсылкой можно воспользоваться один раз и она действительна 24 часа')
            except Exception as e:
                context.bot.send_message(chat_id=update.effective_chat.id,
                                         text=f'Не удалось добавить пользователя "{user.get_fullname()}" в чат "{chat_name}"\n\n{str(e)}')

        return

    buttons = [
            [InlineKeyboardButton("Добавить везде", callback_data=f'add_to_chats|{user.telegram_id}|all')]
        ]

    user_related_chats = user.get_related_chats()

    for chat in user_related_chats:
        chat_name = get_chat_name_by_chat(chat)
        buttons.append([InlineKeyboardButton(f'{chat_name}', callback_data=f'add_to_chats|{user.telegram_id}|{chat["id"]}')])

    buttons.append([InlineKeyboardButton("Список ссылок", callback_data=f'add_to_chats|{user.telegram_id}|links')])

    reply_markup = InlineKeyboardMarkup(buttons, resize_keyboard=False)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Выберете куда необходимо добавить пользователя "{user.get_fullname()}"',
                             reply_markup=reply_markup)


def cb_remove_from_chats(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat and update.message.chat.type != 'private':
        return

    try:
        user: User = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if len(input_args) > 1:
        target_chat_request = int(input_args[1])
        if target_chat_request == 'all':
            user.remove_from_all_chats()
            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'Житель "{user.get_fullname()}" удален из всех чатов')
        else:
            found_chat = None
            for chat in user.get_related_chats():
                if chat['id'] == target_chat_request:
                    found_chat = chat
                    break

            if not found_chat:
                return

            chat_name = get_chat_name_by_chat(found_chat)

            user.remove_from_chat(target_chat_request)

            context.bot.send_message(chat_id=update.effective_chat.id,
                                     text=f'Житель "{user.get_fullname()}" удален из чата "{chat_name}"')

        return

    buttons = [
            [InlineKeyboardButton("Удалить отовсюду", callback_data=f'remove_from_chats|{user.telegram_id}|all')]
        ]

    user_related_chats = user.get_related_chats()

    for chat in user_related_chats:
        chat_name = get_chat_name_by_chat(chat)
        buttons.append([InlineKeyboardButton(f'{chat_name}', callback_data=f'remove_from_chats|{user.telegram_id}|{chat["id"]}')])

    reply_markup = InlineKeyboardMarkup(buttons, resize_keyboard=False)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Выберете откуда необходимо удалить жителя "{user.get_fullname()}"',
                             reply_markup=reply_markup)


def cb_lock_bot_access(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat:
        return

    try:
        user = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if not user.is_identified():
        context.bot.send_message(chat_id=update.effective_chat.id,
                                text=f'Нельзя отозвать доступ у жителя {input_args}')
        return

    reply_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Подтвердить",
                                 callback_data=f'lock_bot_access_submit|{user.telegram_id}')
        ]
    ], resize_keyboard=False)

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Подтвердите отзыв доступа к боту для жителя "{user.get_fullname()}"',
                             reply_markup=reply_markup)


def cb_lock_bot_access_submit(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat:
        return

    try:
        user = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if not user.is_identified():
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'Нельзя отозвать доступ у жителя {input_args}')
        return

    fullname = user.get_fullname()
    user.lock_bot_access()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Доступ к боту отозван у жителя "{fullname}"')


def cb_deactivate_user(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat:
        return

    try:
        user = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if not user.is_identified():
        context.bot.send_message(chat_id=update.effective_chat.id,
                                text=f'Нельзя деактивировать жителя {input_args}')
        return

    reply_markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Подтвердить",
                                 callback_data=f'deactivate_user_submit|{user.telegram_id}')
        ]
    ], resize_keyboard=False)

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Подтвердите дективировацию жителя "{user.get_fullname()}"\nЭто действие приведет к удалению из всех чатов и отзыву доступа к боту',
                             reply_markup=reply_markup)


def cb_deactivate_user_submit(update: Update, context: CallbackContext, *input_args) -> None:
    is_found_chat, chat_building, is_admin_chat, chat_name, chat_section, building_chats \
        = identify_chat_by_tg_update(update)

    if not is_admin_chat:
        return

    try:
        user = USERS_CACHE.get_user(int(input_args[0]))
    except Exception:
        print('Failed to parse input arguments for cb')
        return

    if not user.is_identified():
        context.bot.send_message(chat_id=update.effective_chat.id,
                                 text=f'Нельзя деактивировать жителя {input_args}')
        return

    fullname = user.get_fullname()
    user.deactivate()

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=f'Житель "{fullname}" деактивирован')


def raw_try_respond_to_cb_action_message(update: Update, context: CallbackContext) -> bool:
    try:
        if update.message.reply_to_message and len(update.message.reply_to_message.entities) == 1:
            text = update.message.reply_to_message.text
            user_id = int(update.message.reply_to_message.entities[0].url.split('t.me/')[1])
            for cb_fn_key, cb_fn_keyword in callback_functions_keywords.items():
                if cb_fn_keyword in text:
                    callback_functions[cb_fn_key](update, context, user_id, update.message.text)
                    return True
    finally:
        return False


callback_functions = {
    'change_fullname': cb_change_fullname,
    'change_user_type': cb_change_user_type,
    'change_phone': cb_change_phone,
    'change_phone_visibility': cb_change_phone_visibility,
    'add_to_chats': cb_add_to_chats,
    'remove_from_chats': cb_remove_from_chats,
    'lock_bot_access': cb_lock_bot_access,
    'lock_bot_access_submit': cb_lock_bot_access_submit,
    'deactivate_user': cb_deactivate_user,
    'deactivate_user_submit': cb_deactivate_user_submit,
    'bulk_add_to_chats': cb_bulk_add_to_chats,
}

callback_functions_keywords = {
    'change_fullname': 'сменить имя жителя',
    'change_user_type': 'сменить тип жителя',
    'change_phone': 'сменить номер телефона жителя',
    'change_phone_visibility': 'сменить видимость телефона жителя'
}


def handle_button_callback(update: Update, context: CallbackContext) -> None:
    query = update.callback_query
    query.answer()

    choice = query.data

    function_name, *payload = choice.split('|')

    callback_functions[function_name](update, context, *payload)


def setup_command_handlers(tg_dispatcher):

    tg_dispatcher.add_handler(MessageHandler(Filters.all, stats_collector), group=-1)

    tg_dispatcher.add_handler(MessageHandler(Filters.all, bot_assistant_call), group=-2)

    start_handler = CommandHandler('start', bot_command_start)
    tg_dispatcher.add_handler(start_handler)

    who_handler = CommandHandler('neighbours', bot_command_neighbours)
    tg_dispatcher.add_handler(who_handler)

    who_handler = CommandHandler('who', bot_command_who_is_this)
    tg_dispatcher.add_handler(who_handler)

    stats_handler = CommandHandler('stats', bot_command_stats)
    tg_dispatcher.add_handler(stats_handler)

    help_handler = CommandHandler('help', bot_command_help)
    tg_dispatcher.add_handler(help_handler)

    # TODO: remove this old menu
    help_assistant_handler = CommandHandler('assistant_help', bot_command_help)
    tg_dispatcher.add_handler(help_assistant_handler)

    # Admin commands

    reload_db_handler = CommandHandler('reload', bot_command_reload)
    tg_dispatcher.add_handler(reload_db_handler)

    # reload_db_handler = CommandHandler('reload_db', bot_command_reload_db)
    # tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('start_tables_sync', bot_command_start_tables_sync)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('stop_tables_sync', bot_command_stop_tables_sync)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('flush_users_context', bot_command_flush_users_context)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('flush_all_users_context', bot_command_flush_all_users_context)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('start_users_context_autosave', bot_command_start_users_context_autosave)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('stop_users_context_autosave', bot_command_stop_users_context_autosave)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('start_cached_users_stale', bot_command_start_cached_users_stale)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('stop_cached_users_stale', bot_command_stop_cached_users_stale)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('recalculate_stats', bot_command_recalculate_stats)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('reset_actions_queue', bot_command_reset_actions_queue)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('start_actions_queue', bot_command_start_actions_queue)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('stop_actions_queue', bot_command_stop_actions_queue)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('add_all_users_to_chats', bot_command_add_all_users_to_chats)
    tg_dispatcher.add_handler(reload_db_handler)

    reload_db_handler = CommandHandler('add_all_users_to_chat', bot_command_add_all_users_to_chat)
    tg_dispatcher.add_handler(reload_db_handler)

    revalidate_users_groups_handler = CommandHandler('revalidate_users_groups', bot_command_revalidate_users_groups)
    tg_dispatcher.add_handler(revalidate_users_groups_handler)

    # Other stuff

    tg_dispatcher.add_handler(MessageHandler(Filters.text |
                                             Filters.sticker |
                                             Filters.animation, no_command_handler))

    tg_dispatcher.add_handler(CallbackQueryHandler(handle_button_callback))


def start_telegram_client():
    global TG_CLIENT

    # loop = asyncio.new_event_loop()
    # TG_CLIENT = TelegramClient('sal34_bot_client',
    #                            CONFIGS['service']['identity']['telegram']['client_api_id'],
    #                            CONFIGS['service']['identity']['telegram']['client_api_hash'],
    #                            loop=loop)
    # TG_CLIENT.start()
    print('Telegram client started')


def serve_telegram_requests():
    global TG_UPDATER
    global TG_BOT

    TG_UPDATER = Updater(token=CONFIGS['service']['identity']['telegram']['bot_token'],
                         use_context=True)

    tg_dispatcher = TG_UPDATER.dispatcher
    TG_BOT = tg_dispatcher.bot

    setup_command_handlers(tg_dispatcher)

    TG_UPDATER.start_polling()


def signal_handler(sig, frame):
    print('Please wait until caches evicted...')
    USERS_CACHE.evict()
    # TODO: store actions queue
    print('Good bye!')
    sys.exit(0)


def main():
    signal.signal(signal.SIGINT, signal_handler)
    reload_configs()
    start_telegram_client()
    start_actions_queue()
    start_users_context_save()
    connect_google_service()
    start_tables_sync()
    start_caches_stale()
    serve_telegram_requests()


if __name__ == '__main__':
    main()
