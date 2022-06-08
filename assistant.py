import os.path
import re
import os
from typing import Dict, Callable

import yaml
from telegram import Update
from telegram.ext import CallbackContext
from telegram.ext.commandhandler import CommandHandler

help_file_path = './help.yaml'


def is_bot_assistant_request(update: Update) -> bool:
    return update.message.text[0] != '/' and \
           (update.message.text.lower()[:4] == 'бот,' or
            update.message.chat.type == 'private' or
            is_activation_phrase(update))


def is_activation_phrase(update: Update) -> bool:
    # Интернет
    # подскажите как подключить интернет в квартире
    # Соседи, подскажите, чтобы подключить интернет Ловит, нужно самим купить роутер?
    # Соседи,подскажите,пожалуйста,провайдера,хочу подключить интернет,и какой посоветуете роутер?
    # Скажите, интернет себе кто-нибудь уже подключил?
    # Добрый вечер . А в нашем доме уже  есть возможность подключения интернета ?
    # А куда подключать, в дом проведен интернет?
    # Леонид,подключили вам интернет?вы какую скорость подключили?как работает?и что по цене?
    #
    # Стиральная машина
    #
    #
    # Кондиционер
    # скажите,короб под кондей у нас где над или под квартирой? забыла
    # Я чёт туплю может, а выходы под кондей в каждой комнате должны быть?
    # Добрый день. А есть памятка по установке у нас кондиционера?
    # Подскажите, пожалуйста. Распашонка 2шка. Забыл простучать, где трассы для кондиционера. Визуально виден только один выход в спальне, где висит белая штука продолговатая. А в другой комнате и на кухне где-то замурованы для них трассы? Над дверными проемами?
    # Соседи, подскажите, приемщик не нашел выводы под кондиционер, вписал как замечание. Сказал, что может они спрятаны, а может нет  а проверить их не может. А что, выводов может не быть в принципе?
    # Добрый день, соседи!  На 9 этаже не вижу в квартире выведенную трассу под кондиционеры. Снаружи под окном всё есть, куда ведёт не понятно, может на 8-й этаж? А где искать на 9-м?
    # Подскажите по кондиционерам установке. Нужно с соседями сверху договариваться для установки внешнего блока? Или можно как из своей квартиры установить?
    # Соседи, добрый вечер! На 1м этаже кондиционер кто нибудь делал? Где находятся выводы под внутренний блок?
    # а кто кондиционер уже ставил, как на последний этаж его ставить? в крепеж, который подо мной или куда?
    # Соседи, добрый день! Кто-то ставил уже кондиционер? Или планирует мжт кто? Есть контакты установщиков вменяемых? Которые в курсе про нюансы ПИКа
    # Кто нибудь в курсе.. Почему не выведена трасса для кондиционеров из стены ...и где она есть вообще ?
    # Добрый день , соседи . Кондиционер уже кто -то устанавливает ? У нас корзины подключения под своим окном или у соседей сверху ? Заранее спасибо
    # Вопрос) трасса кондиционера только в одной комнате) а двух других нету) у всех так?
    #
    # Входная дверь (личинка и цвет двери)
    # У нас размер личинки 35/45 или 45/35?
    # Соседи, где брали личинку? Есть ссылка ?
    # Добрый день, не подскажите, какой размер личинки, для входной двери?
    # Добрый день! Кто-нибудь менял личинку в замке? Я купила по параметрам как в чате советовали)
    # А ещё где нибудь находили личинки к нашим дверям? На озоне через неделю доставка только
    # Отпишитесь пожалуйста, кто будет менять, соответствует ли личинка в наших дверях этим размерам
    # Соседи, вопрос про личинку замка. Какой у нее типоразмер?
    # Соседи,всем доброго дня Хотелось бы услышать мнение по поводу входных дверей,а точнее про их цвет Что скажете? Только белые ?
    #
    # Ключи
    # Доброе утро. Пытаюсь разобраться с магнитными ключами. Не могу понять: каждый из них открывает определённую дверь? Или одним ключом можно открыть двери от подъезда, двора, входа на паркинг и кладовки?
    # Добрый день! Подскажите пожалуйста, кто уже получил ключи от паркинга. Брелки у вас корректно работает? И есть ещё дополнительный магнитный брелок. Он только от двери, которая выходит от лифта на паркинг? А то вчера у меня брелок не работал (
    #
    # Другое
    # коллеги, подскажите, а куда можно жаловаться на наш УК?
    # Кто где находил антену кроме как у входной двери?
    #
    # Do not react
    # Может кому то нужна личина для пиковских дверей, у менч лишняя упаковка
    return False


def get_command_handler_by_name(context: CallbackContext, command_name) -> Callable or None:
    for handler in context.dispatcher.handlers[0]:
        if isinstance(handler, CommandHandler) and handler.command[0] == command_name:
            return handler.callback
    return None


class HelpAssistant:
    def __init__(self, rows):

        self.db = []
        self.load_from_table(rows)

    def load_from_table(self, rows):
        # Columns:
        # 0. Name
        # 1. Status
        # 2. Request Body
        # 3. Forward
        # 4. Response Body
        # 5. Response Test Queries
        for row in rows:
            if len(row) == 6 and row[1].lower() == 'готов' and row[2] != '' and (row[3] != '' or row[4] != ''):
                entry = {
                    'query': re.compile(row[2].strip())
                }
                if row[3] != '':
                    action = row[3].strip().split(' ')
                    if action[0] == 'forward':
                        entry['forward'] = [action[1],action[2]]
                    elif action[0] == 'command':
                        entry['command'] = action[1]
                else:
                    entry['response'] = row[4].strip()

                entry['name'] = row[0].strip()
                entry['test_queries'] = row[5].strip().split('\n')

                self.db.append(entry)
                print(f"Loaded \"{row[0]}\" entry")

        print(f'Loaded {len(self.db)} assistant entries')

    def load_from_file_v1(self):
        if not os.path.isfile(help_file_path):
            raise Exception('Help file "help.yaml" is not exists!')

        with open(help_file_path, 'r') as file:
            db_raw = yaml.load(file.read())

        if not db_raw or not isinstance(db_raw, dict) or not db_raw.get('db'):
            raise Exception('Help file is invalid!')

        for query in db_raw.get('db'):
            for i, substrings_raw in enumerate(query.get('query')):
                query['query'][i] = substrings_raw.split('|')
            self.db.append(query)

    def proceed_request(self, update: Update, context: CallbackContext, user, building_chats):
        query_text = update.message.text.lower().replace('бот,', '').strip()
        response = self.proceed_query_v2(query_text)

        if response is None:
            admin_chat = None
            for chat in building_chats:
                if chat['name'] == 'admin':
                    admin_chat = chat['id']
                    break
            if admin_chat is not None:
                return context.bot.send_message(
                    chat_id=admin_chat,
                    parse_mode='MarkdownV2',
                    text=f"Неизвестный запрос ассистенту от {user.get_linked_fullname()}:\n`{query_text}`"
                )
        elif response.get('response'):
            return context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=response['response'],
                reply_to_message_id=update.message.message_id
            )
        elif response.get('forward'):
            return context.bot.forward_message(
                update.effective_chat.id,
                response['forward'][0],
                response['forward'][1]
            )
        elif response.get('command'):
            return get_command_handler_by_name(context, response['command'])(update, context)

    def proceed_query_v1(self, query_text: str) -> Dict or None:
        response = None

        for query in self.db:
            sub_strings_failed = False
            for sub_strings in query['query']:
                any_substring_found = False

                for substring in sub_strings:
                    if substring in query_text:
                        any_substring_found = True
                        break

                if not any_substring_found:
                    sub_strings_failed = True
                    break

            if not sub_strings_failed:
                return query

        return response

    def proceed_query_v2(self, query_text: str) -> Dict or None:
        response = None

        for entry in self.db:
            match = entry['query'].search(query_text)
            if match is not None:
                return entry

        return response
