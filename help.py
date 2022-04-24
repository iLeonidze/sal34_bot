import os.path
import re
import os
from typing import Dict

import yaml
from telegram import Update
from telegram.ext import CallbackContext

help_file_path = './help.yaml'


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
                    entry['forward'] = row[3].strip().split('/')
                else:
                    entry['response'] = row[4].strip()

                self.db.append(entry)

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

    def proceed_request(self, update: Update, context: CallbackContext, user):
        query_text = update.message.text.lower().replace('бот,', '').strip()
        response = self.proceed_query_v2(query_text)

        if response is None:
            # TODO: send to proper admin chat for building
            return context.bot.send_message(
                chat_id=-1001198401765,
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
