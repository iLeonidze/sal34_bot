import os.path
import re
import os
from typing import Dict

import yaml
from telegram import Update
from telegram.ext import CallbackContext

help_file_path = './help.yaml'


class HelpAssistant:
    def __init__(self):

        self.db = []

        if not os.path.isfile(help_file_path):
            raise Exception('Help file "help.yaml" is not exists!')

        with open(help_file_path, 'r') as file:
            db_raw = yaml.load(file.read())

        if not db_raw or not isinstance(db_raw, dict) or not db_raw.get('db'):
            raise Exception('Help file is invalid!')

        for query in db_raw.get('db'):
            for i, substrings_raw in query.get('query'):
                query['query'][i] = substrings_raw.split('|')
            self.db.append(query)

    def proceed_request(self, update: Update, context: CallbackContext):
        query_text = update.message.text.lower().replace('бот,', '').strip()
        response = self.proceed_query(query_text)

        if not response:
            # bot failed to find something
            pass
        if response.get('message'):
            return context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=response['message']
            )
        elif response.get('forward'):
            return context.bot.forward_message(
                update.effective_chat.id,
                response['forward'][0],
                response['forward'][1]
            )

    def proceed_query(self, query_text: str) -> Dict or None:
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