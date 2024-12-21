import json
import os
from copy import deepcopy

from yaml import safe_load as yaml_safe_load
from yaml import safe_dump as yaml_safe_dump


def main():
    old_users_path = '../users/'
    new_users_path = '../users_new/'
    merged_users_path = '../users_merged/'
    for file in sorted(os.listdir(new_users_path)):
        new_user_file_path = new_users_path + file
        old_user_file_path = old_users_path + file
        merged_user_file_path = merged_users_path + file

        with open(new_user_file_path, "r") as f:
            new_user = json.load(f)

        with open(old_user_file_path, "r") as f:
            old_user = json.load(f)

        merged_user = deepcopy(new_user)

        if not new_user['private_chat']['bot_started'] and old_user['private_chat']['bot_started']:
            merged_user['private_chat']['bot_started'] = old_user['private_chat']['bot_started']
            print('Updated bot_started for %s' % file)

        for key, value in old_user['joined_chats'].items():
            if not new_user.get(key):
                merged_user[key] = value
                print('Updated joined_chats for %s' % file)

        if old_user['stats']['sended_private_messages_total'] > new_user['stats']['sended_private_messages_total']:
            merged_user['stats']['sended_private_messages_total'] = old_user['stats']['sended_private_messages_total']
            print('Updated sended_private_messages_total for %s' % file)

        if old_user['stats']['sended_public_messages_total'] > new_user['stats']['sended_public_messages_total']:
            merged_user['stats']['sended_public_messages_total'] = old_user['stats']['sended_public_messages_total']
            print('Updated sended_public_messages_total for %s' % file)

        if old_user['stats']['total_garbage_detected_for_user'] > new_user['stats']['total_garbage_detected_for_user']:
            merged_user['stats']['total_garbage_detected_for_user'] = old_user['stats']['total_garbage_detected_for_user']
            print('Updated total_garbage_detected_for_user for %s' % file)

        for key, value in old_user['stats']['sended_public_messages_per_chat'].items():
            if not  new_user['stats']['sended_public_messages_per_chat'].get(key) or  value > new_user['stats']['sended_public_messages_per_chat'][key]:
                merged_user['stats']['sended_public_messages_per_chat'][key] = value
            print('Updated sended_public_messages_per_chat for %s' % file)

        with open(merged_user_file_path, 'w') as f:
            json.dump(merged_user, f, ensure_ascii=False)


if __name__ == '__main__':
    main()
