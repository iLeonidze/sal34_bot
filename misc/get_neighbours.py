import math

import requests

LIMIT = 10
QUERY = 'https://ed.mos.ru/api/chat/users/contacts/person/?limit=%s&offset=%s&name=&sort=address&direction=ASC'
AUTHORIZATION = 'X'


def get_neighbours_page(offset=0):
    uri = QUERY % (LIMIT, offset)
    r = requests.get(uri,
                     verify=False,
                     headers={
                         'Cookie': 'Authorization=%s;' % AUTHORIZATION
                     })

    results = r.json()

    return results


def get_neighbours():
    print('Loading neighbours initial page')

    results = get_neighbours_page()

    contacts_amount = results.get('data', {}).get('allContacts')
    if not contacts_amount:
        print('Failed')
        return

    neighbours = []
    neighbours = neighbours + results['data']['contacts']

    iterations = math.ceil(contacts_amount / LIMIT)
    if iterations == 1:
        return neighbours

    i = 1
    while i < iterations:
        print('Loading iteration %s' % i)
        offset = i * LIMIT
        results = get_neighbours_page(offset)
        contacts_amount = results.get('data', {}).get('allContacts')
        if not contacts_amount:
            print('Failed')
            return
        neighbours = neighbours + results['data']['contacts']
        i += 1

    return neighbours


def main():
    r = get_neighbours()
    print(len(r))


if __name__ == '__main__':
    main()
