import requests

from service.list_reader_service import load_identifier_list_of_type

if __name__ == '__main__':
    users = load_identifier_list_of_type('users')
    for user in users:
        url = 'https://api-eu.hosted.exlibrisgroup.com/almaws/v1/users/{}?accept=application/json&apikey=l8xx7fdf544bbefe415396184bc86d353025'.format(user)
        get = requests.get(url=url, headers={'Accept': 'application/json'})
        get.encoding = 'utf-8'
        if get.status_code == 200:
            info = get.text
            payload = info.replace('"record_type":{"value":"PUBLIC","desc":"Public"}', '"record_type":{"value":"STAFF","desc":"Staff"}')
            update = requests.put(url=url, data=payload.encode('utf-8'), headers={'Content-Type': 'application/json'})
            update.encoding = 'utf-8'
            if update.status_code == 200:
                print('succesfully updated user {}'.format(user))
            else:
                print('problem updating user {}:{}'.format(user, update.text))
