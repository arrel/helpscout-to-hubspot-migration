import copy
import datetime
import json
import logging
import os
import time
import urllib

import requests
import requests_cache
from envparse import env

env.read_envfile()
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.INFO)

# avoid magic strings if possible and prepare for API changes
KEYS = {
    "AccessToken": "access_token",
    "User": "users",
    "Customer": "customers",
    "Conversation": "conversations",
    "Error": "error",
    "Errors": "errors",
    "ErrorDescription": "error_description",
    "Message": "message",
    "Mailbox": "mailboxes",
    "Embedded": "_embedded",
    "Lines": "lines",
    "Links": "_links",
    "Threads": "threads",
    "Photos": "photoUrl",
    "CreatedBy": "createdBy",
    "AssignedTo": "assignedTo",
    "Author": "author",
    "Assignee": "assignee",
    "Value": "value",
    "Type": "type",
    "Thread": "threads",
    "Body": "body",
    "Source": "source",
    "HREF": "href",
}
DEFAULT_NESTED_KEYS = ["address", "emails", "phones", "threads"]
THREAD_TYPES_TO_KEEP = ["message", "customer"]  # determine which type of nested data
MAX_RETRIES = 5

_attempts = 0
_token = "unknown"
_cache = False


def help():
    return "You need to get access keys from each API and create .env first"


def _get_page(url):
    global _cache
    global _attempts
    global _token
    token = _token  # env.str("HELPSCOUT_API_TOKEN")
    headers = {"Authorization": "Bearer {}".format(token)}
    logging.info("Fetching {} with token {} of type {}".format(url, token, type(token)))

    # setup cache
    if not _cache:
        requests_cache.install_cache("vg_helpscout_cache")
        _cache = True

    response = requests.get(url, headers=headers)

    # check if token expired and refresh, otherwise return
    if _has_expired_token(response) and _attempts <= MAX_RETRIES:
        logging.info("get token and try again")
        _attempts += 1
        _refresh_token()
        return _get_page(url)
    elif response.status_code == 404:
        _attempts = 0  # reset
        return {}
    else:
        _attempts = 0  # reset
        return response.json()


def _refresh_token():
    global _token
    logging.info("Fetching new access token")
    request_data = {
        "grant_type": "client_credentials",
        "client_id": env.str("HELPSCOUT_CLIENT_ID"),
        "client_secret": env.str("HELPSCOUT_CLIENT_SECRET"),
    }
    path = env.str("HELPSCOUT_API_URL")
    endpoint = "/oauth2/token"
    url = path + endpoint
    response = requests.post(url, data=request_data)
    token = response.json().get(KEYS["AccessToken"], "unknown")
    _token = token  # removed .decode('UTF-8')


def _has_expired_token(response):
    return response.status_code == 401


def _has_error(obj):
    return True if obj.get(KEYS["Error"], None) is not None else False


def _has_next_page(obj):
    if _has_error(obj):
        return False

    links = obj[KEYS["Links"]]
    return True if links.get("next", None) is not None else False


def _get_next_page(obj):
    links = obj[KEYS["Links"]]
    next_page_url = links.get("next")[KEYS["HREF"]]
    time.sleep(0.01)  # avoid exceeding 10 req/sec rate limit
    return _get_page(next_page_url)


def _get_initial_records(type, params=None):
    path = env.str("HELPSCOUT_API_URL")
    endpoint = "/{}".format(type)
    url = path + endpoint

    # add querystring params if exist (Dictionary)
    if params is not None:
        url += "?{}".format(urllib.parse.urlencode(params))  # from urllib import parse

    return _get_page(url)


def _add_nested_data(obj, keys=DEFAULT_NESTED_KEYS):
    links = obj.get(KEYS["Links"], None)
    if links is None:
        return obj

    new_obj = copy.deepcopy(obj)
    for key in keys:
        if key in links:
            response = _get_page(links[key][KEYS["HREF"]])
            if KEYS["Embedded"] in response:
                nested = response[KEYS["Embedded"]][key]
                if key == KEYS["Threads"]:
                    # filter out unwanted thread types and just grab desired fields
                    new_obj[key] = [
                        {
                            KEYS["Author"]: rec.get(KEYS["CreatedBy"], None),
                            KEYS["Assignee"]: rec.get(KEYS["AssignedTo"], None),
                            KEYS["Body"]: rec.get(KEYS["Body"], None),
                            KEYS["Source"]: rec.get(KEYS["Source"], None),
                        }
                        for rec in nested
                        if rec[KEYS["Type"]] in THREAD_TYPES_TO_KEEP
                    ]
                else:
                    new_obj[key] = [
                        {
                            KEYS["Type"]: rec.get(KEYS["Type"], None),
                            KEYS["Value"]: rec.get(KEYS["Value"], None),
                        }
                        for rec in nested
                    ]
            elif KEYS["Lines"] in response:
                new_obj[key] = response[KEYS["Lines"]]

    return new_obj


def _without_keys(obj, keys):
    new_obj = {}
    for k, v in obj.items():  # changed from obj.iteritems()
        if k not in keys:
            new_obj[k] = v

    return new_obj


def _clean_up_data(data, keys=[KEYS["Links"], KEYS["Photos"]]):
    logging.info("Removing [{}] from data file".format(keys))
    clean_data = [_without_keys(rec, keys) for rec in data]
    return clean_data


# records
def get_records(type, params=None):
    return _get_initial_records(type, params)


def get_all_records(type, params=None):
    logging.info("get_all_records")
    logging.info(type)
    response = get_records(type, params)
    records = []

    if _has_error(response):
        logging.error("Error: {}".format(response[KEYS["ErrorDescription"]]))
    else:
        # add initial page to list
        logging.debug(str(response))
        records.extend(response[KEYS["Embedded"]][type])

        # if multiple pages, loop, fetch and append additional records
        # while _has_next_page(response) and reps < 1:
        response = _get_next_page(response)
        records.extend(response[KEYS["Embedded"]][type])

    # get nested data within records via _links
    updated_records = [_add_nested_data(rec) for rec in records]

    logging.info("Returning {} {}".format(len(updated_records), type))
    return updated_records


def get_mailbox_ids():
    response = get_records(KEYS["Mailbox"])

    if response is None:
        return []

    mailboxes = response[KEYS["Embedded"]][KEYS["Mailbox"]]
    id_list = [box["id"] for box in mailboxes]
    return id_list


def dict_to_file(data, filename):
    clean_data = _clean_up_data(data)
    logging.info("Writing to file {}".format(filename))
    with open(filename, "w") as file:
        file.write(json.dumps(clean_data))


def main():
    logging.info(KEYS["Mailbox"])
    print(get_mailbox_ids())
    print("-" * 30)
    mailboxes = get_all_records(KEYS["Mailbox"])
    print(mailboxes)
    print("-" * 30)
    active_conversations = get_all_records(KEYS["Conversation"])
    print(active_conversations)
    logging.info("Finished")

    # logging.info(KEYS["User"])
    # timestamp = datetime.datetime.utcnow().replace(microsecond=0).isoformat()
    # records = get_all_records(KEYS["User"]) # no params for testing to figure out thread
    # dict_to_file(records, "{}-{}.json".format(KEYS["User"], timestamp))
    # logging.info("Finished")


if __name__ == "__main__":
    main()
