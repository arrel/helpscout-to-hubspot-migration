# -*- coding: utf-8 -*-

import copy
import csv
import datetime
import json
import logging
import os
import sys
import time
import traceback

from envparse import env

env.read_envfile()
logging.basicConfig(format="%(levelname)s:%(message)s", level=logging.DEBUG)

KEYS = {
    "Title": "title",
    "Source": "source",
    "Dest": "dest",
    "Excludes": "excludes",
    "List": "list",
    "Parent": "_parent",
    "FieldMailbox": "mailboxId",
}
DOT_DELIMITER = "."

MAILBOX_TO_PIPELINE = {"123": "Test Mailbox"}


def _get_dot_val(obj, string_field, ctx=None):
    value = None

    try:
        parts = string_field.split(DOT_DELIMITER)

        for part in parts[:-1]:
            if part == KEYS["Parent"]:
                logging.debug("Parent field detected so switching to context source")
                obj = ctx if ctx is not None else {}
            else:
                obj = obj.get(part, None)

        if obj is not None and isinstance(obj, dict):
            raw_value = obj.get(parts[-1])
            value = (
                MAILBOX_TO_PIPELINE[str(raw_value)]
                if string_field == KEYS["FieldMailbox"]
                else raw_value
            )
            logging.debug("Got value of type {}".format(type(value).__name__))
    except Exception:
        logging.error("Error getting field {}".format(string_field))

    return value


def _get_header_fields_from_mapping(mapping):
    fields = []

    if len(mapping) <= 0:
        logging.warn("Mapping has no fields")
    else:
        fields = [field[KEYS["Title"]] for field in mapping]

    logging.debug("Returning header fields {}".format(fields))
    return fields


def _get_transformed_obj(obj, mapping, ctx=None):
    new_obj = {}
    new_list = []

    for field in mapping:
        if _is_nested_mapping(field):
            logging.debug("Handling nested field {}".format(json.dumps(field)))
            for item in ctx[field[KEYS["Source"]]]:
                temp_obj = {}
                logging.debug("Item in items {}".format(json.dumps(item)))
                for sub_field in field[KEYS["Dest"]]:
                    logging.debug("Handling sub_field {}".format(json.dumps(sub_field)))
                    temp_obj[sub_field[KEYS["Dest"]]] = _get_dot_val(
                        item, sub_field[KEYS["Source"]], ctx
                    )

                new_list.append(temp_obj)
        else:
            new_obj[field[KEYS["Dest"]]] = _get_dot_val(obj, field[KEYS["Source"]])

    logging.debug("Returning transformed obj {}".format(json.dumps(new_obj)))
    return new_list if len(new_list) > 0 else new_obj


def _is_nested_mapping(field):
    return type(field[KEYS["Dest"]]).__name__ == KEYS["List"]


def _is_excluded(obj, mapping):
    exclude = False

    for field in mapping:
        exclude_list = field.get(KEYS["Excludes"], None)
        if exclude_list is not None and len(exclude_list) > 0:
            test_val = str(_get_dot_val(obj, field[KEYS["Source"]]))
            for to_exclude in exclude_list:
                if to_exclude in test_val:
                    exclude = True

    return exclude


def flatten(obj):
    new_obj = {}

    if isinstance(obj, dict):
        for k, v in obj.items():
            if isinstance(v, list):
                new_obj[k] = {str(idx): flatten(val) for idx, val in enumerate(v)}
            elif isinstance(v, dict):
                new_obj[k] = flatten(v)
            else:
                new_obj[k] = v
    else:
        logging.debug("*** Object was not a dict ***".format(obj))
        new_obj = obj

    logging.debug("Returning new obj {}".format(json.dumps(new_obj)))
    return new_obj


def transform(data, mapping):
    new_data = []

    for row in data:
        flattened = flatten(row)
        if (_is_excluded(flattened, mapping)) is False:
            transformed = _get_transformed_obj(flattened, mapping, row)
            logging.debug("transformed type is {}".format(type(transformed).__name__))
            if type(transformed).__name__ == KEYS["List"]:
                new_data.extend(transformed)
            else:
                new_data.append(transformed)

    logging.debug("Returning transformed data {}".format(json.dumps(new_data)))
    return new_data


def json_to_dict(filename):
    try:
        with open(filename) as file:
            return json.loads(file.read())
    except IOError as ioe:
        logging.error(ioe)
        return None


def list_to_csv(data, mapping, filename):
    error_count = 0

    if len(mapping) > 0:
        first_field = mapping[0]
        if _is_nested_mapping(first_field):
            mapping = first_field[KEYS["Dest"]]

    with open(filename, "w+", newline="") as output_file:
        out = csv.writer(output_file, quoting=csv.QUOTE_MINIMAL)
        out.writerow(_get_header_fields_from_mapping(mapping))
        for row in data:
            try:
                out.writerow([str(row[val[KEYS["Dest"]]]) for val in mapping])
            except Exception:
                traceback.print_exc()
                logging.info("--- Skipped row ---")
                error_count += 1

        logging.info("Generated CSV file {} with {} rows".format(filename, len(data)))
        logging.info("Caught {} errors".format(error_count))


def main():
    print("View README or example.py for usage examples")


if __name__ == "__main__":
    main()
