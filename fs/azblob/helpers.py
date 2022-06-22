import datetime

from fs.enums import ResourceType
from fs.info import Info
from fs.time import datetime_to_epoch

from fs.azblob.const import BASIC, DETAILS, IS_DIR, TYPE


def _convert_to_epoch(props: dict) -> None:
    for k, v in props.items():
        if isinstance(v, datetime.datetime):
            props[k] = datetime_to_epoch(v)


def _info_from_dict(info: dict, namespaces) -> Info:
    if DETAILS in namespaces:
        if DETAILS not in info:
            info[DETAILS] = {}
        if info[BASIC][IS_DIR]:
            info[DETAILS][TYPE] = ResourceType.directory
        else:
            info[DETAILS][TYPE] = ResourceType.file
    return Info(info)
