import datetime
import encodings
from dataclasses import dataclass
import re
import ipaddress
import mysql.connector
from datetime import timedelta
import base58

err1 = open("./../Measurements/new/Modified-agent 5/errcleaned.txt", encoding="iso8859_1")

errconcated1 = open("./../Measurements/new/Modified-agent 5/errconcat.txt", "w+", encoding="iso8859_1")
for l in err1.readlines():
    if l.startswith("2021"):
        errconcated1.write("\n")
        errconcated1.write(l.removesuffix("\n"))
    else:
        errconcated1.write(l.removesuffix("\n"))