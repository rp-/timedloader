#!/usr/bin/python3
__author__ = 'rp'

import os
from base64 import b64encode
from urllib.parse import urlparse
import http.client
import time
import argparse
import logging
import hashlib
import enum
import datetime
from typing import Optional
from ephemerid import Ephemerid


logging.basicConfig()


class DiffCheckType(enum.Flag):
    ETAG = "etag"
    FILE = "file"


def parsenetloc(netloc):
    auth = None
    p = netloc.split('@')
    if len(p) > 1:
        loc = p[1]
        auth = p[0]
    else:
        loc = p[0]
    return loc, auth


def foldername():
    return time.strftime('%Y_%m_%d')


def formatfilename(path):
    return time.strftime('%Y_%m_%d_%H_%M_%S_') + os.path.basename(path)


def time2dayseconds(t: datetime.time):
    return t.hour * 3600 + t.minute * 60 + t.second


def download(
        url: str,
        interval: int,
        basedestination: str,
        diffcheck: DiffCheckType,
        ephemerid: Optional[Ephemerid] = None
):
    starttime = datetime.time(hour=0, minute=0, second=0)
    endtime = datetime.time(hour=23, minute=59, second=59)

    o = urlparse(url)
    loc, auth = parsenetloc(o.netloc)
    if o.scheme.startswith("https"):
        h = http.client.HTTPSConnection(loc)
    else:
        h = http.client.HTTPConnection(loc)
    logging.debug(loc)
    headers = {}
    if auth:
        userandpass = b64encode(auth.encode()).decode('ascii')
        headers = {'Authorization': 'Basic {encpass}'.format(encpass=userandpass)}

    lasttag = None
    while True:
        try:
            md5hash = hashlib.md5()
            write_file = False

            if ephemerid:
                starttime = ephemerid.sunrise()
                endtime = ephemerid.sunset()

            dl_start = time.time()
            if starttime < datetime.datetime.now().time() < endtime:
                destination = os.path.join(basedestination, foldername())
                os.makedirs(destination, exist_ok=True)
                dlfilename = os.path.join(destination, formatfilename(o.path))
                if not os.path.exists(dlfilename):
                    h.connect()
                    curtag = None
                    if diffcheck == DiffCheckType.ETAG:
                        h.request('HEAD', o.path, headers=headers)
                        resp = h.getresponse()
                        resp.read()
                        curtag = resp.headers.get('etag', None)
                        logging.debug("ETAG: " + str(curtag))

                    if lasttag != curtag:
                        if diffcheck == DiffCheckType.ETAG:
                            write_file = True
                        else:
                            logging.debug("Same etag as last request.")

                        h.request('GET', o.path, headers=headers)
                        res = h.getresponse()
                        if res.code <= 400:
                            data = res.read()

                            if diffcheck == DiffCheckType.FILE:
                                md5hash.update(data)
                                curtag = md5hash.digest()
                                write_file = lasttag != curtag

                            if write_file:
                                lasttag = curtag
                                with open(dlfilename, 'wb+') as ofile:
                                    bsize = ofile.write(data)
                                logging.info("Wrote {f} with size {s}".format(f=dlfilename, s=bsize))
                            else:
                                logging.debug("File is the same as last request.")
                        else:
                            logging.error(res.read())
                    h.close()
                now = time.time()
                dur = now - dl_start
                time.sleep((interval - dur) / 1000)
            else:
                now = datetime.datetime.now()
                sleep_time = 10
                if now.time() > starttime:
                    sleep_time = 86400 - time2dayseconds(now.time())
                    logging.debug("sleep til midnight " + str(sleep_time))
                else:
                    sleep_time = time2dayseconds(starttime) - time2dayseconds(now.time())
                    logging.debug("Downloading between {s} and {e}".format(s=starttime, e=endtime))
                    logging.debug("sleeping for {s} seconds".format(s=sleep_time))
                time.sleep(sleep_time)
        except OSError as oe:
            print(oe)
            time.sleep(sleep_time)
        finally:
            h.close()


def dt_from_time(t: datetime.time) -> datetime.datetime:
    def_dt = datetime.datetime.now()
    return datetime.datetime(
        year=def_dt.year,
        month=def_dt.month,
        day=def_dt.day,
        hour=t.hour,
        minute=t.minute,
        second=t.second
    )


def main():
    parser = argparse.ArgumentParser(description='timed file downloader')
    parser.add_argument('-i', '--interval', type=int, default=1000, help='interval in ms')
    parser.add_argument('-d', '--destination', default='./', help='folder to put downloaded files')
    parser.add_argument('--diffcheck', choices=['etag', 'file'], default='etag')
    parser.add_argument('--daytime', action="store_true")
    parser.add_argument('--pos', default='48.21,16.36')
    parser.add_argument('-v', '--verbose', action="store_true", help='use verbose logging')
    parser.add_argument('url')

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    ephemerid: Optional[Ephemerid] = None
    if args.daytime:
        lat, long = args.pos.split(',')
        ephemerid = Ephemerid(lat=float(lat), long=float(long))

    download(
        args.url,
        args.interval,
        args.destination,
        DiffCheckType(args.diffcheck),
        ephemerid=ephemerid
    )


if __name__ == '__main__':
    main()
