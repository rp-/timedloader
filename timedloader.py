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


def download(url: str, interval: int, basedestination: str, diffcheck: DiffCheckType):
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
    try:
        lasttag = None
        while True:
            md5hash = hashlib.md5()
            write_file = False
            starttime = time.time()
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
            opendtime = time.time()
            dur = opendtime - starttime
            time.sleep((interval - dur) / 1000)
    finally:
        h.close()


def main():
    parser = argparse.ArgumentParser(description='timed file downloader')
    parser.add_argument('-i', '--interval', type=int, default=1000, help='interval in ms')
    parser.add_argument('-d', '--destination', default='./', help='folder to put downloaded files')
    parser.add_argument('--diffcheck', choices=['etag', 'file'], default='etag')
    parser.add_argument('-v', '--verbose', action="store_true", help='use verbose logging')
    parser.add_argument('url')

    args = parser.parse_args()
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    download(args.url, args.interval, args.destination, DiffCheckType(args.diffcheck))


if __name__ == '__main__':
    main()
