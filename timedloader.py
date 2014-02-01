__author__ = 'rp'

import os
from base64 import b64encode
from urllib.parse import urlparse
import http.client
import time
import argparse


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
    return time.strftime('%Y_%m_%d/%H')


def formatfilename(path):
    return time.strftime('%Y_%m_%d_%H_%M_%S_') + os.path.basename(path)


def download(url, interval, basedestination):
    o = urlparse(url)
    loc, auth = parsenetloc(o.netloc)
    h = http.client.HTTPConnection(loc)
    headers = {}
    if auth:
        userandpass = b64encode(auth.encode()).decode('ascii')
        headers = {'Authorization': 'Basic {encpass}'.format(encpass=userandpass)}
    try:
        while True:
            starttime = time.time()
            destination = os.path.join(basedestination, foldername())
            os.makedirs(destination, exist_ok=True)
            dlfilename = os.path.join(destination, formatfilename(o.path))
            if not os.path.exists(dlfilename):
                h.request('GET', o.path, headers=headers)
                res = h.getresponse()
                with open(dlfilename, 'wb+') as ofile:
                    ofile.write(res.read())
            opendtime = time.time()
            dur = opendtime - starttime
            time.sleep((interval - dur) / 1000)
    finally:
        h.close()


def main():
    parser = argparse.ArgumentParser(description='timed file downloader')
    parser.add_argument('-i', '--interval', default=1000, help='interval in ms')
    parser.add_argument('-d', '--destination', default='./', help='folder to put downloaded files')
    parser.add_argument('url')

    args = parser.parse_args()
    download(args.url, args.interval, args.destination)

if __name__ == '__main__':
    main()