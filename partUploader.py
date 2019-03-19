#!/usr/bin/env python
###############################################################################
#
# Multipart uploader for ArcGIS Online via Portal REST API
#
###############################################################################
from __future__ import print_function
import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import traceback
import json
import time
import argparse
import datetime
import Queue
import multiprocessing
import threading
import requests
import os
import mmap
import io
import signal

################################################################################
# 
# Globals
#
referer = "http://www.arcgis.com/sharing"
gInterrupt = False
MAX_PARTS = 9000
PAGE_SIZE = 8192
MIN_FILE_SIZE = 5 * 1024 * 1024
MAX_FILE_SIZE = 800 * 1024 * 1024 * 1024
DEFAULT_PART_SIZE = 8 * 1024 * 1024
MAX_PART_SIZE = 150 * 1024 * 1024

################################################################################
#
# Get Config
#
def readConfig(cfgfile):
    json_data = open(cfgfile)
    config = json.load(json_data)
    json_data.close()
    return config
   
############################################################################3
def sizeof_fmt(num, suffix='B'):
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)

############################################################################3
def isEmpty(queue):
    try:
        queue.get_nowait()
    except Queue.Empty:
        return True
    return False
    
############################################################################3
def cancelThreads(num, queue):
    for _ in range(num):
        queue.put({"cancel":True})
        
################################################################################
def login(config):
    props = { 'username': config['username'], 'password': config['password'], 'referer': referer, 'f': 'json', 'expiration': 1440 * 14}
    try:
        r = requests.post(config['baseurl'] + "/sharing/generateToken", props)
        jsonr = r.json()
        verboseprint(str(jsonr))
        return jsonr['token']
    except:
        e = sys.exc_info()[0]
        print (e)
        print("Error reading response")
    return None

############################################################################3
def startMultipart(config, filename, user, token, overwrite):

    if overwrite: 
        overwriteStr = 'true' 
    else: 
        overwriteStr = 'false'
        
    props = { 'token':  token, 'f': 'json', 'multipart': 'true', 'fileName': filename, 'overwrite': overwriteStr }
    try:
        r = requests.post(config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/addItem", data=props, headers={'Referer': referer})
        jsonr = r.json()
        verboseprint(str(jsonr))
        if ('error' in jsonr):
            print("Error starting multipart: " + jsonr['error']['message'])
        else:
            return jsonr['id']
    except:
        e = sys.exc_info()
        print (e)
        print("Error reading response")
    return None

############################################################################3
def commitMultipart(config, itemid, user, token, itemtype, itemtitle):

    props = { 'token':  token, 'f': 'json' }
    if itemtype:
        props['type'] = itemtype
    if itemtitle:
        props['title'] = itemtitle
    try:
        r = requests.post(config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/items/" + itemid + "/commit", data=props, headers={'Referer': referer})
        jsonr = r.json()
        verboseprint(str(jsonr))
    except:
        e = sys.exc_info()
        print (e)
        print("Error reading response")
    
############################################################################3
def cancelMultipart(config, itemid, user, token):

    props = { 'token':  token, 'f': 'json' }
    try:
        verboseprint("Calling cancel on itemid: " + itemid)
        r = requests.post(config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/items/" + itemid + "/cancel", data=props, headers={'Referer': referer})
        jsonr = r.json()
        verboseprint(str(jsonr))
    except:
        e = sys.exc_info()
        print (e)
        print("Error reading response")
 
############################################################################3
def status(config, itemid, user, token):

    props = { 'token':  token, 'f': 'json' }
    try:
        r = requests.post(config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/items/" + itemid + "/status", data=props, headers={'Referer': referer})
        jsonr = r.json()
        verboseprint(str(jsonr))
        return jsonr['status']
    except:
        e = sys.exc_info()
        print (e)
        print("Error reading response")

############################################################################3
def addPart(config, filestream, itemid, partNum, partLen, user, token, nostream):
    
    files = {
        'file': ('file', filestream, 'application/octet-stream', {'Content-Length': partLen})
    }
    
    url = config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/items/" + itemid + "/addPart?partNum=" + str(partNum) + "&size=" + str(partLen) + "&streamdata=true&f=json&token=" + token
    if nostream:
        url = config['baseurl'] + "/sharing/rest/content/users/" + str(user) + "/items/" + itemid + "/addPart?partNum=" + str(partNum) + "&f=json&token=" + token
            
    try: 
        r = requests.post(url, files=files, headers={'Referer': referer})
        jsonr = r.json()
        verboseprint(str(jsonr))
        if ('success' in jsonr and jsonr['success'] == True):
            return True
        else:
            print("Error uploading part. Response: " + str(jsonr))
    except:
        traceback.print_exc(file=sys.stdout)
        print("Error reading response")
    return False
            
############################################################################3
def uploader_thread(config, filename, itemid, user, token, iq, oq, intq, verbose, nostream):
    
    # Define verbose print again in the thread for Windows
    if args.verbose:
        def verboseprint(*args):
            o = str(datetime.datetime.today()) + " - "
            for arg in args:
                o += arg
            print(o)
    else:   
        verboseprint = lambda *a: None

    done = False
    while isEmpty(intq):
        partinfo = iq.get();
        if partinfo is None:
            done = True
            break

        verboseprint("Adding part " + str(partinfo))
        t1 = time.time()
        
        r = False
        retry = 10
        while r == False and retry > 0:
            b = None
            with open(filename, "rb") as f:
                mm = mmap.mmap(f.fileno(), partinfo['len'], access=mmap.ACCESS_READ, offset=partinfo['start'])
                buf = mm.read(partinfo['len'])
                if len(buf) != partinfo['len']:
                    print("Bytes read not the same as bytes required!")
                b = io.BytesIO(buf)
                mm.close()
#                f.seek(partinfo['start'])
#                b = io.BytesIO(f.read(partinfo['len']))
            r = addPart(config, b, itemid, partinfo['partNum'], partinfo['len'], user, token, nostream)
            if (r == False and retry > 1):
                print("Retrying part upload...")
            retry -= 1
        
        dur = time.time() - t1
        if (r == True):
            oq.put({"partNum": partinfo['partNum'], "success": True, "bytes": partinfo['len'], "duration": dur}) 
        else:
            oq.put({"partNum": partinfo['partNum'], "success": False})
    if not done:
        verboseprint("Canceling uploader thread.")
    oq.put(None)
    
############################################################################3
def uploadfile(config, filename, multiprocess, threads, overwrite, itemtype, itemtitle, itemfilename, verbose, nostream, partsize):

    global gInterrupt
    pool = []   
    iq = None
    oq = None
    intq = None
    
    if partsize == None:
        partsize = DEFAULT_PART_SIZE
        
    filesize = os.stat(filename).st_size

    if (filesize > MAX_FILE_SIZE):
        print("Error: File too big")
        return
    
    if (filesize < MIN_FILE_SIZE):
        print("Error: File too small")
        return

    if (partsize > MAX_PART_SIZE):
        print("Error: Partsize too large")
        return
            
    parts = int(filesize/partsize) + 1
    if (parts > MAX_PARTS):
        partsize = (filesize/MAX_PARTS % PAGE_SIZE) * PAGE_SIZE
        parts = int(filesize/partsize) + 1
        
    print("Splitting file of size " + sizeof_fmt(filesize) + " into " + str(parts) + " parts with part size of " + sizeof_fmt(partsize))
    
    user = config['username']
    token = login(config)
    if (token is None):
        print("Error: Unable to log in")
        return

    startTime = time.time()
    
    if not itemfilename:
        itemfilename = filename
        
    itemid = startMultipart(config, itemfilename, user, token, overwrite)
    if (itemid is None):
        print("Error: Unable to start multipart")
        return
    
    if (not multiprocess):
        iq = Queue.Queue()
        oq = Queue.Queue()
        intq = Queue.Queue()
        for _ in range(threads):
            worker = threading.Thread(
                target = uploader_thread,
                args = (config, filename, itemid, user, token, iq, oq, intq, verbose, nostream)
            )
            pool.append(worker)
            worker.start()
    else:
        iq = multiprocessing.Queue()
        oq = multiprocessing.Queue()
        intq = multiprocessing.Queue()
        for _ in range(threads):
            worker = multiprocessing.Process(
                target = uploader_thread,
                args = (config, filename, itemid, user, token, iq, oq, intq, verbose, nostream)
            )
            pool.append(worker)
            worker.start()
 
    for part in range(parts):
        start = part * partsize
        partlen = partsize
        if (part == parts - 1):
            partlen = filesize - part * partsize
        iq.put({'start': start, 'len': partlen, 'partNum': part + 1})
    
    # Tell all threads to stop once we have no more parts
    for _ in range(len(pool)):
        iq.put(None)
       
    bytesdone = 0
    endcount = 0
    cancelled = False
    while endcount < len(pool):
        partinfo = oq.get() 
        if (partinfo == None):
            endcount += 1
        else:
            if (partinfo['success'] == False):
                print("Error uploading part " + str(partinfo['partNum']) + ". Canceling")
                cancelThreads(len(pool), intq)
                cancelled = True
            else:
                bytesdone += partinfo['bytes']
                sys.stdout.write("Transferred " + sizeof_fmt(bytesdone) + "/" + sizeof_fmt(filesize) + "               \r")
                sys.stdout.flush()
        if (gInterrupt):
            cancelThreads(len(pool), intq)
            cancelled = True
        
    for thread in pool:
        thread.join()

    if not cancelled:
        commitMultipart(config, itemid, user, token, itemtype, itemtitle)
        t = time.time() - startTime
        print('\nTransferred ' + sizeof_fmt(filesize) + ' bytes in ' + str(t) + ' seconds. Rate: ' + sizeof_fmt(filesize/t) + '/s')
        print('\nWaiting for server to process upload. This might take a while.')
        while 1:
            sys.stdout.write('.')
            sys.stdout.flush()
            st = status(config, itemid, user, token)
            if (st == 'error' or st == 'failed'):
                print("\nError processing upload.")
                break
            elif (st == 'completed'):
                print('\nProcessing completed. Item id: ' + itemid)
                break
            time.sleep(10)
    else:
        cancelMultipart(config, itemid, user, token)
        print("\nUpload Cancelled.")
    

###################################################################################
#
# Try to stop the threads that are running.
#
def shutdown_handler(signum, frame):
    global gInterrupt
    print("Got kill signal. Cleaning up and canceling upload.")
    gInterrupt = True 
    
################################################################################
if __name__ == '__main__':

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)
    
    parser = argparse.ArgumentParser(description='AGO Uploader Utility')
    parser.add_argument('--cfg', '-c', default='./agouploader.cfg', help='config file to use')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='verbose output. can be specified multiple times.')
    parser.add_argument('--multiprocess', '-mp', action='store_true', help='use multiple processes')
    parser.add_argument('--threads', '-th', default=10, type=int, action='store', help='number of threads to run')
    parser.add_argument('--overwrite', '-o', action='store_true', help='overwrite existing file')
    parser.add_argument('--type', '-ty', action='store', help='item type')
    parser.add_argument('--title', '-tt', action='store', help='item title')
    parser.add_argument('--filename', '-fn', action='store', help='filename on target item')
    parser.add_argument('--partsize', '-ps', default=None, type=int, action='store', help='custom partsize for upload')
    parser.add_argument('--ignoreurllib3warnings', '-k', action='store_true', help='ignore warnings from urllib3')
    parser.add_argument('--nostream', '-ns', action='store_true', help='no streaming. use old add part API way.')
    parser.add_argument('file', action='store', help='input file to upload')
    args = parser.parse_args()
 
    # Simple verbose method used throughout the code
    if args.verbose:
        def verboseprint(*args):
            o = str(datetime.datetime.today()) + " - "
            for arg in args:
                o += arg
            print(o)
    else:   
        verboseprint = lambda *a: None
        
    if args.ignoreurllib3warnings:
        requests.packages.urllib3.disable_warnings()
        
    #
    # Read config params
    #
    config = readConfig(args.cfg)
    
    if (args.threads > 50):
        print('Error. Too many threads.')
        exit(1)
    
    print(str(datetime.datetime.today()) + " - Start")
    
    uploadfile(config, args.file, args.multiprocess, args.threads, args.overwrite, args.type, args.title, args.filename, args.verbose, args.nostream, args.partsize)   
        
    print(str(datetime.datetime.today()) + " - Done")
