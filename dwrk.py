#!/usr/bin/env python2
# -*- Mode: python -*-

import os
import sys
import shutil
import subprocess
import threading
import urllib
import tarfile

CWD = os.path.dirname(os.path.abspath(sys.argv[0]))

HOST = 'hosts'

WRK_URL = 'https://github.com/innomentats/wrk/archive/master.tar.gz'
WRK_TAR = 'master.tar.gz'
WRK_DIR = 'wrk-master'
WRK_BIN = os.path.join(CWD, 'wrk')
WRK_BIN_RMT = '/tmp/wrk'

class Manager:

    def __init__(self, runners):
        self.runners = runners
        self.runners_uniq = list(set(runners))

    def run(self):
        self.poll([runner.init() for runner in self.runners_uniq])
        self.poll([runner.work() for runner in self.runners])
        self.poll([runner.exit() for runner in self.runners_uniq])

    def poll(self, cs):
        ps, ts = list(), list()
        for c in cs:
            ps.append(subprocess.Popen(c, stdout=subprocess.PIPE, stderr=subprocess.PIPE, \
                    stdin=subprocess.PIPE))
        for p in ps:
            f = lambda p: p.communicate()
            t = threading.Thread(target=f, args=(p,))
            t.start()
            ts.append(t)
        for t in ts:
            t.join()

class Runner:

    def __init__(self, host, cmd, opt):
        self.host = host
        self.cmd = cmd
        self.opt = opt

    def init(self):
        return ['scp', WRK_BIN, '{}:{}'.format(self.host, WRK_BIN_RMT)]

    def exit(self):
        return ['ssh', self.host, 'rm', WRK_BIN_RMT]

    def work(self):
        return ['ssh', '-t', '-t', self.host, '/bin/bash', '-O', 'huponexit', '-c', \
                '\"%s\"' % ' '.join([WRK_BIN_RMT] + self.opt)]

    def __eq__(self, other):
        return self.host == other.host

    def __hash__(self):
        return hash(self.host)

def build_binary(path='/tmp'):
    os.chdir(path)

    # download
    print 'Downloading WRK...'
    urllib.urlretrieve(WRK_URL, WRK_TAR)
    tar = tarfile.open(WRK_TAR, 'r:gz')
    tar.extractall()

    # build
    print 'Building WRK...'
    os.chdir(WRK_DIR)
    subprocess.check_call(['make'])

    # copy file
    os.chdir(CWD)
    shutil.copy2(os.path.join(path, WRK_DIR, 'wrk'), WRK_BIN)

    # clear
    os.remove(os.path.join(path, WRK_TAR))
    shutil.rmtree(os.path.join(path, WRK_DIR), ignore_errors=True)

def read_hosts():
    with open(HOST) as f:
        hosts = f.read()
        return filter(lambda x: x != '' and not x.startswith('#'), \
                [line.strip() for line in hosts.splitlines()])

def verify_hosts(hosts):
    print 'Verifying hosts...'
    for host in set(hosts):
        subprocess.check_call(['ssh', '-t', '-t', host, '-oStrictHostKeyChecking=no', 'exit', '0'])
        print 'Host %s is OK' % host

def main():
    print 'Verifying ssh...'
    subprocess.check_call(['ssh', '-V'])

    if not os.path.exists(HOST):
        print 'No host file found'
        sys.exit(1)

    hosts = read_hosts()
    if not hosts:
        print 'No host specified'
        sys.exit(1)

    verify_hosts(hosts)

    if not os.path.exists(WRK_BIN):
        build_binary()

    opt = sys.argv[1:]

    runners = [Runner(host, WRK_BIN_RMT, opt) for host in hosts]
    manager = Manager(runners)
    manager.run()

if __name__ == '__main__':
    main()

