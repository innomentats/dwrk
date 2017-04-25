#!/usr/bin/env python2
# -*- Mode: python -*-

import os
import sys
import shutil
import subprocess
import threading
import urllib
import tarfile
import json

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
        self.js = list()

    def run(self):
        class ManagerContext:
            def __init__(self, manager):
                self.manager = manager
            def __enter__(self):
                self.manager.fence([runner.init() for runner in self.manager.runners], \
                        self.manager.runners, self.manager.proc_init)
                return self
            def __exit__(self, exc_type, exc_value, traceback):
                self.manager.fence([runner.exit() for runner in self.manager.runners], \
                        self.manager.runners, self.manager.proc_exit)
            def run(self):
                try:
                    self.manager.fence([runner.work() for runner in self.manager.runners], \
                            self.manager.runners, self.manager.proc_work)
                    self.manager.fence([runner.stat() for runner in self.manager.runners], \
                            self.manager.runners, self.manager.proc_stat)
                except KeyboardInterrupt:
                    pass
                except Exception as e:
                    print e
        with ManagerContext(self) as ctx:
            ctx.run()

    def fence(self, cmds, runners, routine):
        ps = [subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, \
                    stdin=subprocess.PIPE) for cmd in cmds]
        ts = [threading.Thread(target=routine, args=(p, runner)) for (p, runner) in zip(ps, runners)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()

    def stat(self):
        if self.js:
            print len(self.js)
            print self.js

    def proc_init(self, p, runner):
        p.communicate()

    def proc_exit(self, p, runner):
        p.communicate()

    def proc_work(self, p, runner):
        runner.stdout, runner.stderr = p.communicate()

    def proc_stat(self, p, runner):
        stdoutdata, stderrdata = p.communicate()
        if stdoutdata:
            try:
                print 'statfile: ', stdoutdata
                self.js.append(json.loads(stdoutdata))
                return
            except ValueError:
                print 'Invalid json returned from', str(runner)
        else:
            print 'No json returned from', str(runner)
        print 'STDOUT from %s:' % str(runner)
        print runner.stdout
        print 'STDERR from %s:' % str(runner)
        print runner.stderr

class Runner:

    def __init__(self, host, cmd, opt):
        self.host = host
        self.cmd = cmd
        self.opt = opt
        self.statfile = '/tmp/stat.wrk.{}'.format(id(self))

    def init(self):
        return ['scp', WRK_BIN, '{}:{}'.format(self.host, WRK_BIN_RMT)]

    def exit(self):
        return ['ssh', self.host, 'rm', '-f', WRK_BIN_RMT, self.statfile]

    def work(self):
        return ['ssh', '-t', '-t', '-q', self.host, '/bin/bash', '-O', 'huponexit', \
                '-c', '\"{}\"'.format(' '.join([WRK_BIN_RMT, '--json', self.statfile] + self.opt))]

    def stat(self):
        return ['ssh', self.host, 'cat', self.statfile]

    def __eq__(self, other):
        return self.host == other.host

    def __hash__(self):
        return hash(self.host)

    def __str__(self):
        return self.host

def build_binary(path='/tmp'):
    os.chdir(path)
    try:
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
        shutil.copy2(os.path.join(path, WRK_DIR, 'wrk'), os.path.join(path, WRK_BIN))
    finally:
        # switch back
        os.chdir(CWD)
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
    for host in hosts:
        subprocess.check_call(['ssh', '-t', '-t', '-q', host, '-oStrictHostKeyChecking=no', \
                'exit', '0'])
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
    manager.stat()

if __name__ == '__main__':
    main()

