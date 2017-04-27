#!/usr/bin/env python2
# -*- Mode: python -*-

import os
import sys
import getopt
import shutil
import subprocess
import threading
import urllib
import tarfile
import json
import re

CWD = os.getcwd()

WRK_URL = 'https://github.com/innomentats/wrk/archive/master.tar.gz'
WRK_TAR = 'master.tar.gz'
WRK_DIR = 'wrk-master'
WRK_BIN = os.path.join(CWD, 'wrk')
WRK_HOST = os.path.join(CWD, 'host')
WRK_REMOTE = '/tmp/wrk'

SCALE_TIME = {
        'ms': 1000,
        's': 1000 * 1000,
        'm': 1000 * 1000 * 60,
        'h': 1000 * 1000 * 60 * 60,
}

SCALE_METRIC = {
        'K': 1e3,
        'M': 1e6,
        'G': 1e9,
        'T': 1e12,
        'P': 1e15,
}

SCALE_BINARY = {
        'KB': 1 << 10,
        'MB': 1 << 20,
        'GB': 1 << 30,
        'TB': 1 << 40,
        'PB': 1 << 50,
}

class Record:

    fmt = '''\
threads: {record.threads}
connections: {record.connections}
time_set: {record.time_set}
time_run: {record.time_run}
requests: {record.requests}
rps: {record.rps}
read: {record.read}
bandwidth: {record.bandwidth}
thread_stat_latency:
    mean: {ltc[mean]}
    stdev: {ltc[stdev]}
    max: {ltc[max]}
    +/- stdev: {ltc[+/- stdev]}
thread_stat_rps:
    mean: {rps[mean]}
    stdev: {rps[stdev]}
    max: {rps[max]}
    +/- stdev: {rps[+/- stdev]}'''

    def __init__(self):
        self.threads = 0
        self.connections = 0
        self.time_set = 0
        self.time_run = 0
        self.requests = 0
        self.rps = 0
        self.read = 0
        self.bandwidth = 0
        self.thread_stat_latency = {
                "mean": 0,
                "stdev": 0,
                "max": 0,
                "+/- stdev": 0,
                }
        self.thread_stat_rps = {
                "mean": 0,
                "stdev": 0,
                "max": 0,
                "+/- stdev": 0,
                }
        self.samples = 0

    def __str__(self):
        return Record.fmt.format(record=self,
                ltc=self.thread_stat_latency,
                rps=self.thread_stat_rps)

    def parse(self, json):
        if json:
            self.json = json
            self.samples = 1
            if 'threads' in self.json:
                self.threads = int(self.json['threads'])
            if 'connections' in self.json:
                self.connections = int(self.json['connections'])
            if 'time_set' in self.json:
                self.time_set = self.parse_time(self.json['time_set'])
            if 'time_run' in self.json:
                self.time_run = self.parse_time(self.json['time_run'])
            if 'requests' in self.json:
                self.requests = int(self.json['requests'])
            if 'rps' in self.json:
                self.rps = float(self.json['rps'])
            if 'read' in self.json:
                self.read = self.parse_binary(self.json['read'])
            if 'bandwidth' in self.json:
                self.bandwidth = self.parse_binary(self.json['bandwidth'])
            if 'thread_stat_latency' in self.json:
                self.parse_stat('thread_stat_latency', self.parse_time)
            if 'thread_stat_rps' in self.json:
                self.parse_stat('thread_stat_rps', self.parse_metric)
        return self

    def parse_stat(self, attr, parser):
        dat, res = self.json[attr], getattr(self, attr)
        if 'mean' in dat:
            res['mean'] = parser(dat['mean'])
        if 'stdev' in dat:
            res['stdev'] = parser(dat['stdev'])
        if 'max' in dat:
            res['max'] = parser(dat['max'])
        if '+/- stdev' in dat:
            res['+/- stdev'] = self.parse_decimal(dat['+/- stdev'])

    def parse_time(self, time):
        p = re.compile('([\d.]+)\s*(\w+)')
        t = p.match(time).groups()
        if len(t) == 2:
            val = float(t[0])
            scl = SCALE_TIME[t[1]]
            return val * scl
        else:
            raise ValueError('Invalid time: %s' % time)

    def parse_binary(self, binary):
        p = re.compile('([\d.]+)\s*(\w+)')
        t = p.match(binary).groups()
        if len(t) == 1:
            return float(t[0])
        if len(t) == 2:
            val = float(t[0])
            scl = SCALE_BINARY[t[1]]
            return val * scl
        else:
            raise ValueError('Invalid binary: %s' % binary)

    def parse_metric(self, metric):
        p = re.compile('([\d.]+)\s*(\w+)')
        t = p.match(metric).groups()
        if len(t) == 1:
            return float(t[0])
        if len(t) == 2:
            val = float(t[0])
            scl = SCALE_METRIC[t[1]]
            return val * scl
        else:
            raise ValueError('Invalid metric: %s' % metric)

    def parse_decimal(self, decimal):
        p = re.compile('([\d.]+)\s*([%])')
        t = p.match(decimal).groups()
        if len(t) == 1:
            return float(t[0])
        if len(t) == 2:
            return float(t[0]) * 0.01
        else:
            raise ValueError('Invalid decimal: %s' % decimal)

    def merge(self, other):
        self.merge_stat(other, 'thread_stat_latency')
        self.merge_stat(other, 'thread_stat_rps')

        self.time_set = self.merge_samples(other, 'time_set')
        self.time_run = self.merge_samples(other, 'time_run')

        self.threads += other.threads
        self.connections += other.connections
        self.requests += other.requests
        self.rps += other.rps
        self.read += other.read
        self.bandwidth += other.bandwidth

        self.samples += other.samples
        return self

    def merge_samples(self, other, attr):
        return Record.merge_mean(getattr(self, attr), getattr(other, attr), \
                self.samples, other.samples)

    def merge_stat(self, other, attr):
        sa, oa = getattr(self, attr), getattr(other, attr)
        sa['+/- stdev'] = Record.merge_stdev_perc(sa['+/- stdev'], oa['+/- stdev'], \
                sa['stdev'], oa['stdev'], sa['mean'], oa['mean'], \
                self.requests, other.requests)
        sa['stdev'] = Record.merge_stdev(sa['stdev'], oa['stdev'], \
                sa['mean'], oa['mean'], self.requests, other.requests)
        sa['mean'] = Record.merge_mean(sa['mean'], oa['mean'], \
                self.requests, other.requests)
        sa['max'] = max(sa['max'], oa['max'])


    @staticmethod
    def merge_mean(ma, mb, na, nb):
        return (ma * na + mb * nb) / (na + nb)

    @staticmethod
    def merge_stdev(da, db, ma, mb, na, nb):
        return ((na * (da ** 2 + ma ** 2) + nb * (db ** 2 + mb ** 2)) / (na + nb) - \
                Record.merge_mean(ma, mb, na, nb) ** 2) ** 0.5

    @staticmethod
    def merge_stdev_perc(pa, pb, da, db, ma, mb, na, nb):
        return 0

class Parser:

    def __init__(self, js):
        self.js = js
        self.records = None
        self.result = Record()

    def parse(self):
        self.records = [Record().parse(j) for j in self.js]

    def merge(self):
        self.result = reduce(lambda x, y: x.merge(y), self.records, self.result)
        #for r in self.records:
        #    print r
        print self.result

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
            parser = Parser(self.js)
            parser.parse()
            parser.merge()

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
        return ['scp', WRK_BIN, '{}:{}'.format(self.host, WRK_REMOTE)]

    def exit(self):
        return ['ssh', self.host, 'rm', '-f', WRK_REMOTE, self.statfile]

    def work(self):
        return ['ssh', '-t', '-t', '-q', self.host, '/bin/bash', '-O', 'huponexit', \
                '-c', '\"{}\"'.format(' '.join([WRK_REMOTE, '--json', self.statfile] + self.opt))]

    def stat(self):
        return ['ssh', self.host, 'cat', self.statfile]

    def __eq__(self, other):
        return self.host == other.host

    def __hash__(self):
        return hash(self.host)

    def __str__(self):
        return self.host

def build_binary(opt=None):
    if not opt:
        path='/tmp'
    elif len(opt) == 1:
        path = opt[0]
    else:
        raise ValueError('Too many arguments: {}'.format(str(opt)))

    if not os.path.exists(path) or not os.path.isdir(path):
        raise ValueError('Invalid build directory: {}'.format(path))

    try:
        # build dir
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
        shutil.copy2(os.path.join(path, WRK_DIR, 'wrk'),  WRK_BIN)

    finally:
        try:
            # switch back
            os.chdir(CWD)
            # clear downloaded file and build dir
            os.remove(os.path.join(path, WRK_TAR))
            shutil.rmtree(os.path.join(path, WRK_DIR), ignore_errors=True)
        except:
            pass

def read_hosts():
    with open(WRK_HOST) as f:
        hosts = f.read()
        return filter(lambda x: x != '' and not x.startswith('#'), \
                [line.strip() for line in hosts.splitlines()])

def verify_hosts(hosts):
    print 'Verifying hosts...'
    for host in hosts:
        subprocess.check_call(['ssh', '-t', '-t', '-q', host, '-oStrictHostKeyChecking=no', \
                'exit', '0'])
        print 'Host %s is OK' % host

def parse_opt():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "b:h:", [])
    except getopt.GetoptError as err:
        raise err

    global WRK_BIN, WRK_HOST
    for o, a in opts:
        if o == '-b':
            WRK_BIN = os.path.abspath(a)
        elif o == '-h':
            WRK_HOST = os.path.abspath(a)
        else:
            raise ValueError("unhandled option")

    if not args:
        return None, None

    cmd = args[0]
    cmd_opt = args[1:]
    return cmd, cmd_opt

def print_help():
    print '''\
Usage: {dwrk} <options> command <command_options>

Commands:
    help
        Show this help message
    build
        Download and build wrk binary
    run <wrk_options> url
        Run dwrk using wrk compatible options

Options:
    -b file
        Specify wrk binary file
    -h file
        Specify host file

Example:
    {dwrk} build
    {dwrk} run -t2 -c10 -d10s http://127.0.0.1
    {dwrk} -b /tmp/wrk -h /tmp/host run -t2 -c10 -d10s http://127.0.0.1
'''.format(dwrk=sys.argv[0])

def main():
    cmd, cmd_opt = parse_opt()

    if not cmd or cmd not in ("help", "build", "run"):
        if cmd:
            print 'Unknown command: %s' % cmd
        print_help()
        return

    if cmd == 'help':
        print_help()
        return
    elif cmd == 'build':
        try:
            build_binary(cmd_opt)
        except ValueError as e:
            print 'Failed to build binary: {}'.format(e)
        return

    if not os.path.exists(WRK_BIN):
        print 'No wrk binary found. You can use \'{} build\' to create one, '\
                'or specify it using \'-b\' option'.format(sys.argv[0])
        print 'See more help info using \'{} help\''.format(sys.argv[0])
        sys.exit(1)

    if not os.path.exists(WRK_HOST):
        print 'No host file found. Specify it using \'-h\' option'
        print 'See more help info using \'{} help\''.format(sys.argv[0])
        sys.exit(1)

    hosts = read_hosts()
    if not hosts:
        print 'No host specified'
        sys.exit(1)

    verify_hosts(hosts)

    runners = [Runner(host, WRK_REMOTE, cmd_opt) for host in hosts]
    manager = Manager(runners)
    manager.run()
    manager.stat()

if __name__ == '__main__':
    main()

