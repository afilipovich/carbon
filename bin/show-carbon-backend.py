#!/usr/bin/python

"Parses packrat metric logs and show destination backend for every metric path"

usage = "%prog [OPTIONS] <path/to/relay.conf> <path/to/packrat/logs|metric name>"

epilog = """If neither --is-packrat-log nor --is-metric-path are provided second argument type will be a guess"""

import sys
import imp
from os.path import dirname, join, abspath, exists, realpath, splitext
from os import listdir
from optparse import OptionParser

# Figure out where we're installed
BIN_DIR = dirname(abspath(__file__))
ROOT_DIR = dirname(BIN_DIR)
CONF_DIR = join(ROOT_DIR, 'conf')
default_relayrules = join(CONF_DIR, 'relay.conf')

# Make sure that carbon's 'lib' dir is in the $PYTHONPATH if we're running from
# source.
LIB_DIR = join(ROOT_DIR, 'lib')
sys.path.insert(0, LIB_DIR)

from carbon.routers import ConsistentHashingRouter

def setupOptionParser():
    parser = OptionParser(usage=usage, epilog=epilog)
    parser.add_option('--is-packrat-log',
                default=False,
                action = 'store_true',
                help='Interpret second argument as path to directory with packrat logs')
    parser.add_option('--is-metric-path',
                default=False,
                action = 'store_true',
                help='Interpret second argument as metric path')
    parser.add_option('--show-port',
                default=False,
                action = 'store_true',
                help='Show backend port number and name')
    return parser

def parse_config(config_path):
    options = {}
    f = open(config_path)
    for l in f.readlines():
        l = l.strip()
        if len(l) == 0 or l.startswith('#'):
            continue
        k, v = l.split('=')
        k = k.strip()
        v = v.strip()
        options[k] = v
    return options

def configure_router(config):
    if config['RELAY_METHOD'] == 'consistent-hashing':
        router = ConsistentHashingRouter(config['REPLICATION_FACTOR'])
    else:
        print "Unsupported relay method '%s'" % config['RELAY_METHOD']
        sys.exit(1)
    return router

def load_carbon_hosts(destinations):
    hosts = []
    for host in destinations.split(','):
        host = host.strip()
        parts = host.split(':')
        server = parts[0]
        port = int( parts[1] )
        if len(parts) > 2:
            instance = parts[2]
        else:
            instance = None
        hosts.append( (server, int(port), instance) )
    return hosts

def list_packrat_logs(packrat_logs_path):
    for l in listdir(packrat_logs_path):
        if splitext(l)[-1] == '.log':
            yield abspath(join(packrat_logs_path, l))

def list_metric_lines(packrat_log):
    f = open(packrat_log)
    for l in f.readlines():
        yield(l)

def get_metrics(packrat_logs_path):
    for packrat_log in list_packrat_logs(packrat_logs_path):
        for metric_line in list_metric_lines(packrat_log):
            yield metric_line.split()[0]

def autodetect_path_arg(metric_path):
    if len(metric_path.split('/')) > 3:
        return True
    if len(metric_path.split('.')) > 3:
        return False
    return None

def print_destinations(router, metric, show_port=False):
    for d in router.getDestinations(metric):
        # do not show stacktrace for a broken pipe, e.g. if piped into |head
        try:
            if show_port:
                print '%s  ->  %s:%d:%s' % (metric, d[0], d[1], d[2])
            else:
                print '%s  ->  %s' % (metric, d[0])
        except IOError:
            sys.exit(2)

def main():
    opts_parser = setupOptionParser()
    (options, args) = opts_parser.parse_args()
    if len(args) < 2:
        opts_parser.error("This tool takes at least two arguments")
        sys.exit(1)
    carbon_config_path = args[0]
    metric_path = args[1]
    is_packrat_log = None
    if options.is_packrat_log and options.is_metric_path:
        print "Error: --is-packrat-log and --is-metric-path options are mutually exclusive."
        sys.exit(1)
    elif options.is_packrat_log:
        is_packrat_log = True
    elif options.is_metric_path:
        is_packrat_log = False
    else:
        is_packrat_log = autodetect_path_arg(metric_path)
    if is_packrat_log is None:
        print """Can not recognize if %s is packrat log directory or graphite metric path.
Please provide either --is-packrat-log or --is-metric-path option"""
        sys.exit(1)
    config = parse_config(carbon_config_path)
    router = configure_router(config)
    if config.has_key('KEYFUNC'):
        router.setKeyFunctionFromModule(config['KEYFUNC'])
    for d in load_carbon_hosts(config['DESTINATIONS']):
        router.addDestination(d)
    if is_packrat_log:
        for m in get_metrics(metric_path):
            print_destinations(router, m, options.show_port)
    else:
        print_destinations(router, metric_path, options.show_port)

if __name__ == '__main__':
    main()

