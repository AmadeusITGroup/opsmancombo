#!/usr/bin/env python

import argparse
from collections import namedtuple
from datetime import datetime, timedelta
import json
import logging
import sys
import time

from pymongo import MongoClient
import requests

logger = logging.getLogger(__name__)

ConnectionInfo = namedtuple('ConnectionInfo', ['user', 'passwd', 'host', 'port'])


def _parse_version(v):
    parts = v.split('.')
    return tuple(map(int, parts))


def _tuple_subtract(tupa, tupb):
    return tuple([tupa[0] - tupb[0], tupa[1] - tupb[1]])


def _version_str(v):
    return '.'.join(map(str, v))


def _major_version(v):
    return v[:3]


def _enterprise_edition(v):
    return v + '-ent'


class OpsManInvalidState(Exception):
        pass


class OpsManager(object):
    def __init__(self, mmsurl, user, key, verify):
        self.mmsurl = mmsurl
        self.user = user
        self.key = key

        self._session = requests.Session()
        self._session.verify = verify
        self._session.auth = requests.auth.HTTPDigestAuth(self.user, self.key)

    def _url(self, *urlparts):
        return self.mmsurl + '/'.join(urlparts)

    def _get(self, *urlparts, **kwargs):
        r = self._session.get(self._url(*urlparts), **kwargs)
        r.raise_for_status()
        return r.json()

    def _put_json(self, data, *urlparts):
        r = self._session.put(
            self._url(*urlparts),
            data=json.dumps(data),
            headers={'content-type': 'application/json'},
        )
        r.raise_for_status()
        return r.json()

    def _post_json(self, data, *urlparts):
        r = self._session.post(
            self._url(*urlparts),
            data=json.dumps(data),
            headers={'content-type': 'application/json'},
        )
        r.raise_for_status()
        return r.json()

    def _delete_json(self, *urlparts):
        r = self._session.delete(
            self._url(*urlparts),
            headers={'content-type': 'application/json'},
        )
        r.raise_for_status()
        return r.json()

    def get_group_by_name(self, group):
        '''get group from Ops Manager via REST API call'''
        return self._get('/api/public/v1.0/groups/byName', group).get('id')

    def get_groups(self, page=1, items_per_page=100):
        '''get all clusters list from Ops Manager via REST API call'''
        params = {}
        if page != 1:
            params['pageNum'] = page
            params['itemsPerPage'] = items_per_page
        return self._get('/api/public/v1.0/groups', params=params)

    def search_ops(self):
        pair = {}
        num = 1
        while True:
            j = self.get_groups(num)
            for i in j.get('results'):
                if i.get('activeAgentCount') > 0:
                        pair[i.get('name')] = self.get_group_hosts(i.get('id'))
            num += 1
            if j['links'][-1]['rel'] != 'next':
                break
        return pair

    def search_host(self, search):
        '''search node in Ops Manager'''
        result = self.search_ops()
        for k, v in result.iteritems():
            for node in v:
                if node == search:
                    return k

    def get_group_alerts(self, group):
        return self._get('/api/public/v1.0/groups', group, 'alerts')

    def _automation_config_url(self, group):
        return '/api/public/v1.0/groups', group, 'automationConfig'

    def get_automation_config(self, group):
        return self._get(*self._automation_config_url(group))

    def _maintenance_url(self, group, idnum=''):
        return '/api/public/v1.0/groups', group, 'maintenanceWindows', idnum

    def put_automation_config(self, group, data):
        return self._put_json(data, *self._automation_config_url(group))

    def get_maintenance_window(self, group):
        return self._get(*self._maintenance_url(group))

    def post_maintenance_window(self, group, data):
        return self._post_json(data, *self._maintenance_url(group))

    def delete_maintenance_window(self, group, idnum):
        return self._delete_json(*self._maintenance_url(group, idnum))

    def shutdown_db(self, group, host, option):
        j = self.get_automation_config(group)
        for p in j.get('processes'):
            if p.get('hostname') == host and p.get('processType') != 'mongos':
                p['disabled'] = option
        self.put_automation_config(group, j)

    def get_login_data(self, group):
        j = self.get_automation_config(group)
        for k, v in j.get('auth').items():
            if k == 'autoPwd':
                passwd = v
            if k == 'autoUser':
                user = v
        for con in j.get('processes'):
            for k, v in con.iteritems():
                if k == 'processType' and v == 'mongos':
                    host = con.get('hostname')
                    port = int(con.get('args2_6')['net']['port'])
                    break
        return ConnectionInfo(user, passwd, host, port)

    def check_sync(self, group):
        login = self.get_login_data(group)
        tab = []
        connection = MongoClient(login.host, int(login.port))
        connection.admin.authenticate(login.user, login.passwd, mechanism='SCRAM-SHA-1')
        db = connection.admin
        cursor = db.command('listShards')
        for doc in cursor['shards']:
            shardConnection = MongoClient(doc['host'])
            shardConnection.admin.authenticate(login.user, login.passwd, mechanism='SCRAM-SHA-1')
            db = shardConnection.admin
            for i in db.command('replSetGetStatus')['members']:
                tab.append(i.get('state'))
            ''' Each member of a MongoDB replica set has a state
            that reflects its disposition within the set.
            Check if members of  MongoDB replica set are in right state.
            https://docs.mongodb.com/manual/reference/replica-states/'''
        tabnum = set([0, 3, 5, 6, 8, 9])
        numtab = set(tab)
        if numtab.intersection(tabnum):
            raise OpsManInvalidState('MongoDB replicas are in poor condition, contact DBA')
        else:
            logger.debug('MongoDB replicas are in good condition')
            return 0

    def mongo_alerts(self, group):
        j = self.get_group_alerts(group)
        tab = [i.get('status') for i in j.get('results')]
        if 'OPEN' in tab:
            raise OpsManInvalidState('MongoDB has open alert in logs, contact DBA')
        else:
            logger.debug('No open alerts on MongoDB cluster')
            return 0

    def enable_version(self, j, ver):
        mongodb_ver = [{'builds': [{'architecture': 'amd64',
                                    'bits': 64,
                                    'flavor': 'rhel',
                                    'maxOsVersion': '7.0',
                                    'minOsVersion': '6.2',
                                    'gitVersion': '3f76e40c105fc223b3e5aac3e20dcd026b83b38b',
                                    'modules': [
                                                'enterprise'
                                               ],
                                    'platform': 'linux',
                                    'url': ''}],
                        'name': ''}]
        check = [version.get('name') for version in j.get('mongoDbVersions')]
        new_version = _enterprise_edition(ver)
        if new_version not in check:
            path = '/automation/mongodb-releases/linux/mongodb-linux-x86_64-enterprise-rhel62-'
            link = self._url(path + ver + '.tgz')
            for i in mongodb_ver:
                i['builds'][0]['url'] = link
                i['name'] = new_version
            mdb_ver = [i for i in mongodb_ver]
            j['mongoDbVersions'].extend(mdb_ver)
        return j

    def compatibility_version(self, group, j, ver):
        major_ver = _parse_version(ver)
        feature_ver = _parse_version(j.get('processes')[1].get('featureCompatibilityVersion'))
        sub_ver = _tuple_subtract(major_ver, (0, 2))
        new_ver = _version_str(sub_ver)
        if _tuple_subtract(major_ver, feature_ver) > (0, 2):
            for i in j.get('processes'):
                i['featureCompatibilityVersion'] = new_ver
            self._put_json(j, *self._automation_config_url(group))
            self.deploy_change(group)
        if _major_version(j.get('processes')[1].get('version')) < _major_version(ver):
            for i in j.get('processes'):
                i['featureCompatibilityVersion'] = new_ver
        return j

    def upgrade_mongodb(self, group, ver):
        j = self.get_automation_config(group)
        j = self.enable_version(j, ver)
        j = self.compatibility_version(group, j, ver)
        for p in j.get('processes'):
            p['version'] = _enterprise_edition(ver)
        self._put_json(j, *self._automation_config_url(group))
        self.deploy_change(group)

    def get_group_hosts(self, group):
        j = self._get('/api/public/v1.0/groups', group, 'hosts')
        hosts = [
            i.get('hostname') for i in j.get('results') if i.get('typeName') != 'NO_DATA'
        ]
        return list(set(hosts))

    def get(self, url):
        return self._get(url)

    def sleep(self, tim):
        for i in xrange(tim):
            time.sleep(1)
            sys.stdout.flush()

    def cluster_goal_status(self, cluster):
        '''Check if Ops Manager deploying change, please also read link below:
        https://docs.opsmanager.mongodb.com/current/reference/api/automation-status/'''

        j = self._get('/api/public/v1.0/groups', cluster, 'automationStatus')
        goal = j['goalVersion']
        return all([
            goal == i.get('lastGoalVersionAchieved') for i in j.get('processes')
        ])

    def deploy_change(self, group):
        ''' loop untill Ops Manager finish apply new settings'''
        while True:
            if self.cluster_goal_status(group):
                logger.debug('Change has been deployed on MongoDB cluster')
                return 0
                break
            self.sleep(2)

    def check_cluster_health(self, group):
        if not self.cluster_goal_status(group):
            raise OpsManInvalidState('Operation on cluster, try later')
        self.mongo_alerts(group)
        self.check_sync(group)

    def check_maintenance(self, group):
        j = self.get_maintenance_window(group)
        if j.get('results'):
            for i in j.get('results'):
                logger.debug('Maintenance window set, try later')
                logger.debug('Start date : %s', i.get('startDate'))
                logger.debug('End date : %s', i.get('endDate'))
            return False
        else:
            logger.debug('Maintenance window not set')
            return True

    def create_maintenance(self, group):
        j = {}
        j['startDate'] = datetime.now().isoformat()
        j['endDate'] = datetime.max.isoformat()
        j['alertTypeNames'] = ['CLUSTER']
        self.post_maintenance_window(group, j)

    def delete_maintenance(self, group):
        j = self.get_maintenance_window(group)
        if j.get('results'):
            idnum = j.get('results')[0].get('id')
            self.delete_maintenance_window(group, idnum)

    def set_maintenance(self, group):
        if self.check_maintenance(group):
            self.create_maintenance(group)
        else:
            raise OpsManInvalidState('Maintenance window set, try later')


def _parse_args(args=None):
    parser = argparse.ArgumentParser(description='Script to maitenance \
                                     MongoDB cluster via Ops Manager')
    parser.add_argument('-u', '--user',
                        help='Ops Manager user name',
                        required='True')
    parser.add_argument('-k', '--key',
                        help='Ops Manager user REST API key',
                        required='True')
    parser.add_argument('-m', '--mms',
                        help='base Ops Manager url',
                        required='True')
    parser.add_argument('-l', '--error_logfile',
                        help='the path to error log file',
                        required=False)
    parser.add_argument('--verify PATH', dest='verify',
                        help='the path to directory with \
                        certificates of CAs',
                        required=False)
    parser.add_argument('--no-verify', dest='verify', action='store_false')
    parser.set_defaults(verify=True)

    subparsers = parser.add_subparsers(dest='which', help='sub-command help')
    parser_a = subparsers.add_parser('maintenance', help='maintenance help')
    parser_a.add_argument('-n', '--host',
                          help='host FQDN name',
                          required=False)
    parser_a.add_argument('-a', '--action',
                          help='Action',
                          choices=['start', 'stop', 'sync', 'alert', 'check'],
                          required='True')

    parser_b = subparsers.add_parser('upgrade', help='upgrade help')
    parser_b.add_argument('-d', '--database',
                          help='MongoDB database name',
                          required=False)
    parser_b.add_argument('-v', '--version',
                          help='desired MongoDB version',
                          required='True')
    return parser.parse_args(args)


def _setup_logging():
    console_format = '%(levelname)s:Ops Manager: %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=console_format)


def _setup_errorlog(filename):
    file_format = '%(levelname)s: %(asctime)s: %(message)s'
    handler = logging.FileHandler(filename)
    handler.setLevel(logging.ERROR)
    handler.setFormatter(logging.Formatter(file_format))
    logging.getLogger().addHandler(handler)


def ansible_setup():
    from ansible.module_utils.basic import AnsibleModule

    module = AnsibleModule(argument_spec=dict(
        cluster=dict(required=True, type='str'),
        host=dict(required=False, type='str'),
        user=dict(required=True, type='str'),
        key=dict(required=True, type='str'),
        mms=dict(required=True, type='str'),
        verify=dict(required=True),
    ))

    opsmanager = OpsManager(module.params['mms'], module.params['user'],
                            module.params['key'], module.params['verify'])

    return module, opsmanager


def main():
    _setup_logging()
    args = _parse_args(sys.argv[1:])

    log = logging.getLogger('OpsManagerCombo')

    if args.error_logfile:
        _setup_errorlog(args.error_logfile)

    manager = OpsManager(args.mms, args.user, args.key, args.verify)

    if args.which == 'maintenance':
        cluster = manager.search_host(args.host)
        group = manager.get_group_by_name(cluster)

        if args.action == 'stop':
            manager.check_cluster_health(group)
            manager.set_maintenance(group)
            manager.shutdown_db(group, args.host, True)
            manager.deploy_change(group)
            manager.mongo_alerts(group)

        if args.action == 'start':
            manager.mongo_alerts(group)
            manager.shutdown_db(group, args.host, False)
            manager.deploy_change(group)
            manager.check_cluster_health(group)
            manager.delete_maintenance(group)

        if args.action == 'alert':
            manager.mongo_alerts(group)

        if args.action == 'check':
            if manager.cluster_goal_status(group):
                log.debug('No operation on cluster')
            else:
                log.error('operation on cluster, try later')

        if args.action == 'sync':
            manager.check_sync(group)

    if args.which == 'upgrade':
        group = manager.get_group_by_name(args.database)
        manager.check_cluster_health(group)
        manager.set_maintenance(group)
        manager.upgrade_mongodb(group, args.version)
        manager.check_cluster_health(group)
        manager.delete_maintenance(group)


if __name__ == '__main__':
    main()
