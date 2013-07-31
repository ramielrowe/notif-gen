from copy import deepcopy
import datetime
import json
import operator
import re

import kombu.common
import kombu.entity
import kombu.pools
import MySQLdb
import pymongo

KEY_SEP = '.'
LIST_IDENT = '*'
SPECIAL_KEY_CHARS = [KEY_SEP, LIST_IDENT]
SPECIAL_RE = {}
for char in SPECIAL_KEY_CHARS:
    SPECIAL_RE[char] = re.compile(r'(?<!\\)[%s]' % char)


def _escape_specials(s):
    for c in SPECIAL_KEY_CHARS:
        s = s.replace(c, '\\%s' % c)
    return s


def _unescape_specials(s):
    for c in SPECIAL_KEY_CHARS:
        s = s.replace('\\%s' % c, c)
    return s

MySQL_INSERT = "INSERT INTO `notifications` SET `message_id` = '%(message_id)s', `key` = '%(key)s', `value` = '%(value)s', `index` = '%(index)s';"


class MySQLNotifier(object):
    def __init__(self):
        self.con = MySQLdb.connect('localhost', 'root', 'password', 'notif_test')
        self.times = []
        self.last_avg_time = datetime.datetime.utcnow()

    def notify(self, notif):
        start = datetime.datetime.utcnow()
        with self.con:
            cur = self.con.cursor()
            for entry in self._event_body_to_entries(notif):
                cur.execute(MySQL_INSERT % entry)
        end = datetime.datetime.utcnow()
        delta = end-start
        self.times.append(delta)
        if end > self.last_avg_time + datetime.timedelta(seconds=10):
            self.last_avg_time = end
            print "%s - %s" % (len(self.times),reduce(operator.add, self.times)/len(self.times))
            self.times = []

    def _list_to_entries(self, event, loc, index, a_list):
        entries = []
        cur_index = 0

        if len(a_list) == 0:
            return [{'message_id': event, 'key': loc, 'index': index+',-1', 'value':''}]

        if loc != '':
            loc += KEY_SEP + LIST_IDENT
        else:
            loc = LIST_IDENT

        for obj in a_list:
            if index != '':
                my_index = index + ',%s' % cur_index
            else:
                my_index = index + '%s' % cur_index

            if isinstance(obj, list):
                entries.extend(self._list_to_entries(event, loc, my_index,
                                                     obj))
            elif isinstance(obj, dict):
                entries.extend(self._dict_to_entries(event, loc, my_index,
                                                     obj))
            else:
                entries.append({'message_id': event, 'key': loc, 'index': my_index, 'value': obj})
            cur_index += 1
        return entries

    def _dict_to_entries(self, event, loc, index, a_dict):
        entries = []

        if len(a_dict) == 0:
            return [{'message_id': event, 'key': loc, 'index': index+',-2', 'value': ''}]

        for key, value in a_dict.iteritems():

            key = _escape_specials(key)

            if loc != '':
                location = loc + KEY_SEP + key
            else:
                location = key

            if isinstance(value, list):
                entries.extend(self._list_to_entries(event, location, index,
                                                     value))
            elif isinstance(value, dict):
                entries.extend(self._dict_to_entries(event, location, index,
                                                     value))
            else:
                entries.append({'message_id': event, 'key': location, 'index': index, 'value': value})
        return entries

    def _event_body_to_entries(self, notif):
        body = deepcopy(notif)
        return self._dict_to_entries(notif['message_id'], '', '', body)


class MongoNotifier(object):
    def __init__(self):
        client = pymongo.MongoClient('primary.mongo.ramielrowe.com', 27017)
        db = client.test
        self.col = db.test2
        self.times = []
        self.last_avg_time = datetime.datetime.utcnow()

    def notify(self, notif):
        start = datetime.datetime.utcnow()
        self.col.insert(notif)
        end = datetime.datetime.utcnow()
        delta = end-start
        self.times.append(delta)
        if end > self.last_avg_time + datetime.timedelta(seconds=10):
            self.last_avg_time = end
            print "%s - %s" % (len(self.times),reduce(operator.add, self.times)/len(self.times))
            self.times = []


class AMQPNotifier(object):

    def __init__(self):
        conn_params = dict(hostname='stack.dev.ramielrowe.com',
                           port=5672,
                           userid='',
                           password='openstack',
                           transport="librabbitmq",
                           virtual_host='test')
        self.conn = kombu.connection.BrokerConnection(**conn_params)
        self.exchange = self._create_exchange('nova', 'topic', durable=True)

    def _send_notification(self, message, routing_key):
        with kombu.pools.producers[self.conn].acquire(block=True) as producer:
            kombu.common.maybe_declare(self.exchange, producer.channel)
            producer.publish(message, routing_key)


    def _create_exchange(self, name, type, exclusive=False, auto_delete=False,
                         durable=True):
        return kombu.entity.Exchange(name, type=type, exclusive=auto_delete,
                                     auto_delete=exclusive, durable=durable)

    def notify(self, notif):
        self._send_notification(notif, 'monitor_test.info')


class PrintNotifier(object):
    count = 0
    def notify(self, notif):
        self.count += 1
        print "%s - %s - %s" % (self.count, notif['event_type'], notif['payload']['instance_id'])


class NoOpNotifier(object):
    def notify(self, notif):
        pass