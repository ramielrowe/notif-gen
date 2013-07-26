from copy import deepcopy
import datetime
import time
import random
import uuid

import notifications

BASE_NOTIFICATION = {
    '_context_auth_token': 'f00c6ad2ae0cc7a30dfff80e187e1214',
    '_context_instance_lock_checked': False,
    '_context_is_admin': False,
    '_context_project_id': '11a0a07e2aff4638abd8c8765df6e690',
    '_context_project_name': 'invisible_to_admin',
    '_context_quota_class': None,
    '_context_read_deleted': 'no',
    '_context_remote_address': '166.78.17.138',
    '_context_request_id': 'req-9300151b-819d-4c8c-b0f7-628c3897a325',
    '_context_roles': ['Member', 'admin'],
    '_context_service_catalog': [{'endpoints': [{
                                                'adminURL': 'http://166.78.17.138:8776/v1/11a0a07e2aff4638abd8c8765df6e690',
                                                'id': '54316f256c0d418a976e2dde81fe9ae8',
                                                'internalURL': 'http://166.78.17.138:8776/v1/11a0a07e2aff4638abd8c8765df6e690',
                                                'publicURL': 'http://166.78.17.138:8776/v1/11a0a07e2aff4638abd8c8765df6e690',
                                                'region': 'RegionOne'}],
                                  'endpoints_links': [],
                                  'name': 'cinder',
                                  'type': 'volume'}],
    '_context_tenant': '11a0a07e2aff4638abd8c8765df6e690',
    '_context_timestamp': '2013-07-09T16:10:02.816472',
    '_context_user': '11bf1b5b95d549b98c3f9a05006ac830',
    '_context_user_id': '11bf1b5b95d549b98c3f9a05006ac830',
    '_context_user_name': 'demo',
    '_unique_id': 'b996666d011f473d9a6f9766824150fa',
    'event_type': 'compute.instance.create.end',
    'message_id': '3895266e-2012-4e07-a71a-ed291f35fde6',
    'payload': {'access_ip_v4': None,
                'access_ip_v6': None,
                'architecture': None,
                'availability_zone': None,
                'created_at': '2013-07-09T16:10:03.000000',
                'deleted_at': '',
                'disk_gb': 0,
                'display_name': 'test',
                'ephemeral_gb': 0,
                'host': 'stack.dev.ramielrowe.com',
                'hostname': 'test',
                'image_meta': {
                    'base_image_ref': '0582af52-4ce7-4cae-a94e-1952a19831bf',
                    'kernel_id': '2012c25b-2649-4d5a-8c6a-25b49330f1a6',
                    'ramdisk_id': '013b554a-2a66-41b7-bf4a-ac0f651b4cea'},
                'image_name': 'cirros-0.3.1-x86_64-uec',
                'image_ref_url': 'http://166.78.17.138:9292/images/0582af52-4ce7-4cae-a94e-1952a19831bf',
                'instance_id': 'c0256680-27fc-4357-a531-83ce02b5fb15',
                'instance_type': 'm1.nano',
                'instance_type_id': 6,
                'kernel_id': '2012c25b-2649-4d5a-8c6a-25b49330f1a6',
                'launched_at': '2013-07-09T16:10:17.210284',
                'memory_mb': 64,
                'message': 'Success',
                'metadata': [],
                'node': 'stack.dev.ramielrowe.com',
                'os_type': None,
                'ramdisk_id': '013b554a-2a66-41b7-bf4a-ac0f651b4cea',
                'reservation_id': 'r-yoww3da0',
                'root_gb': 0,
                'state': 'active',
                'state_description': '',
                'tenant_id': '11a0a07e2aff4638abd8c8765df6e690',
                'user_id': '11bf1b5b95d549b98c3f9a05006ac830',
                'vcpus': 1},
    'priority': 'INFO',
    'publisher_id': 'compute.stack.dev.ramielrowe.com',
    'timestamp': '2013-07-09 16:10:17.542449'}


def uuid4():
    return str(uuid.uuid4())


class Instance(object):
    def __init__(self, uuid, tenant, cur_time,
                 name=None, instance_type="m1.small", type_id=1):
        self.uuid = uuid

        if not name:
            name = "instance-%s" % uuid
        self.name = name

        self.tenant = tenant
        self.type = instance_type
        self.type_id = type_id
        self.new_type_id = None
        self.created_at = cur_time
        self.launched_at = ''
        self.deleted_at = ''
        self.busy = False

    def to_notification(self, cur_time, event_type, request_id):
        notif = deepcopy(BASE_NOTIFICATION)
        notif['message_id'] = uuid4()

        notif['event_type'] = event_type
        notif['_context_timestamp'] = str(cur_time)
        notif['_context_request_id'] = request_id

        notif['_context_project_id'] = self.tenant
        notif['_context_tenant'] = self.tenant
        notif['_context_user'] = self.tenant
        notif['_context_user_id'] = self.tenant

        payload = notif['payload']
        payload['display_name'] = self.name
        payload['instance_id'] = self.uuid
        payload['instance_type'] = self.type
        payload['instance_type_id'] = self.type_id
        if self.new_type_id:
            payload['new_instance_type_id'] = self.new_type_id
        payload['tenant_id'] = self.tenant
        payload['user_id'] = self.tenant
        payload['created_at'] = str(self.created_at)
        payload['launched_at'] = str(self.launched_at)
        payload['deleted_at'] = str(self.deleted_at)

        return notif


class InstanceAction(object):
    steps = []

    def __init__(self, notifier, cur_time):
        self.notifier = notifier
        self.request_id = "req-%s" % uuid4()
        self.start_time = cur_time
        self.step_start_time = cur_time
        self.instance = None
        self.cur_step = 0

    def tick(self, cur_time):
        elapsed = cur_time - self.step_start_time
        if self.cur_step < len(self.steps):
            step_length = self.steps[self.cur_step][0]
            if step_length <= elapsed:
                self.step(cur_time)
                self.cur_step += 1
                self.step_start_time = cur_time

    def step(self, current_time):
        if self.cur_step < len(self.steps):
            self.steps[self.cur_step][1](current_time)

    def is_done(self):
        return self.cur_step == len(self.steps)

    def get_instance(self):
        return self.instance


class CreateAction(InstanceAction):
    def __init__(self, notifier, cur_time, tenant):
        super(CreateAction, self).__init__(notifier, cur_time)
        self.notifier = notifier
        self.instance_uuid = uuid4()
        self.instance_tenant = tenant
        self.steps = [
            (datetime.timedelta(minutes=0), self._create_start),
            (datetime.timedelta(minutes=random.randrange(2, 7)), self._create_end)
        ]

    def _create_start(self, cur_time):
        self.instance = Instance(self.instance_uuid, self.instance_tenant,
                                 cur_time)
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.create.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _create_end(self, cur_time):
        self.instance.launched_at = cur_time
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.create.end',
                                              self.request_id)
        self.notifier.notify(notif)


class RebuildAction(InstanceAction):
    def __init__(self, notifier, cur_time, instance):
        super(RebuildAction, self).__init__(notifier, cur_time)
        self.notifier = notifier
        self.instance = instance
        self.steps = [
            (datetime.timedelta(minutes=0), self._rebuild_start),
            (datetime.timedelta(minutes=random.randrange(1, 3)), self._rebuild_end)
        ]

    def _rebuild_start(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.rebuild.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _rebuild_end(self, cur_time):
        self.instance.launched_at = cur_time
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.rebuild.end',
                                              self.request_id)
        self.notifier.notify(notif)


class ResizeAction(InstanceAction):
    def __init__(self, notifier, cur_time, instance):
        super(ResizeAction, self).__init__(notifier, cur_time)
        self.notifier = notifier
        self.instance = instance
        self.type_id = random.randrange(1,8)
        self.steps = [
            (datetime.timedelta(minutes=0), self._resize_prep_start),
            (datetime.timedelta(minutes=random.randrange(1, 3)), self._resize_prep_end),
            (datetime.timedelta(minutes=random.randrange(2, 4)), self._resize_start),
            (datetime.timedelta(minutes=random.randrange(2, 4)), self._resize_end),
            (datetime.timedelta(minutes=random.randrange(2, 4)), self._resize_finish_start),
            (datetime.timedelta(minutes=random.randrange(2, 4)), self._resize_finish_end)
        ]

    def _resize_prep_start(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.resize.prep.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _resize_prep_end(self, cur_time):
        self.instance.new_type_id = self.type_id
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.resize.prep.end',
                                              self.request_id)
        self.notifier.notify(notif)

    def _resize_start(self, cur_time):
        self.instance.new_type_id = None
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.resize.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _resize_end(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.resize.end',
                                              self.request_id)
        self.notifier.notify(notif)

    def _resize_finish_start(self, cur_time):
        self.instance.type_id = self.type_id
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.finish_resize.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _resize_finish_end(self, cur_time):
        self.instance.launched_at = cur_time
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.finish_resize.end',
                                              self.request_id)
        self.notifier.notify(notif)


class DeleteAction(InstanceAction):
    def __init__(self, notifier, cur_time, instance):
        super(DeleteAction, self).__init__(notifier, cur_time)
        self.notifier = notifier
        self.instance = instance
        self.steps = [
            (datetime.timedelta(minutes=0), self._delete_start),
            (datetime.timedelta(minutes=random.randrange(1, 2)), self._shutdown_start),
            (datetime.timedelta(minutes=random.randrange(1, 2)), self._shutdown_end),
            (datetime.timedelta(minutes=random.randrange(1, 2)), self._delete_end),
        ]

    def _delete_start(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.delete.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _shutdown_start(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.shutdown.start',
                                              self.request_id)
        self.notifier.notify(notif)

    def _shutdown_end(self, cur_time):
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.shutdown.end',
                                              self.request_id)
        self.notifier.notify(notif)

    def _delete_end(self, cur_time):
        self.instance.deleted_at = cur_time
        notif = self.instance.to_notification(cur_time,
                                              'compute.instance.delete.end',
                                              self.request_id)
        self.notifier.notify(notif)

RANDOM_ACTIONS = [RebuildAction, ResizeAction]


class Compute(object):
    def __init__(self, start_time, tick_length, sleep_time):
        self.notifier = notifications.MySQLNotifier()
        self.start_time = start_time
        self.cur_time = start_time
        self.sleep_time = sleep_time
        self.tick_length = tick_length
        self.tenants = [uuid4(), ]
        self.instances = []
        self.deleted_instances = []
        self.actions = []
        self.running = False
        self.audit_period_start = datetime.datetime(year=start_time.year,
                                                    month=start_time.month,
                                                    day=start_time.day)
        self.audit_period_end = self.audit_period_start + \
                                datetime.timedelta(days=1)

    def _action(self, klass, *args, **kwargs):
        return klass(self.notifier, self.cur_time,
                     *args, **kwargs)

    def _send_exists(self, instance, end=None):
        if not end:
            end = self.audit_period_end

        notif = instance.to_notification(self.cur_time,
                                         'compute.instance.exists',
                                         uuid4())
        payload = notif['payload']
        payload['audit_period_beginning'] = str(self.audit_period_start)
        payload['audit_period_ending'] = str(end)

        self.notifier.notify(notif)

    def _clean_deleted(self):
        def keep(instance):
            return instance.deleted_at > self.audit_period_end

        self.deleted_instances[:] = \
            [x for x in self.deleted_instances if keep(x)]

    def run(self):
        self.running = True
        while self.running:

            if self.cur_time > self.audit_period_end:
                # Do Audit
                for instance in self.instances:
                    self._send_exists(instance)

                for instance in self.deleted_instances:
                    if instance.deleted_at <= self.audit_period_end:
                        self._send_exists(instance)
                self._clean_deleted()

                self.audit_period_start += datetime.timedelta(days=1)
                self.audit_period_end += datetime.timedelta(days=1)

            if random.randrange(0, len(self.tenants)+1) == len(self.tenants):
                # Add New Tenant?
                self.tenants.append(uuid4())

            if random.randrange(0,6) == 3:
                # Create New Instance?
                tenant = random.choice(self.tenants)
                self.actions.append(self._action(CreateAction, tenant))

            free_instances = [x for x in self.instances if not x.busy]

            if free_instances:
                # Do Random Action?
                if random.randrange(0,8) == 4:
                    instance = random.choice(free_instances)
                    instance.busy = True
                    action = random.choice(RANDOM_ACTIONS)
                    free_instances.remove(instance)
                    self._send_exists(instance, end=self.cur_time)
                    self.actions.append(self._action(action, instance))

            if free_instances:
                # Delete Random Instance?
                if random.randrange(0,12) == 6:
                    instance = random.choice(free_instances)
                    instance.busy = True
                    free_instances.remove(instance)
                    self.actions.append(self._action(DeleteAction, instance))

            for action in self.actions:
                # Tick Actions
                action.tick(self.cur_time)
                if action.is_done():
                    instance = action.get_instance()
                    instance.busy = False

                    if isinstance(action, CreateAction):
                        self.instances.append(instance)

                    if isinstance(action, DeleteAction):
                        self.deleted_instances.append(instance)
                        self.instances.remove(instance)

                    self.actions.remove(action)

            stats = (len(self.tenants), len(self.instances),
                     len(self.deleted_instances), len(self.actions))
            #print "T: %s, I: %s, D: %s, A: %s" % stats

            self.cur_time += self.tick_length
            #time.sleep(self.sleep_time)