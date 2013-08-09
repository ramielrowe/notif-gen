import datetime

import notifications
import nova_notif

start = datetime.datetime.utcnow()
tick_time = datetime.timedelta(minutes=1)
notifer = notifications.MongoNotifier('data02.mongo.ramielrowe.com', 27017)
compute = nova_notif.Compute(start, tick_time, 0.00,
                             initial_tenants=30,
                             initial_instances=200,
                             active_actions_target=25,
                             notifier=notifer)
compute.run()