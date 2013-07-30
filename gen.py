import datetime

import nova

start = datetime.datetime.utcnow()
tick_time = datetime.timedelta(minutes=1)
compute = nova.Compute(start, tick_time, 05,
                       initial_tenants=30, initial_instances=200,
                       active_actions_target=25)
compute.run()