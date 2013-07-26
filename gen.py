import datetime

import nova

start = datetime.datetime.utcnow()
tick_time = datetime.timedelta(minutes=1)
compute = nova.Compute(start, tick_time, .02)
compute.run()