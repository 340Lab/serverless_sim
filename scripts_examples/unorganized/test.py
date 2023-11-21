# import pprint

import serverless_sim
import json

# pprint.pprint(serverless_sim)

serverless_sim.fn_reset(json.dumps({"plan":"hpa"}))