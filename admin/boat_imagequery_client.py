import os
import sys

for i in range(3):
    os.system("docker exec -it boat_container curl -X POST -d '{ \"input\": \"" + str(
        i) + "\" }' --header \"Content-Type:application/json\" 127.0.0.1:" + sys.argv[1] + "/predict")
