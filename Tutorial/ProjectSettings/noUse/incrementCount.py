"""
-- Test
"""

import psycopg2, time, os, shutil
from platform import python_version
from datetime import datetime


# local folder
localDest = r"W:\BENUTZER\adebola.hassan\_230222"
protocolMain = f"{localDest}/protocol_transfer_1.txt"


with open(protocolMain, "w") as f:
    f.write(f"Test.\n\n")
    f.close()

jj = 0
while jj <= 10:
    with open(protocolMain, "a") as f:
        f.write(f"I am line {jj} @ - {datetime.now().strftime('%y%m%d_%H:%M:%S')}\n")
        f.close()
    jj += 1

print('Count finished.')