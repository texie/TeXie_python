#!/usr/bin/env python3

from texie.client import *

import time

client = TexieClient("demo", "demo")
client.run()

while not client.connected():
	time.sleep(0.1)

print("Time:", client.read("TIME"))
client.write("test", 1)

time.sleep(1)
print("Test:", client.read("test"))
print("Done :)")