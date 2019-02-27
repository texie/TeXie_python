#!/usr/bin/env python3

from groundstation import *
from twisted.internet import reactor


groundstation = TeXieGroundstation("demo", "demo")
satellite_listener = ProtocolFactory(groundstation)
reactor.listenTCP(23456, satellite_listener)
reactor.run()