#!/usr/bin/env python3

from client import TexieClient
from twisted.internet import reactor, task
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

import random
import re
import socket
import socketserver
import string
import threading
import time

class Protocol(LineReceiver):
	isLeaf = True
	acs = re.escape(string.ascii_letters+string.digits+"\()_.-/ ")
	WRITE_DATASET_RE = re.compile("^W(I|S|F|B)["+acs+"]+:["+acs+"]+$")
	READ_LATEST_DATASET_RE = re.compile("^R["+acs+"]+$")
	type_mapping = {"I": int, "F": float, "B": bool, "S": str}

	def answer(self, msg):
		if type(msg) != bytes:
			msg = msg.encode("utf-8")
		self.sendLine(msg)

	def do_init(self, tg):
		self.delimiter = b"\n"
		self.account_id = True
		self.timeout = False
		self.var = False
		self.tg = tg

	def lineReceived(self, request, *args):
		global string_seconds, seconds, main_queue, index_name
		msg = request.decode("utf-8")
		if msg[-2:] == "\r\n":
			msg = msg[:-2]
		elif msg[-1] == "\n":
			msg = msg[:-1]
		try:
			if not msg:
				return
			elif self.WRITE_DATASET_RE.match(msg) and self.account_id and not self.timeout:  # Write
				return self.handle_write(msg)
			elif self.READ_LATEST_DATASET_RE.match(msg) and self.account_id and not self.timeout:  # Read
				self.handle_read(msg)
			else:
				print("Dismissing due to no matched re:", msg)
			return
		except:
			print("Error occured handling msg:", msg)

	def handle_write(self, msg):
		stream, value = msg.split(":")
		typ = stream[1]
		stream = stream[2:]
		print("Handle write:", typ, stream, value)
		try:
			self.tg.write(stream, self.type_mapping[typ](value))
		except Exception as e:
			print(e)

	def handle_read(self, msg):
		stream = msg[1:]
		data = self.tg.read(stream)
		data_type = None
		for k, v in self.type_mapping.items():
			if type(data) == v:
				data_type = k
				break
		if data_type is None:
			return
		answer = "A"+msg+":"+data_type+str(data)
		print(answer)
		self.answer(answer)



class ProtocolFactory(Factory):
	def __init__(self, tg):
		self.tg = tg

	def buildProtocol(self, addr):
		s = Protocol()
		s.do_init(tg)
		return s


class TeXieGroundstation:
	def __init__(self, apikey, secret, server=None):
		if server is None:
			server = socket.gethostbyname_ex("api.texie.io")[2]
		self.client = TexieClient(apikey, secret, server=server)
		self.client.run()
		self.threading_server = None
		while not self.client.connected():
			time.sleep(0.1)

	def read(self, stream):
		return self.client.read(stream)

	def write(self, stream, var):
		self.client.write(stream, var)

def sec_info(client):
	while True:
		print(client.info())
		time.sleep(10)

def work(tg):
	while True:
		print(tg.read("TIME"))
		print(tg.read("FTIME"))
		tg.write("gs1/up", 1)
		time.sleep(30)

if __name__ == "__main__":
	"""#cluster_adresses = ("46.232.251.142", "37.120.164.104")
	tg = TeXieGroundstation("demo", "demo")# , cluster_adresses)
	tg.client.run()
	threading.Thread(target=sec_info, daemon=True, args=(tg.client,)).start()

	#cf = ProtocolFactory(tg)
	#reactor.listenTCP(34567, cf)
	threading.Thread(target=work, daemon=True, args=(tg,)).start()
	#threading.Thread(target=reactor.run, daemon=True).start()
	reactor.run()"""

	cluster_adresses = ("46.232.251.142", "37.120.164.104")
	#tg = TeXieGroundstation("demo", "demo")#, cluster_adresses)
	tg = TeXieGroundstation("demo", "demo")#, "api.texie.io")
	print(1)

	threading.Thread(target=sec_info, daemon=True, args=(tg.client,)).start()

	cf = ProtocolFactory(tg)
	reactor.listenTCP(34567, cf)
	threading.Thread(target=work, daemon=True, args=(tg,)).start()
	#threading.Thread(target=reactor.run, daemon=True).start()
	reactor.run()

