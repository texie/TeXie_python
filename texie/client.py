#!/usr/bin/env python3

import hashlib
import time
import threading

import random
import socket


class TexieClient:
	type_mapping = {"I": int, "F": float, "B": bool, "S": str}
	
	def __init__(self, apikey, secret, server="api.texie.io"):
		self.apikey = apikey
		self.secret = secret
		self.conn = False
		self.state = None
		self.server = server
		self.current_server = None
		self.readbuffer = ""
		self._waitfor_prefix = False
		self._waitfor_erg = False
		self.running = False
	
	def reconnecting(self):
		return self.state == "reconnecting"
	
	def connected(self):
		return self.state == "connected"
	
	def auth(self):
		self.send_line("XH")
		erg = self._wait_for("AXH")
		challenge = erg[3:]
		print("Got challenge:", challenge)
		self.send_line("XA"+self.apikey+":"+hashlib.sha1(str(challenge+self.secret).encode("utf-8")).hexdigest())
		erg = self._wait_for("AXA")
		if erg == "AXAok":
			print("login succesful")
			self.readbuffer = ""
			self.state = "connected"
			return True
		else:
			print("Failed to login...")
			self.state = "auth failed"
			return False
	
	def reconnect(self):
		if self.reconnecting():
			return
		self.state = "reconnecting"
		print("reconnecting...")
		self.conn = False
		self._waitfor_erg = True
		self.readbuffer = ""
		while True:
			try:
				while not self.connect():
					time.sleep(0.01)
				threading.Thread(target=self.auth, daemon=True).start()
				break
			except ConnectionRefusedError:
				continue
			except ConnectionAbortedError:
				continue
		print("reconnected")
	
	def connect(self):
		self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		try:
			if type(self.server) != str:
				self.current_server = random.choice(self.server)
			else:
				self.current_server = self.server
			self.conn.connect((self.current_server, 10100))
			self.conn.settimeout(0.5)
		except OSError:
			return False
		return True
	
	def send_line(self, line):
		if line[-1] != "\n":
			line = line+"\n"
		if type(line) != bytes:
			line = line.encode("utf-8")
		self.conn.send(line)
	
	def read(self, stream):
		if not self.connected():
			raise Exception("connection lost")
		self.send_line("R"+stream)
		erg = self._wait_for("AR"+stream+":")[2+len(stream):]
		if not erg:
			return
		typ = erg[1]
		value = erg[2:]
		try:
			return self.type_mapping[typ](value)
		except KeyError:
			raise NotImplementedError("Unknown type")
	
	def run(self):
		threading.Thread(target=self._run, daemon=True).start()
	
	def _run(self):
		self.running = True
		try:
			while True:
				try:
					erg = ""
					if self.conn:
						erg = self.conn.recv(1).decode("utf-8")
					if erg == "":
						self.reconnect()
					elif erg == "\n":
						if self.readbuffer[:len(self._waitfor_prefix)] == self._waitfor_prefix:
							self._waitfor_erg = self.readbuffer
						self.readbuffer = ""
					else:
						self.readbuffer = self.readbuffer+erg
				except Exception as e:
					if str(e) != "timed out":
						raise
			self.running = False
		except:
			self.running = False
			raise
	def _wait_for(self, prefix):
		self._waitfor_erg = False
		self._waitfor_prefix = prefix
		while not self._waitfor_erg:
			time.sleep(0.1)
		if self._waitfor_erg == True:
			raise Exception("connection lost")
		return self._waitfor_erg
	
	def write(self, stream, var):
		if not self.connected():
			raise Exception("connection lost")
		typ = None
		for k, v in self.type_mapping.items():
			if type(var) == v:
				typ = k
				break
		if typ is None:
			raise NotImplementedError("Unknown type to write...")
		for _ in range(10):
			try:
				self.send_line("W"+typ+stream+":"+str(var))
				return True
			except socket.timeout:
				time.sleep(0.1)
		return False
	
	def info(self):
		return str("Client state: %s, current_server: %s, is_running: %s" %(self.state, self.current_server, self.running))


def sec_info(client):
	while True:
		print(client.info())
		time.sleep(1)

if __name__ == "__main__":
	client = TexieClient("demo", "demo")
	client2 = TexieClient("demo2", "demo2")
	threading.Thread(target=sec_info, daemon=True, args=(client,)).start()

	client.run()
	client2.run()
	while not client.connected() or not client2.connected():
		time.sleep(0.1)

	print("Writing data")
	client.write("zimmer1/temp", 23.5)
	client.write("zimmer2/temp", 23.5)
	client.write("zimmer3/temp", 23.5)
	client.write("zimmer4/temp", 23.5)
	client.write("zimmer1/light", 1)
	client.write("zimmer2/light", 1)
	client.write("zimmer3/light", 1)
	client.write("zimmer4/light", 1)

	client.write("stream", 1)
	client2.write("stream", 2)
	time.sleep(2)

	print("Client1 data:", client.read("stream"))
	print("Client2 data:", client2.read("stream"))

	for e in ("zimmer1/temp", "zimmer2/light"):
		erg = client.read(e)
		print(e, erg, type(erg))

	for i in range(100):
		print("Write:", i)
		client.write("test/nummer1", i)
		time.sleep(1)
		print("read:", client.read("test/nummer1"))