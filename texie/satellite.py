#!/usr/bin/env python3

import hashlib
import time
import threading


GROUNDSTATION = ("127.0.0.1", 34567)

import random
import socket


class TexieSatellite:
	type_mapping = {"I": int, "F": float, "B": bool, "S": str}

	def __init__(self, groundstation="localhost", port=34567):
		self.conn = False
		self.state = None
		self.groundstation = groundstation
		self.port = port
		self.readbuffer = ""
		self._waitfor_prefix = False
		self._waitfor_erg = False
		self.running = False

	def reconnecting(self):
		return self.state == "reconnecting"

	def connected(self):
		return self.state == "connected"

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
				self.state = "connected"
				break
			except ConnectionRefusedError:
				continue
			except ConnectionAbortedError:
				continue
		print("reconnected")

	def connect(self):
		self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		#random.shuffle(self.server)
		#for server in self.server:
		try:
			self.conn.connect((self.groundstation, self.port))
			#self.conn.setblocking(False)
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

	def read(self, stream, timeout=3):
		if not self.connected():
			raise Exception("connection lost")
		self.send_line("R"+stream)
		erg = self._wait_for("AR"+stream+":", timeout=timeout)
		if not erg:
			return
		erg = erg[2+len(stream):]
		#print(erg)
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
				#time.sleep(0.01)
				try:
					erg = ""
					if self.conn:
						erg = self.conn.recv(1).decode("utf-8")
					if erg == "":
						self.reconnect()
					elif erg == "\n":
						#print(self.readbuffer)
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

	def _wait_for(self, prefix, timeout=3):
		self._waitfor_erg = False
		self._waitfor_prefix = prefix
		stop = time.time()+timeout
		while not self._waitfor_erg and stop >= time.time():
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
		self.send_line("W"+typ+stream+":"+str(var))
		return True

	def info(self):
		return str("Client state: %s, is_running: %s" %(self.state, self.running))

def sec_info(client):
	while True:
		print(client.info())
		time.sleep(1)

if __name__ == "__main__":
	texie = TexieSatellite(groundstation=GROUNDSTATION[0], port=GROUNDSTATION[1])
	threading.Thread(target=sec_info, daemon=True, args=(texie,)).start()
	texie.run()
	while not texie.connected():
		time.sleep(0.1)

	print("Connected :) writing data...")

	texie.write("satellite/up", 1)
	print(texie.read("gs1/up"))

	"""while True:
		for i in range(100000):
			client.write("test/nummer"+str(i), i)#"""

	while True:
		if not texie.connected():
			time.sleep(1)
			continue
		try:
			t = texie.read("TIME")
			print("Time:", t, type(t))
			t = texie.read("FTIME")
			print("Float-Time:", t, type(t))
			print(texie.read("gs1/up"))
			time.sleep(10)
		except socket.error:
			texie.reconnect()
		except NotImplementedError:
			pass
		except Exception as e:
			#print(e)
			if str(e) == "connection lost":
				#client.reconnect()
				time.sleep(0.1)
				pass
			else:
				raise