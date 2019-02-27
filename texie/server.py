#!/usr/bin/env python3

from collections import deque
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from threading import Thread, RLock, currentThread
from twisted.internet import reactor, task
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver

import copy
import hashlib
import string
import random
import re
import time
import logging

ELASTIC_HOST = ["10.0.0.5", "10.0.0.6", "10.0.0.9"]

logging.basicConfig(level=logging.ERROR)

def iter_first_x_elements(iter, stop):
	for e, _ in zip(iter, range(stop)):
		yield e


def read(es, stream, account_id):
	es = Elasticsearch(ELASTIC_HOST)
	body = {
		"sort": [{"timestamp": "desc"}],
		"query": {
			"bool": {
				"must": [
					{"match": {"stream": stream}},
					{"match": {"account_id": account_id}}
				]
			}
		}
	}
	result = es.search(index="data_*", size=1, body=body)
	print(result)
	index = {"i": "I", "f": "F", "b": "B"}[result["hits"]["hits"][0]["_index"][5]]
	value = result["hits"]["hits"][0]["_source"]["value"]
	return index+value

def get_key_by_account(es, account):
	es = Elasticsearch(ELASTIC_HOST)
	result = es.get(index="accounts", doc_type="_doc", id=account)
	return result["_source"]["key"]

class Indexer:
	def __init__(self, main_queue, index_chunk_size=1000, es_host="localhost", run=False):
		self.index_chunk_size = index_chunk_size
		self.queue = [None for _ in range(self.index_chunk_size)]
		self.queue_index = 0
		self.es_host = es_host
		self.conn = False
		self.main_queue = main_queue
		self.flushing = False
		self.connected()

		if run:
			self.run()

	def connected(self):
		try:
			if not self.conn:
				es_hosts = copy.deepcopy(self.es_host)
				random.shuffle(es_hosts)
				self.conn = Elasticsearch(es_hosts)
			ping =  self.conn.ping()
			return ping
		except:
			return False

	def flush(self):
		if self.flushing:
			return
		self.flushing = True
		try:
			start_time = time.time()
			#print("Flushing %i elements..." %self.queue_index)
			if self.queue_index and self.connected():
				with rlock:
					if self.queue_index == self.index_chunk_size:
						for _ in bulk(self.conn, self.queue, chunk_size=self.index_chunk_size):
							pass
					else:
						for _ in bulk(self.conn, iter_first_x_elements(self.queue, self.queue_index), chunk_size=self.index_chunk_size):
							pass
			runtime = time.time() - start_time
			if self.queue_index:
				print("Flushed %i elements in %.2f seconds" %(self.queue_index, runtime))
			self.queue_index = 0
		except:
			self.flushing = False
			raise
		self.flushing = False

	def get_values(self):
		while True:
			if self.flushing:
				time.sleep(0.1)
				continue
			try:
				#with rlock:
				yield self.main_queue.pop()
			except:
				time.sleep(0.1)

	def run(self):
		for e in self.get_values():
			if self.queue_index < self.index_chunk_size:
				self.queue[self.queue_index] = e
				self.queue_index += 1
				if self.queue_index >= self.index_chunk_size:
					self.flush()
			else:
				print("dropping data")


class Protocol(LineReceiver):
	isLeaf = True
	acs = re.escape(string.ascii_letters+string.digits+"\()_.-/ ")
	WRITE_DATASET_RE = re.compile("^W(I|S|F|B)["+acs+"]+:["+acs+"]+$")
	READ_LATEST_DATASET_RE = re.compile("^R["+acs+"]+$")
	X_COMMAND_RE = re.compile("^X(A|B|H)["+acs+"]*:?["+acs+"]*$")

	def answer(self, msg):
		if type(msg) != bytes:
			msg = msg.encode("utf-8")
		self.sendLine(msg)

	def do_init(self):
		self.delimiter = b"\n"
		self.account_id = False
		self.timeout = False
		self.var = False

	def lineReceived(self, request, *args):
		global string_seconds, seconds, main_queue, index_name
		msg = request.decode("utf-8")
		if msg[-2:] == "\r\n":
			msg = msg[:-2]
		elif msg[-1] == "\n":
			msg = msg[:-1]
		#print("Got msg:", msg)
		try:
			if not msg:
				return
			elif self.X_COMMAND_RE.match(msg):
				return self.handle_x(msg)
			elif self.WRITE_DATASET_RE.match(msg) and self.account_id and not self.timeout:  # Write
				return self.handle_write(msg)
			elif self.READ_LATEST_DATASET_RE.match(msg) and self.account_id and not self.timeout:  # Read
				erg = self.handle_read(msg)
				print("Read-erg:", erg)
				self.answer(erg)
				return
			else:
				print("Dismissing due to no matched re:", msg)
			return
		except:
			print("Error occured handling msg:", msg)

	def handle_read(self, msg):
		if msg == "RTIME":
			return "ARTIME:I"+str(int(time.time()))
		elif msg == "RFTIME":
			return "ARFTIME:F"+str(time.time())
		else:
			stream = msg
			result = read(None, stream[1:], self.account_id)
			print(result)
			print("Read not implemented yet:",msg)
			erg = "A"+stream+":"+result
			print("Erg:", erg)
			return erg

	def handle_write(self, msg):
		stream, value = msg.split(":")
		typ = stream[1]
		stream = stream[2:]
		with rlock:
			main_queue.appendleft({"_index": index_names[typ], "_id":self.account_id+":"+stream+":"+string_seconds, "_type": "_doc", \
							  "timestamp": seconds, "stream": stream, "value": value, "account_id": self.account_id})
		return

	def handle_x(self, msg):
		if msg == "XH":
			self.var = "".join([random.choice(string.ascii_letters+string.digits) for _ in range(32)])
			self.answer("AXH"+self.var)
		elif msg[:2] == "XA":
			key, resp = msg[2:].split(":")
			print(key, resp)
			secret = get_key_by_account(None, key)
			print("Got secret:", secret)
			if key and resp == hashlib.sha1(str(self.var+secret).encode("utf-8")).hexdigest():
				self.answer("AXAok")
				self.account_id = key
			else:
				self.answer("AXAfalse")


class ProtocolFactory(Factory):
	def __init__(self):
		pass

	def buildProtocol(self, addr):
		s = Protocol()
		s.do_init()
		return s

def clock():
	global seconds, string_seconds
	seconds = int(time.time()*1000)
	string_seconds = str(int(time.time()))
	#print("clock "+string_seconds)

def looping_call(target, interval):
	while True:
		start_time = time.time()
		try:
			target()
		except Exception as e:
			print("Erroroccured: ",e)
		runtime = time.time()-start_time
		time.sleep(max(0, interval-runtime))

if __name__ == "__main__":
	rlock = RLock()
	index_names = {"F": "ingest_float_data", "I": "ingest_integer_data"}
	main_queue = deque(maxlen=10000)
	seconds = 0
	string_seconds = 0
	clock()
	cf = ProtocolFactory()
	reactor.listenTCP(10100, cf)
	task.LoopingCall(clock).start(1.0)
	for _ in range(1):
		indexer = Indexer(main_queue, es_host=ELASTIC_HOST)
		flushloop = Thread(target=looping_call, args=(indexer.flush, 0.5), daemon=True).start()
		it = Thread(target=indexer.run, daemon=True).start()
	reactor.run()