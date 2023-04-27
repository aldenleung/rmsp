from enum import IntEnum
import threading
import time
from abc import ABC, abstractmethod

import socketserver
import http.server
import dill as pickle	
import requests

################
# Request
################
class InteractorRequestStatus(IntEnum):
	SUCCESS = 0
	FAILURE = 1
	PENDING = 2
class InteractorRequest():
	# Need to deal with thread safety later
	def __init__(self, method, request_version, args, kwargs):
		self.method = method
		self.request_version = request_version
		self.args = args
		self.kwargs = kwargs

class InteractorRequestResult():
	def __init__(self, request):
		self.request = request
		self._status = InteractorRequestStatus.PENDING
		self._result = None
		self.callbacks = []
		self.lock = threading.Condition()
		
	def register_callback(self, callback):
		self.callbacks.append(callback)
	
	# Need to deal with thread safety later
	def fireUpdateEvent(self):
		for callback in self.callbacks:
			callback(self)
			
	def setResult(self, status, result):
		self.lock.acquire()
		self._status = status
		self._result = result
		self.lock.notify_all()
		self.lock.release()
		self.fireUpdateEvent()
		
		
	def wait(self):
		'''
		block until completion
		'''
		self.lock.acquire()
		while self._status == InteractorRequestStatus.PENDING:
			self.lock.wait()
		self.lock.release()

	@property
	def status(self):
		self.lock.acquire()
		s = self._status
		self.lock.release()
		return s
	@property
	def result(self):
		self.lock.acquire()
		r = self._result
		self.lock.release()
		return r

################
# Interactor
################			
class Interactor(ABC):
	@abstractmethod
	def _send_request(self, irequest_result):
		pass
	def request(self, method, args=[], kwargs={}, callback=None, request_version=None):
		'''
		Request to execute an RMS method. 
		Returns an InteractorRequestResult which include the execution status and results or exception after the execution completes.
		This method does not block.  
		'''
		irequest = InteractorRequest(method, request_version, args, kwargs)
		irequest_result = InteractorRequestResult(irequest)
		if callback is not None:
			irequest_result.register_callback(callback) 
		threading.Thread(target=self._send_request, args=[irequest_result]).start()
		return irequest_result
	
	def execute(self, method, args=[], kwargs={}, request_version=None):
		'''
		Execute an RMS method directly. 
		Block until the result is returned or an exception is raised
		'''
		irequest = InteractorRequest(method, request_version, args, kwargs)
		irequest_result = InteractorRequestResult(irequest)
		self._send_request(irequest_result)
		if irequest_result.status != InteractorRequestStatus.SUCCESS:
			raise Exception()
		return irequest_result.result
	
class LocalInteractor(Interactor):
	def __init__(self, interaction_core):
		self.interaction_core = interaction_core
	
	def _send_request(self, irequest_result):
		irequest = irequest_result.request
		try:
			irequest_result.setResult(InteractorRequestStatus.SUCCESS, self.interaction_core.execute(irequest.method, irequest.args, irequest.kwargs, irequest.request_version))
		except:
			irequest_result.setResult(InteractorRequestStatus.FAILURE, None)
	
	def register_listener(self, event_name, listener):
		self.interaction_core.register_listener(event_name, listener)
class RemoteInteractorClient(Interactor):
	def __init__(self, host, port):
		self.host = host 
		self.port = port
		self.listeners_dict = {} 
		threading.Thread(target=self._background_event_update).start()
		self.last_event_idx_dict = {}
	def _send_request(self, irequest_result):
		irequest = irequest_result.request
		
		response = requests.post(f"http://{self.host}:{self.port}", data=pickle.dumps(irequest))
		if response.status_code == 200:
			irequest_result.setResult(InteractorRequestStatus.SUCCESS, pickle.loads(response.content))
		else:
			irequest_result.setResult(InteractorRequestStatus.FAILURE, None)
	
	def _background_event_update(self):
		while True:
			for event_name in self.listeners_dict:
				irequest = InteractorRequest("_retrieve_events", "1.0.0", [event_name, self.last_event_idx_dict[event_name] + 1, None], kwargs={})
				response = requests.post(f"http://{self.host}:{self.port}", data=pickle.dumps(irequest))
				events_list = pickle.loads(response.content)
				self.last_event_idx_dict[event_name] = self.last_event_idx_dict[event_name] + len(events_list)
				for listener in self.listeners_dict[event_name]:
					for events in events_list:
						listener(events)
			time.sleep(1)
	def register_listener(self, event_name, listener):
		if event_name not in self.listeners_dict:
			self.listeners_dict[event_name] = []
			self.last_event_idx_dict[event_name] = -1
		self.listeners_dict[event_name].append(listener)
class RemoteInteractorHandler(http.server.BaseHTTPRequestHandler):
	def do_POST(self):
		content_len = int(self.headers.get('Content-Length'))
		post_body = self.rfile.read(content_len)
		irequest = pickle.loads(post_body)
		if irequest.method == "_retrieve_events":
			try:
				result = self.server.interaction_core._retrieve_events(*irequest.args, **irequest.kwargs)
				s = pickle.dumps(result)
				self.send_response(200)
				self.send_header("Content-type", "application/octet-stream")
				self.end_headers()
				self.wfile.write(s)
			except Exception as e:
				import traceback
				import sys
				print(str(e))
				print(traceback.format_exception(type(e),  
	                                     e, e.__traceback__),
	          	file=sys.stderr, flush=True)
	
				self.send_response(400)
				
		else:
			try:
				result = self.server.interaction_core.execute(irequest.method, irequest.args, irequest.kwargs, irequest.request_version)
				s = pickle.dumps(result)
				self.send_response(200)
				self.send_header("Content-type", "application/octet-stream")
				self.end_headers()
				self.wfile.write(s)
			except ForbiddenAccessException:
				self.send_response(403)
			except Exception as e:
				import traceback
				import sys
				print(str(e))
				print(traceback.format_exception(type(e),  
	                                     e, e.__traceback__),
	          	file=sys.stderr, flush=True)
	
				self.send_response(400)
	def log_message(self, format, *args):
		pass
class RemoteInteractorServer():
	def __init__(self, interaction_core, host, port):
		self.interaction_core = interaction_core
		self.host = host
		self.port = port
	def serve(self):
		with socketserver.TCPServer((self.host, self.port), RemoteInteractorHandler) as httpd:
			httpd.interaction_core = self.interaction_core
			httpd.serve_forever()
		
# 	def set_protocol_version(self, protocol_version):
# 		self.protocol_version = protocol_version
		
################
# InteractionCore
################		
class InteractionCoreRestriction(IntEnum):
	NO_RESTRICTION = 0
	READ_ONLY = 1

class InteractionCore(ABC):
	@abstractmethod
	def execute(self, method, args=[], kwargs={}, request_version=None):
		pass
			
	def _retrieve_events(self, event_name, start_idx, end_idx):
		pass
	
class ForbiddenAccessException(Exception):
	pass