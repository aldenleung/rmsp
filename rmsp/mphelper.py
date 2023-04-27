
from enum import Enum
import multiprocessing
import threading
import time
import datetime
import queue
_global_lock = threading.Condition()
def _run_target(q, func, args, kwargs):
	q.put(func(*args, **kwargs))

class ProcessWrapState(Enum):
	RUNNING = 0
	COMPLETE = 1
	ERROR = 2
	PICKLING_ERROR = 3
	PENDING = 4
	
class ProcessWrap():
	def __init__(self, func, args=[], kwargs={}, callback=None, pid=None, use_thread=False):
		#call back will send the following: callback(self.state, self.r, self.begin_time, self.end_time, self.pid)
		self.func = func
		self.args = args
		self.kwargs = kwargs
		self.callback = callback
		self.state = ProcessWrapState.PENDING
		self.pid = pid
		self.use_thread = use_thread
		self.plock = threading.Condition()
		
	def start(self):
		threading.Thread(target=self._start_process, args=[self.func, self.args, self.kwargs]).start()
# 		self._start_process(self.func, self.args, self.kwargs)
	def kill(self):
		self.plock.acquire()
		if self.state == ProcessWrapState.RUNNING:
			self.p.kill()
		self.plock.release()
	def _start_process(self, func, args, kwargs):
		self.begin_time = datetime.datetime.now()
		
		trials = 10
		
		while trials > 0:
			self.plock.acquire()
# 			_global_lock.acquire()
			try:
				if self.use_thread:
					q = queue.Queue()
					p = threading.Thread(target=_run_target, args=[q, func, args, kwargs])
				else:
					q = multiprocessing.Queue()
					p = multiprocessing.Process(target=_run_target, args=[q, func, args, kwargs])
					
				self.p = p
				self.q = q
				self.r = None
				p.start()
# 				_global_lock.release()
				#threading.Thread(target=self._completion_check).start()
				self.plock.release()
				self.state = ProcessWrapState.RUNNING
				self._completion_check()
				break
			except:
				print("There could be unexpected exception on multiprocessing. Retrying pid: ", self.pid)
# 				_global_lock.release()
			#threading.Thread(target=self._completion_check).start()
				self.plock.release()
				time.sleep(3)
			trials -= 1
			
		else:
			print("Cannot start the process. pid: ") + self.pid
	def _completion_check(self):
		# Update frequency
		while self.p.is_alive() and self.q.empty():
			time.sleep(0.5)
		# Try to guess the state: Complete, error, or pickling error?
		# Deduction is not guaranteed to be accurate.
		# In the future I may want to add support to corrupted queue.
		self.plock.acquire()
# 		self.p.join()
		if not self.q.empty():
			self.r = self.q.get()
			max_trials = 3
			while max_trials >= 0:
				try:
					self.p.join()
					break
				except:
					time.sleep(3)
					max_trials -= 1
			self.state = ProcessWrapState.COMPLETE
		else:
			max_trials = 3
			while max_trials >= 0:
				try:
					self.p.join()
					break
				except:
					time.sleep(3)
					max_trials -= 1
			
			if self.use_thread:
				self.state = ProcessWrapState.ERROR
			else:
				if self.p.exitcode != 0:
					self.state = ProcessWrapState.ERROR
				elif self.p.exitcode == 0:
					self.state = ProcessWrapState.PICKLING_ERROR
				else:
					assert False
		self.end_time = datetime.datetime.now()
		
		if not self.use_thread:
			self.p.close()
			self.q.close()
		self.plock.release()
		if self.callback is not None:
			self.callback(self.state, self.r, self.begin_time, self.end_time, self.pid)

# import dill
def _run_target2(q, func, args, kwargs):
	args = [dill.loads(arg) for arg in args]
	kwargs = {dill.loads(k):dill.loads(arg) for k, arg in kwargs.items()}
	q.put(dill.dumps(func(*args, **kwargs)))

class ProcessWrap2():
	# Testing class
	def __init__(self, func, args=[], kwargs={}, callback=None, pid=None, use_thread=False):
		self.func = func
		self.args = args
		self.kwargs = kwargs
		self.callback = callback
		self.state = ProcessWrapState.PENDING
		self.pid = pid
		self.use_thread = use_thread
		self.plock = threading.Condition()
		
	def start(self):
		threading.Thread(target=self._start_process, args=[self.func, self.args, self.kwargs]).start()
# 		self._start_process(self.func, self.args, self.kwargs)
	def kill(self):
		self.plock.acquire()
		if self.state == ProcessWrapState.RUNNING:
			self.p.kill()
		self.plock.release()
	def _start_process(self, func, args, kwargs):
		self.plock.acquire()
		self.begin_time = datetime.datetime.now()
		
		if self.use_thread:
			q = queue.Queue()
			p = threading.Thread(target=_run_target, args=[q, func, args, kwargs])
		else:
			q = multiprocessing.Queue()
			args = [dill.dumps(arg) for arg in args]
			kwargs = {dill.dumps(k):dill.dumps(arg) for k, arg in kwargs.items()}
			
			p = multiprocessing.Process(target=_run_target, args=[q, func, args, kwargs])
			
		self.p = p
		self.q = q
		self.r = None
		_global_lock.acquire()
		p.start()
		_global_lock.release()
		self.state = ProcessWrapState.RUNNING
		#threading.Thread(target=self._completion_check).start()
		self.plock.release()
		self._completion_check()
		
	def _completion_check(self):
		# Update frequency
		while self.p.is_alive() and self.q.empty():
			time.sleep(0.5)
		# Try to guess the state: Complete, error, or pickling error?
		# Deduction is not guaranteed to be accurate.
		# In the future I may want to add support to corrupted queue.
		self.plock.acquire()
# 		self.p.join()
		if not self.q.empty():
			self.r = dill.loads(self.q.get())
			self.p.join()
			self.state = ProcessWrapState.COMPLETE
		else:
			self.p.join()
			if self.use_thread:
				self.state = ProcessWrapState.ERROR
			else:
				if self.p.exitcode != 0:
					self.state = ProcessWrapState.ERROR
				elif self.p.exitcode == 0:
					self.state = ProcessWrapState.PICKLING_ERROR
				else:
					assert False
		self.end_time = datetime.datetime.now()
		
		if not self.use_thread:
			self.p.close()
			self.q.close()
		self.plock.release()
		if self.callback is not None:
			self.callback(self.state, self.r, self.begin_time, self.end_time, self.pid)


from collections import OrderedDict, defaultdict
import concurrent.futures
import logging 
class ProcessWrapPool():
	'''
	Process is not recycled. Hence this is very inefficient for running many short processes. 
	However, it benefits from interruptable Process, and not affected by jupyter's "stop"   
	'''
	def __init__(self, nthread, default_func = None, use_thread=False): #, use_hybrid_process_thread=False 
		self.default_func = default_func
		self.nthread = nthread
		
		self.pending_tasks_lock = threading.Condition() # A lock for accessing pending_tasks and next_pid
		self.pending_tasks = OrderedDict()
		self.next_pid = 0
		
		self.finished_tasks_lock = threading.Condition() # A lock for accessing finished_tasks
		self.finished_tasks = set()
		
		self.futures_lock = threading.Condition() # A lock for accessing futures and futures_pid_map
		self.futures = OrderedDict()
		self.futures_pid_map = OrderedDict() # It is no longer used

		self.closing = False
		
		self.use_thread = use_thread
		# The thread run tills closing is True and no more pending tasks remain.
		threading.Thread(target=self._run_pending_tasks).start()
		
		
	def run(self, func=None, dependencies=[], args=[], kwargs={}):
		'''
		'''
		if self.closing:
			raise ValueError("Invalid state")
		if func is None:
			func = self.default_func
		self.pending_tasks_lock.acquire()
		pid = self.next_pid
		self.next_pid += 1
		self.pending_tasks[pid] = (func, dependencies, args, kwargs)
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		return pid
	
	def _run_pending_tasks(self):
		'''
		Check pending tasks and submit them to the pool executor if the dependencies are fulfilled.
		The submitted pending tasks are removed from self.pending_tasks
		This thread will be run at the beginning, until all pending tasks are complete and close
		'''
		self.pending_tasks_lock.acquire()
		while not self.closing or len(self.pending_tasks) > 0: # Loop until closing and no more pending tasks remain
			# Check if I can run any jobs from the pending tasks
			self.futures_lock.acquire()
			self.finished_tasks_lock.acquire()
			total_running_processes = len(self.futures) - len(self.finished_tasks)
			self.finished_tasks_lock.release()
			self.futures_lock.release()
			
			if total_running_processes < self.nthread:
				nidlethread = self.nthread - total_running_processes
			
				self.finished_tasks_lock.acquire()
				logging.debug(f"Checking pending tasks to submit...")
				available_to_run_pids = []
				for pid, (func, dependencies, args, kwargs) in self.pending_tasks.items():
					if len(dependencies) == 0 or all(dependency in self.finished_tasks for dependency in dependencies):
						available_to_run_pids.append(pid)
				logging.debug(f"Total pending pids available to submit: {len(available_to_run_pids)} / {len(self.pending_tasks)}")
				self.finished_tasks_lock.release()
				
				if len(available_to_run_pids) > 0:
					for pid in available_to_run_pids[:nidlethread]:
						func, dependencies, args, kwargs = self.pending_tasks.pop(pid)
						pw = ProcessWrap(func, args, kwargs, self._completion_callback, pid, use_thread=self.use_thread)
						pw.start()
						
						self.futures_lock.acquire()
						self.futures[pid] = pw
						self.futures_lock.release()
	# 					future.add_done_callback(self._future_result_update)
					logging.info(f"Remaining pending tasks: {len(self.pending_tasks)}")
				else:
					self.pending_tasks_lock.wait()
			else:
				self.pending_tasks_lock.wait()
		self.pending_tasks_lock.release()
	
	def _completion_callback(self, state, r, begin_time, end_time, pid):
		'''
		Add pids to finished tasks. 
		Notify pending tasks
		'''
		self.futures_lock.acquire() # I don't think I need this here
		self.finished_tasks_lock.acquire()
		self.finished_tasks.add(pid)
		logging.info(f"{pid} has finished.")
		#logging.info(f"{len(self.finished_tasks)} / {len(self.futures_pid_map)} has completed.")
		self.finished_tasks_lock.notify_all()
		self.finished_tasks_lock.release()
		self.futures_lock.release()
		
		self.pending_tasks_lock.acquire()
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		


	def get(self, wait=False):
		'''
		A point for synchronization before further job submission. 
		Block and return all results. 
		'''
		if wait:
			self.pending_tasks_lock.acquire()
			while len(self.pending_tasks) > 0:
				self.pending_tasks_lock.wait()
			self.pending_tasks_lock.release()
			self.finished_tasks_lock.acquire()
			while True:
				self.futures_lock.acquire()
				total_running_processes = len(self.futures) - len(self.finished_tasks)
				self.futures_lock.release()
				if total_running_processes == 0:
					break
				logging.info(f"Waiting for {total_running_processes} to complete.")
				self.finished_tasks_lock.wait()
			self.finished_tasks_lock.release()

			
		self.finished_tasks_lock.acquire()
		self.futures_lock.acquire()
		results = OrderedDict()
		error_pids = defaultdict(list)
		for pid in sorted(self.finished_tasks):
			pw = self.futures[pid]
			if pw.state == ProcessWrapState.COMPLETE:
				results[pid] = pw.r
			else:
				error_pids[pw.state].append(pid)
		for pwstate, pids in error_pids.items():
			logging.warn(f"Process State {pwstate.name} occurs for the following pids: " + ",".join(map(str, pids)))
		self.futures_lock.release()
		self.finished_tasks_lock.release()
		return results

	def cancel(self, pids):
		'''
		Attempts to cancel a task if it is still pending. 
		If a task is being executed or completed, it will be cancelled.  
		See also kill. 
		'''
		removed_from_pending_tasks = []
		self.pending_tasks_lock.acquire()
		for pid in pids:
			if pid in self.pending_tasks:
				self.pending_tasks.pop(pid)
				removed_from_pending_tasks.append(pid)
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		
# 		logging.info(f"Removed from pending tasks: {join_str(removed_from_pending_tasks, ',')}")
# 		logging.info(f"Removed from pools: {join_str(removed_from_futures, ',')}")
	def cancel_all(self):
		'''
		Attempts to cancel all pending tasks 
		'''
		removed_from_pending_tasks = []
		self.pending_tasks_lock.acquire()
		for pid in list(self.pending_tasks.keys()):
			self.pending_tasks.pop(pid)
			removed_from_pending_tasks.append(pid)
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		
		removed_from_futures = []
		self.futures_lock.acquire()		
		for pid in self.futures.keys():
			if self.futures[pid].cancel():
				removed_from_futures.append(pid)
		self.futures_lock.release()
		
	def kill(self, pids):
		self.pending_tasks_lock.acquire()
		self.futures_lock.acquire()
		for pid in pids:
			self.futures[pid].kill()
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		self.futures_lock.release()
		
	def kill_all(self, pids):
		self.pending_tasks_lock.acquire()
		self.futures_lock.acquire()
		for pid in list(self.futures.keys()):
			self.futures[pid].kill()
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		self.futures_lock.release()

# 		logging.info(f"Removed from pending tasks: {join_str(removed_from_pending_tasks, ',')}")
# 		logging.info(f"Removed from pools: {join_str(removed_from_futures, ',')}")
	def clear(self):
		'''
		'''
		raise NotImplementedError()
		pass
	def close(self):
		'''
		Terminate the MultiProcessRun
		'''
		self.closing = True
		
		# Notify the pending_tasks_lock so that the thread knows to terminate
		self.pending_tasks_lock.acquire()
		self.pending_tasks_lock.notify_all()
		self.pending_tasks_lock.release()
		
		self.get()
		#self.pool.shutdown()
		

	def __enter__(self): 
		return self
	
	def __exit__(self, type, value, traceback): 
		self.close()
	


# def run_list(data, func, step, nthread):
# 	def f():
# 		return [func(d) for d in data]
# 			
# 	pool = ProcessWrapPool(nthread)
# 	for i in range(0, len(data), step):
# 		pool.run(func, )
# 	.get()
# 	