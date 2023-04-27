import itertools
import psutil
import types	
import os
import traceback

from .rmscore import RMSEntryType
from .interactorhelper import InteractionCore, InteractionCoreRestriction,\
	ForbiddenAccessException 

class RMSTemplateLibraryInteractionCore(InteractionCore):
	def __init__(self, rmstlib):
		self.rmstlib = rmstlib
	def get_books(self):
		return self.rmstlib.get_books()
	def get_section(self, bookname, chaptername, bookmark):
		return self.rmstlib.get_section(bookname, chaptername, bookmark)
	def get_doc(self, bookname, chaptername, bookmark):
		return self.rmstlib.get_doc(bookname, chaptername, bookmark)
	def get_func_signature(self, bookname, chaptername, bookmark):
		return self.rmstlib.get_func_signature(bookname, chaptername, bookmark)
	def simulate(self, bookname, chaptername, bookmark, args=[], kwargs={}):
		return self.rmstlib.simulate(bookname, chaptername, bookmark, args, kwargs)
	def run(self, bookname, chaptername, bookmark, args=[], kwargs={}):
		return self.rmstlib.run(bookname, chaptername, bookmark, args, kwargs)
	def is_file_exist(self, filename):
		return os.path.exists(filename)
	def stat(self):
		return 0
	
	def execute_builder(self):
		self.rmstlib.execute_builder()
		
	
	def execute(self, method, args=[], kwargs={}, request_version=None):
		if method == "get_books":
			return_value = self.get_books(*args, **kwargs)
		elif method == "get_section":
			return_value = self.get_section(*args, **kwargs)
		elif method == "get_doc":
			return_value = self.get_doc(*args, **kwargs)
		elif method == "get_func_signature":
			return_value = self.get_func_signature(*args, **kwargs)
		elif method == "simulate":
			return_value = self.simulate(*args, **kwargs)
		elif method == "run":
			return_value = self.run(*args, **kwargs)
		elif method == "is_file_exist":
			return_value = self.is_file_exist(*args, **kwargs)
		elif method == "execute_builder":
			return_value = self.execute_builder(*args, **kwargs)
		elif method == "stat":
			raise Exception()
			return_value = self.stat(*args, **kwargs)
		else:
			raise Exception("Unsupported request")
		return return_value
		
class RMSPoolInteractionCore(InteractionCore):
	'''
	An interface to interact with RMSPool
	
	
	'''
	def __init__(self, rmspool, restriction=InteractionCoreRestriction.NO_RESTRICTION):
		self.rmspool = rmspool
		self.restriction = restriction
		
	def stat(self, poolstattype):
		if poolstattype == "pending":
			return len(self.rmspool.pending_tasks)
		elif poolstattype == "running":
			return len(self.rmspool.futures) - len(self.rmspool.error_tasks) - len(self.rmspool.finished_tasks)
		elif poolstattype == "finished":
			return len(self.rmspool.finished_tasks)
		elif poolstattype == "error":
			return len(self.rmspool.error_tasks)
		elif poolstattype == "cpu":
			return psutil.getloadavg()[0]/psutil.cpu_count()
		elif poolstattype == "memory":
			mem = psutil.virtual_memory()
			return (mem.total - mem.available) / mem.total
		else:
			raise Exception("Unsupported stat type")
	
	def execute(self, method, args=[], kwargs={}, request_version=None):
		if method == "run":
			self.rmspool.run(*args, **kwargs)
		elif method == "run_template":
			raise Exception()
		elif method == "execute_builder":
			self.execute_builder(*args, **kwargs)
		elif method == "stat":
			return_value = self.stat(*args, **kwargs)
		else:
			raise Exception("Unsupported request")
		return return_value
		
		
class RMSInteractionCore(InteractionCore):
	'''
	An interface to interact with RMS
		
	This is written here so that RMS core can later be accessed via other methods, e.g. network
	'''
	def __init__(self, rms, restriction=InteractionCoreRestriction.NO_RESTRICTION):
		self.rms=rms
		self.restriction = restriction
		self.events_list = []
		rms.registerRMSUpdateListener(types.SimpleNamespace(onRMSUpdate=lambda events: self.events_list.append(events)))
	
	def _retrieve_events(self, event_name, start_idx, end_idx):
		if event_name == "rms":
			if end_idx is None:
				return self.events_list[start_idx:]
			else:
				return self.events_list[start_idx:end_idx]
		
		
	def search_db(self, rmstype, full_search_text, maxitem=100):
		full_search_text = full_search_text.strip()
		queries = full_search_text.split()
		
		if rmstype == "r":
			# It's rare to directly search for a resource, if resource is not documented properly. 
			if len(queries) > 0:	
				condition_term = " AND ".join([f"((rid LIKE '%{search_text}%') OR (description like '%{search_text}%'))" for search_text in queries])
				r_results = self.rms.sql.cursor().execute(f'''SELECT rid FROM resources WHERE {condition_term};''').fetchmany(maxitem)
			else:
				r_results = self.rms.sql.cursor().execute("SELECT rid FROM resources;").fetchmany(maxitem)
			rms_search_result = [self.rms.get_resource(rid) for rid, in r_results]
# 			rms_search_results.append(set([self.rmsp.get_resource(rid).get_full_id() for rid, in r_results]))
		elif rmstype == "f":
			remove_overwritten_condition = "fid NOT IN (SELECT fid FROM file_info WHERE info_key='overwritten')" 
			if len(queries) > 0:	
				condition_term = " AND ".join([f"((fid LIKE '%{search_text}%') OR (file_path like '%{search_text}%') OR (description like '%{search_text}%'))" for search_text in queries])
				condition_term += " AND " + remove_overwritten_condition
				fr_results = self.rms.sql.cursor().execute(f'''SELECT fid FROM files where {condition_term};''').fetchmany(maxitem)
			else:
				fr_results = self.rms.sql.cursor().execute(f"SELECT fid FROM files WHERE {remove_overwritten_condition};").fetchmany(maxitem)
			rms_search_result = [self.rms.get_fileresource(fid) for fid, in fr_results]
# 			rms_search_results.append(set([self.rmsp.get_fileresource(fid) for fid, in fr_results]))
		elif rmstype == "t":
			# search by pipe name
			if len(queries) > 0:	
				cmds = []
				for search_text in queries:
					pid_select_statement = f"SELECT pid FROM pipes WHERE ((pid like '%{search_text}%') OR (func_name like '%{search_text}%') OR (module_name like '%{search_text}%'))"
					task_select_statement = f"SELECT tid FROM tasks WHERE ((tid like '%{search_text}%') OR (description like '%{search_text}%')) OR pid IN ({pid_select_statement})"
					task_args_select_statement = f"SELECT tid FROM tasks_args_json WHERE arg_value LIKE '%{search_text}%'"
					task_kwargs_select_statement = f"SELECT tid FROM tasks_kwargs_json WHERE arg_value LIKE '%{search_text}%'"
					statement = " UNION ".join([task_select_statement, task_args_select_statement, task_kwargs_select_statement])
					cmds.append((f"SELECT tid FROM ({statement})", []))
				sql_cmd = " INTERSECT ".join([cmd[0] for cmd in cmds])
				sql_params = list(itertools.chain.from_iterable([cmd[1] for cmd in cmds]))
# 				print(sql_cmd)
				task_results = self.rms.sql.cursor().execute(sql_cmd, sql_params).fetchmany(maxitem)
			else:
				# Select any task
				task_results = self.rms.sql.cursor().execute("SELECT tid FROM tasks;").fetchmany(maxitem)
			rms_search_result = [self.rms.get_task(tid) for tid, in task_results]
# 			rms_search_results.append(set([self.rmsp.get_task(tid).get_full_id() for tid, in task_results]))
		elif rmstype == "p":
			if len(queries) > 0:				
				condition_term = " AND ".join([f"((pid like '%{search_text}%') OR (func_name like '%{search_text}%') OR (module_name like '%{search_text}%'))" for search_text in queries])
				pipe_results = self.rms.sql.cursor().execute(f'''SELECT pid FROM pipes WHERE {condition_term};''').fetchmany(maxitem)
			else:
				pipe_results = self.rms.sql.cursor().execute("SELECT pid FROM pipes;").fetchmany(maxitem)
			rms_search_result = [self.rms.get_pipe(pid) for pid, in pipe_results]
		else:
			raise Exception()
		return rms_search_result
	def stat(self, rmstype):
		if rmstype == "t":
			count = self.rms.sql.cursor().execute("SELECT COUNT(1) FROM tasks").fetchone()[0]
		elif rmstype == "p":
			count = self.rms.sql.cursor().execute("SELECT COUNT(1) FROM pipes").fetchone()[0]
		elif rmstype == "f":
			count = self.rms.sql.cursor().execute("SELECT COUNT(1) FROM files").fetchone()[0]
		elif rmstype == "r":
			count = self.rms.sql.cursor().execute("SELECT COUNT(1) FROM resources").fetchone()[0]
		else:
			raise Exception("Unsupported")
		return count
	def find_direct_connections(self, src_rmsids, target_rmsids=None):
		if target_rmsids is None:
			target_rmsids = src_rmsids
		connections = []
		for rmsid in src_rmsids:
			upstream_rmsobjs = self.rms.find_upstream_objs([rmsid], 1, target_rmsobjs=target_rmsids)
			for uro in upstream_rmsobjs:
				if uro.get_full_id() != rmsid:
					connections.append((uro.get_full_id(), rmsid))
			downstream_rmsobjs = self.rms.find_downstream_objs([rmsid], 1, target_rmsobjs=target_rmsids)
			for dro in downstream_rmsobjs:
				if rmsid != dro.get_full_id():
					connections.append((rmsid, dro.get_full_id()))
		return connections
	
	def filter_directly_connected_nodes(self, src_rmsids, target_rmsids):
		filtered_rmsids = []
		for rmsid in src_rmsids:
			upstream_rmsobjs = self.rms.find_upstream_objs([rmsid], 1, target_rmsobjs=target_rmsids)
			if len([uro for uro in upstream_rmsobjs if uro.get_full_id() != rmsid]) > 0:
				filtered_rmsids.append(rmsid)
				continue
			downstream_rmsobjs = self.rms.find_downstream_objs([rmsid], 1, target_rmsobjs=target_rmsids)			
			if len([dro for dro in downstream_rmsobjs if rmsid != dro.get_full_id()]) > 0:
				filtered_rmsids.append(rmsid)
				continue
		return filtered_rmsids

	def get_dbid(self):
		return self.rms.database_id	
	def register_file(self, file_path):
		return self.rms.register_file(file_path)
	def get(self, rmsid):
		return self.rms.get(rmsid)
	def has(self, rmsid):
		return self.rms.has(rmsid)
	def delete(self, *rmsids):
		return self.rms.delete(*rmsids)
	
# 	
	def find_connected_nodes(self, rmsids, direction="both", distance=-1, target_rmsobj_type=None):
		if target_rmsobj_type is not None:
			if target_rmsobj_type == "f":
				rmsobj_continue_search_criteria=lambda rmsobj: rmsobj.get_type() != RMSEntryType.FILERESOURCE
			elif target_rmsobj_type == "r":
				rmsobj_continue_search_criteria=lambda rmsobj: rmsobj.get_type() != RMSEntryType.RESOURCE
			elif target_rmsobj_type == "t":
				rmsobj_continue_search_criteria=lambda rmsobj: rmsobj.get_type() != RMSEntryType.TASK
			else:
				rmsobj_continue_search_criteria = lambda rmsobj:True
		else:
			rmsobj_continue_search_criteria = lambda rmsobj:True
		if direction == "downstream":
			results = self.rms.find_downstream_objs(rmsids, distance, rmsobj_continue_search_criteria=rmsobj_continue_search_criteria)
			rmsids = [rmsobj.get_full_id() for rmsobj in results]
		elif direction == "upstream":
			results = self.rms.find_upstream_objs(rmsids, distance, rmsobj_continue_search_criteria=rmsobj_continue_search_criteria)
			rmsids = [rmsobj.get_full_id() for rmsobj in results]
		else: 
			results = self.rms.find_downstream_objs(rmsids, distance, rmsobj_continue_search_criteria=rmsobj_continue_search_criteria)
			rmsids_downstream = [rmsobj.get_full_id() for rmsobj in results]
			results = self.rms.find_upstream_objs(rmsids, distance, rmsobj_continue_search_criteria=rmsobj_continue_search_criteria)
			rmsids_upstream = [rmsobj.get_full_id() for rmsobj in results]
			rmsids = list(set(rmsids_upstream + rmsids_downstream))
		return rmsids
	
	def execute(self, method, args=[], kwargs={}, request_version=None):
		try:
			if self.restriction == InteractionCoreRestriction.READ_ONLY:
				if method in ["register_file", "delete", "create_unruntask"]:
					raise ForbiddenAccessException()
			if method == "search_db":
				return_value = self.search_db(*args, **kwargs)
			elif method == "get_dbid":
				return_value = self.get_dbid(*args, **kwargs)
			elif method == "register_file":
				return_value = self.register_file(*args, **kwargs)
			elif method == "get":
				return_value = self.get(*args, **kwargs)
			elif method == "has":
				return_value = self.has(*args, **kwargs)
			elif method == "delete":
				return_value = self.delete(*args, **kwargs)
			elif method == "create_unruntask":
				raise Exception()
			elif method == "create_unruntask_from_task":
				raise Exception()
			elif method == "link_unruntask":
				raise Exception()
			elif method == "find_direct_connections":
				return self.find_direct_connections(*args, **kwargs)
			elif method == "filter_directly_connected_nodes":
				return self.filter_directly_connected_nodes(*args, **kwargs)
			elif method == "get_template_func_info":
				return self.get_template_func_info(*args, **kwargs)
			elif method == "stat":
				return_value = self.stat(*args, **kwargs)
			elif method == "find_connected_nodes":
				return_value = self.find_connected_nodes(*args, **kwargs)
			else:
				raise Exception("Unsupported request")
		except:
			traceback.print_exc()
			raise Exception("Errors")
		return return_value
	
	def register_listener(self, event_name, listener):
		if event_name == "rms":
			self.rms.registerRMSUpdateListener(types.SimpleNamespace(onRMSUpdate=lambda events, listener=listener: listener(events)))
		
		

	
