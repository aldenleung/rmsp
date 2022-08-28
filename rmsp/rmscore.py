'''
Running a command in a (another) conda environment

Some software must be run within a specific environment (e.g. python 2). We do not support running python functions designed for different environments. 

 
Created on Jan 28, 2021

@author: Alden
'''

import sys
import os
import subprocess
import itertools
import uuid
import json
import datetime
import dateutil.parser
import types
import inspect
import operator
from enum import Enum
import sqlite3
from collections import defaultdict

import networkx as nx
import dill


try:
	import simplevc
	_SUPPORT_SIMPLEVC = True
except:
	_SUPPORT_SIMPLEVC = False
	
###################################
# RMS Update events
###################################

class RMSUpdateEvent(Enum):
	MODIFY = 1
	INSERT = 2
	DELETE = 3
	CONTENTCHANGE = 4
	
###################################
# RMS Entries and related classes
###################################
class RMSEntryType(Enum):
	NONRMSENTRY = 0
	PIPE = 1
	RESOURCE = 2
	FILERESOURCE = 3
	TASK = 4
	UNRUNTASK = 5
	VIRTUALRESOURCE=6

class RMSEntry:
	def get_type(self):
		raise NotImplementedError()
	def get_id(self):
		raise NotImplementedError()
	def get_full_id(self):
		return (self.get_type(), self.get_id())
	def __key(self):
		return self.get_full_id()
	def __hash__(self):
		return hash(self.__key())
	def __eq__(self, other):
		if isinstance(other, RMSEntry):
			return self.__key() == other.__key()
		return NotImplemented

class Task(RMSEntry):
	def __init__(self, tid, pid, args, kwargs, return_values, output_files, begin_time, end_time, description, tags, info):
		self.tid = tid
		self.pid = pid
		self.args = args
		self.kwargs = kwargs
		self.return_values = return_values
		self.output_files = output_files
		self.begin_time = begin_time
		self.end_time = end_time
		self.description = description
		self.tags = tags
		self.info = info
	@property
	def input_pipes(self):
		return [arg for arg in self.args if isinstance(arg, Pipe)] + [arg for arg in self.kwargs.values() if isinstance(arg, Pipe)]
	@property
	def input_resources(self):
		return [arg for arg in self.args if isinstance(arg, Resource)] + [arg for arg in self.kwargs.values() if isinstance(arg, Resource)]
	@property
	def input_fileresources(self):
		return [arg for arg in self.args if isinstance(arg, FileResource)] + [arg for arg in self.kwargs.values() if isinstance(arg, FileResource)]
	@property
	def output_resources(self):
		return self.return_values
	@property
	def output_fileresources(self):
		return self.output_files
	@property
	def run_time(self):
		return self.end_time - self.begin_time
	def get_type(self):
		return RMSEntryType.TASK	
	def get_id(self):
		return self.tid
class UnrunTask(RMSEntry):
	'''
	return_values could be None if not specified. 
	output_files could be None if not specified
	If the above two are specified (as a list of virtual resources), 
	an update will be applied to replace virtual resource to resource / fileresource
	'''
	def __init__(self, uid, pid, ba, return_values, output_files, 
				task_description="", task_tags=set(), task_info={},
				resource_description="", resource_tags=set(), resource_info={}, 
				fileresource_description="", fileresource_tags=set(), fileresource_info={}):
		self.uid = uid
		self.pid = pid
		self.ba = ba
		self.return_values = return_values
		self.output_files = output_files
		self.task_description = task_description
		self.task_tags = task_tags
		self.task_info = task_info
		self.resource_description = resource_description
		self.resource_tags = resource_tags
		self.resource_info = resource_info
		self.fileresource_description = fileresource_description
		self.fileresource_tags = fileresource_tags
		self.fileresource_info = fileresource_info
		self.replacement = None
	@property
	def input_pipes(self):
		return [arg for arg in self.ba.args if isinstance(arg, Pipe)] + [arg for arg in self.ba.kwargs.values() if isinstance(arg, Pipe)]		
	@property
	def input_resources(self):
		return [arg for arg in self.ba.args if isinstance(arg, Resource)] + [arg for arg in self.ba.kwargs.values() if isinstance(arg, Resource)]
	@property
	def input_virtualresources(self):
		return [arg for arg in self.ba.args if isinstance(arg, VirtualResource)] + [arg for arg in self.ba.kwargs.values() if isinstance(arg, VirtualResource)]
	@property
	def input_fileresources(self):
		return [arg for arg in self.ba.args if isinstance(arg, FileResource)] + [arg for arg in self.ba.kwargs.values() if isinstance(arg, FileResource)]
	@property
	def output_resources(self):
		return [arg for arg in self.return_values if isinstance(arg, Resource)]
	@property
	def output_virtualresources(self):
		return [arg for arg in self.return_values if isinstance(arg, VirtualResource)]
	@property
	def output_fileresources(self):
		return self.output_files

	def get_type(self):
		return RMSEntryType.UNRUNTASK
	
	def get_id(self):
		return self.uid
	@property
	def ready(self):
		'''
		An unruntask is ready if all the arguments of ba is set properly.
		'''
		try:
			self.ba.signature.bind(*self.ba.args, **self.ba.kwargs)
		except TypeError:
			return False
		return True
	
def _invalid_func():
	'''
	An indicator for invalid function when loading pipe from database.
	'''
	raise Exception("Invalid function")

class Pipe(RMSEntry):
	#def __init__(self, pid, func, return_tuple, return_volatile, output_func, is_deterministic, description, tags, info, rmsp):
	def __init__(self, pid, func, return_volatile, is_deterministic, module_name, func_name, output_func, description, tags, info, rms):
		self.pid = pid
		self.func = func 
		self.return_volatile = return_volatile
		self.is_deterministic = is_deterministic
		self.module_name = module_name
		self.func_name = func_name		
		self.output_func = output_func
		self.description = description
		self.tags = tags
		self.info = info
		self.rms = rms

	def __call__(self, *args, **kwargs):
		return self.rms.run(self, args, kwargs)
	def get_type(self):
		return RMSEntryType.PIPE	
	
	def get_id(self):
		return self.pid
class Resource(RMSEntry):
	def __init__(self, rid, tid, volatile, description, tags, info, content=None, has_content=True):
		self.rid = rid
		self.tid = tid
		self.volatile = volatile
		self.description = description
		self.tags = tags
		self.info = info
		self.has_content = has_content
		self._content = content
		
	@property
	def content(self):
		if self.has_content:
			if self.volatile:
				self.has_content = False
			return self._content
		else:
			raise Exception("No content is in resource")
		
	@content.setter
	def content(self, value):
		self.has_content = True
		self._content = value
		
	@content.deleter
	def content(self):
		self.has_content = False
		self._content = None
		
	def get_type(self):
		return RMSEntryType.RESOURCE	
	def get_id(self):
		return self.rid
	
	@property
	def content_type(self):
		if self.has_content:
			return type(self._content)
		else:
			raise Exception("No content is in resource")
	
class FileResource(RMSEntry):
	def __init__(self, fid, tid, file_path, md5, description, tags, info):
		self.fid = fid
		self.tid = tid
		self.file_path = file_path
		self.md5 = md5
		self.description = description
		self.tags = tags
		self.info = info
	def get_type(self):
		return RMSEntryType.FILERESOURCE	
		
	def get_id(self):
		return self.fid

class VirtualResource(RMSEntry):
	def __init__(self, vid, tid, volatile, description, tags, info):
		self.vid = vid
		self.replacement = None
	def get_type(self):
		return RMSEntryType.VIRTUALRESOURCE	
		
	def get_id(self):
		return self.vid
	
class ResourceNotReadyException(Exception):
	'''
	An exception that is called when accessing a resource without content.
	'''
	pass
	
	
class VirtualModule():
	'''
	A virtual class to hold the imported module
	'''
	pass
	
##########################
# Useful SQL commands
##########################

# Inserts
def _sql_insert_fileresource(fileresource):
	cmds = []
	cmds.append(('''INSERT INTO files(fid, file_path, md5, description) VALUES(?,?,?,?)''',
						[fileresource.fid, fileresource.file_path, fileresource.md5, fileresource.description]))
	for t in fileresource.tags: 
		cmds.append(('''INSERT INTO file_tags(fid, tag_value) VALUES(?,?)''',
						[fileresource.fid, t]))
	for k, v in fileresource.info.items(): 
		cmds.append(('''INSERT INTO file_info(fid, info_key, info_value) VALUES(?,?,?)''',
						[fileresource.fid, k, v]))
	return cmds


def _sql_insert_task(task):
	cmds = []
	for i, arg in enumerate(task.args):
		if isinstance(arg, Resource):
			cmds.append(('''INSERT INTO tasks_args_resource(tid, arg_order, rid) VALUES(?,?,?)''',
							[task.tid, i, arg.rid]))
		elif isinstance(arg, FileResource):
			cmds.append(('''INSERT INTO tasks_args_file(tid, arg_order, fid) VALUES(?,?,?)''',
							[task.tid, i, arg.fid]))
		elif isinstance(arg, Pipe):
			cmds.append(('''INSERT INTO tasks_args_pipe(tid, arg_order, pid) VALUES(?,?,?)''',
							[task.tid, i, arg.pid]))
		else:
			cmds.append(('''INSERT INTO tasks_args_json(tid, arg_order, arg_value) VALUES(?,?,?)''',
							[task.tid, i, json.dumps(arg)]))
	for k, arg in task.kwargs.items():
		if isinstance(arg, Resource):
			cmds.append(('''INSERT INTO tasks_kwargs_resource(tid, arg_key, rid) VALUES(?,?,?)''',
							[task.tid, k, arg.rid]))
		elif isinstance(arg, FileResource):
			cmds.append(('''INSERT INTO tasks_kwargs_file(tid, arg_key, fid) VALUES(?,?,?)''',
							[task.tid, k, arg.fid]))
		elif isinstance(arg, Pipe):
			cmds.append(('''INSERT INTO tasks_kwargs_pipe(tid, arg_key, pid) VALUES(?,?,?)''',
							[task.tid, k, arg.pid]))
		else:
			cmds.append(('''INSERT INTO tasks_kwargs_json(tid, arg_key, arg_value) VALUES(?,?,?)''',
							[task.tid, k, json.dumps(arg)]))
	
	cmds.append(('''INSERT INTO tasks_returnvalue(tid, rid) VALUES(?,?)''',
							[task.tid, task.return_values[0].rid]))
	
	for i, f in enumerate(task.output_files): 
		cmds.append(('''INSERT INTO tasks_outputfiles(tid, forder, fid) VALUES(?,?,?)''',
								[task.tid, i, f.fid]))
		
		
	cmds.append(('''INSERT INTO tasks(tid, pid, begin_time, end_time, description) VALUES(?,?,?,?,?)''', 
		[task.tid, task.pid, task.begin_time.isoformat(), task.end_time.isoformat(), task.description]))
	for t in task.tags: 
		cmds.append(('''INSERT INTO task_tags(tid, tag_value) VALUES(?,?)''',
						[task.tid, t]))
	for k, v in task.info.items(): 
		cmds.append(('''INSERT INTO task_info(tid, info_key, info_value) VALUES(?,?,?)''',
						[task.tid, k, v]))
	return cmds
	
def _sql_insert_pipe(pipe):
	cmds = []
	cmds.append(('''INSERT INTO pipes(pid, func, return_volatile, is_deterministic, module_name, func_name, output_func, description) VALUES(?,?,?,?,?,?,?,?)''', 
		[pipe.pid, dill.dumps(pipe.func).hex(), int(pipe.return_volatile), int(pipe.is_deterministic), pipe.module_name, pipe.func_name, dill.dumps(pipe.output_func).hex() if pipe.output_func is not None else None, pipe.description]))
	for t in pipe.tags: 
		cmds.append(('''INSERT INTO pipe_tags(pid, tag_value) VALUES(?,?)''',
						[pipe.pid, t]))
	for k, v in pipe.info.items(): 
		cmds.append(('''INSERT INTO pipe_info(pid, info_key, info_value) VALUES(?,?,?)''',
						[pipe.pid, k, v]))
	return cmds
	
def _sql_insert_resource(resource):
	cmds = []
	cmds.append(('''INSERT INTO resources(rid, volatile, description) VALUES(?,?,?)''',
						[resource.rid, int(resource.volatile), resource.description]))
	for t in resource.tags: 
		cmds.append(('''INSERT INTO resource_tags(rid, tag_value) VALUES(?,?)''',
						[resource.rid, t]))
	for k, v in resource.info.items(): 
		cmds.append(('''INSERT INTO resource_info(rid, info_key, info_value) VALUES(?,?,?)''',
						[resource.rid, k, v]))
	return cmds

# Updates
def _sql_update_fileresource(fid, **kwargs):
	inserted_str = ",".join(map(lambda i:i[0] + "=?", kwargs.items()))
	return(f'''UPDATE files SET {inserted_str} WHERE fid = ?;''', [i[1] for i in kwargs.items()] + [fid])

def _sql_update_task(tid, **kwargs):
	inserted_str = ",".join(map(lambda i:i[0] + "=?", kwargs.items()))
	return(f'''UPDATE tasks SET {inserted_str} WHERE tid = ?;''', [i[1] for i in kwargs.items()] + [tid])

def _sql_update_pipe(pid, **kwargs):
	inserted_str = ",".join(map(lambda i:i[0] + "=?", kwargs.items()))
	return(f'''UPDATE pipes SET {inserted_str} WHERE pid = ?;''', [i[1] for i in kwargs.items()] + [pid])
def _sql_update_resource(rid, **kwargs):
	inserted_str = ",".join(map(lambda i:i[0] + "=?", kwargs.items()))
	return(f'''UPDATE resources SET {inserted_str} WHERE rid = ?;''', [i[1] for i in kwargs.items()] + [rid])
def _sql_update(full_id, **kwargs):
	if full_id[0] == RMSEntryType.FILERESOURCE:
		return _sql_update_fileresource(full_id[1], **kwargs)
	elif full_id[0] == RMSEntryType.TASK:
		return _sql_update_task(full_id[1], **kwargs)
	elif full_id[0] == RMSEntryType.PIPE:
		return _sql_update_pipe(full_id[1], **kwargs)
	elif full_id[0] == RMSEntryType.RESOURCE:
		return _sql_update_resource(full_id[1], **kwargs)
	else:
		raise Exception()
	
# Used in register, update, delete rmsobj and finish task
def _sql_execute_commands(sql, cmd_parameters):
	c = sql.cursor()
	c.execute("BEGIN")
	try:
		for cmd, parameters in cmd_parameters:
			c.execute(cmd, parameters)
		c.execute("COMMIT")
	except sql.Error as e:
		c.execute("ROLLBACK")
		raise Exception("Failure in SQL command execution")
	
	return c



###############################################
# Resources dump and fetch
###############################################
def _has_resource_content(path, resource):
	return os.path.exists(f"{path}/{resource.rid}")

def _load_resource_content(path, resource):
	'''
	Load the content of resource from the desired path, with the rid used as the file name
	'''
	if not os.path.exists(f"{path}/{resource.rid}"):
		raise Exception(f"Resource {resource.rid} is not available")
	with open(f"{path}/{resource.rid}", "rb") as file:
		resource.content = dill.load(file)
	
def _dump_resource_content(path, resource):
	'''
	Load the content of resource from the desired path, with the rid used as the file name
	'''
	if not resource.has_content:
		raise Exception("The resource has no content to save")
	with open(f"{path}/{resource.rid}", "wb") as file:
		dill.dump(resource.content, file)

#####################################
# Functions and signatures 
#####################################
def _modify_func_code(c):
	'''
	The functions try to modify func.__code__ so that two functions defined in different __main__ will remain the same.
	
	Try to remove all co_filename and co_firstlineno from the func codes.
	'''
	if sys.version_info.major == 3 and sys.version_info.minor>=8:
		# 3.8 onwards
		return c.replace(co_consts=tuple(_modify_func_code(i) if isinstance(i, types.CodeType) else i for i in c.co_consts), co_filename="pipe", co_firstlineno = 1)
	else:
		# 3.7
		return types.CodeType(c.co_argcount,
		c.co_kwonlyargcount,
		c.co_nlocals,
		c.co_stacksize,
		c.co_flags,
		c.co_code,
		tuple(_modify_func_code(i) if isinstance(i, types.CodeType) else i for i in c.co_consts),
		c.co_names,
		c.co_varnames,
		"pipe",#c.co_filename,
		c.co_name,
		1,#c.co_firstlineno,
		c.co_lnotab,
		)

def _get_func_signature(func):
	try:
		s = inspect.signature(func)
	except ValueError:
		# Try to use args and kwargs... 20210604 if no signature is found
		s = inspect.Signature([inspect.Parameter("args", inspect.Parameter.VAR_POSITIONAL), inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD)])
	return s

def _get_simplevc_func_signature(func):
	try:
		s = inspect.signature(func)
	except ValueError:
		s = inspect.Signature([inspect.Parameter("args", inspect.Parameter.VAR_POSITIONAL), inspect.Parameter("kwargs", inspect.Parameter.VAR_KEYWORD)])
	s = s.replace(parameters = tuple([p for p in s.parameters.values() if p.kind != inspect.Parameter.VAR_KEYWORD]) + (inspect.Parameter("version", inspect.Parameter.KEYWORD_ONLY),) + tuple([p for p in s.parameters.values() if p.kind == inspect.Parameter.VAR_KEYWORD]))
	return s

def _get_func_ba(func, args, kwargs, partial=False):
	# simplevc support
	if _SUPPORT_SIMPLEVC and func.__module__ in sys.modules and sys.modules[func.__module__] in simplevc.simplevc._registered_modules and func.__name__ in sys.modules[func.__module__]._method_dicts:
		module = sys.modules[func.__module__]
		if "version" not in kwargs or kwargs["version"] is None:
			version = module._version
		else:
			version = kwargs["version"]
		method_dict = module._method_dicts[func.__name__]
		version = simplevc.simplevc._get_last_version(sorted(method_dict.keys()), version)
		if version is None:
			raise Exception("Cannot find appropriate version")
		func =  method_dict[version]
		kwargs = {**kwargs, "version":version}
		if partial:
			ba = _get_simplevc_func_signature(func).bind_partial(*args, **kwargs)
		else:
			ba = _get_simplevc_func_signature(func).bind(*args, **kwargs)
	else:
		if partial:
			ba = _get_func_signature(func).bind_partial(*args, **kwargs)
		else:
			ba = _get_func_signature(func).bind(*args, **kwargs)
	ba.apply_defaults()
	return ba


#####################################
# Files MD5
#####################################
def _get_file_md5(f):
	if not os.path.exists(f):
		return None
	if os.path.isdir(f):
		return None
	try:
		return subprocess.check_output(["md5sum", f]).decode().split()[0]
	except:
		return None
	
	
	
#####################################
# RMS
#####################################	
class ResourceManagementSystem():
	'''
	A resource management system
	'''
	@property
	def sql(self):
		return sqlite3.connect(self.dbfile) 
	
	def __init__(self, dbfile, resource_dump_dir):
		self.dbfile = dbfile
		self.resources_db = {}
		self.pipes_db = {}
		self.tasks_db = {}
		self.fileresources_db = {}
		self.unruntasks_db = {}
		self.virtualresources_db = {}
		self.resource_dump_dir = resource_dump_dir
		self.tokens = {}
		self.rms_update_listeners = []
		self.vm = VirtualModule()
		self.scriptID = None
		
	############
	# RMS Info #
	############
	@property
	def database_id(self):
		dbid, = self.sql.cursor().execute("SELECT infovalue FROM metainfo WHERE infokey = 'dbid'").fetchone()
		return dbid
	def set_scriptID(self, scriptID):
		self.scriptID = scriptID
	####################
	# RMS Update Event #
	####################
		
	def registerRMSUpdateListener(self, listener):
		'''
		Allow an external listener to listen to the broadcast from RMS.
		'''
		self.rms_update_listeners.append(listener)
		
	def fireRMSUpdateEvent(self, events):
		'''
		Fire RMS update events to the registered listeners.  
		'''
		for listener in self.rms_update_listeners:
			listener.onRMSUpdate(events)
			
	######################
	# Internal functions #
	######################
	
	def _obtain_resource_content(self, resource, autofetch=True):
		if not resource.has_content:
			try:
				_load_resource_content(self.resource_dump_dir, resource)
			except:
				if autofetch:
					self.auto_fetch_resource_content([resource])
				else:
					raise Exception("Resource content not available for " + resource.rid)
		return resource.content
	
	def _obtain_fileresource_file_path(self, fileresource):
		return fileresource.file_path
	
	def _obtain_pipe_func(self, pipe):
		return pipe.func
	
	def _obtain_rms_run_content(self, arg):
		if isinstance(arg, RMSEntry):
			if "overwritten" in arg.info or "obsolete" in arg.info:
				raise Exception("Overwritten or obsolete RMS obj cannot be used.")
			if isinstance(arg, Resource):
				return self._obtain_resource_content(arg)
			elif isinstance(arg, FileResource):
				return self._obtain_fileresource_file_path(arg)
			elif isinstance(arg, Pipe):
				return self._obtain_pipe_func(arg)
			else:
				raise Exception()
		else:
			return arg
		
	def _get_new_id(self):
		return uuid.uuid4().hex

	def _as_rmsobj(self, arg):
		if isinstance(arg, RMSEntry):
			rmsobj = arg
		else:
			rmsobj = self.get(arg)
		return rmsobj
	
	#############################
	# Users register a RMSEntry #
	#############################
	def register_pipe(self, func, return_volatile=False, is_deterministic=True, output_func=None, description="", tags={}, pid=None, force=False):
		'''
		'''
		if not callable(func):
			raise Exception("func is not callable, and cannot be registered")
		info = {}
		if func.__module__ == "__main__":
			try:
				info["sourcecode"] = inspect.getsource(func)
			except:
				pass
			func.__code__ = _modify_func_code(func.__code__)
		if output_func is not None:
			if output_func.__module__ == "__main__":
				try:
					info["outputfunc_sourcecode"] = inspect.getsource(output_func)
				except:
					pass
				output_func.__code__ = _modify_func_code(output_func.__code__)
		
		func = dill.loads(dill.dumps(func))
		func_name = func.__name__
		module_name = func.__module__
		c = self.sql.cursor()
		
		c.execute('''SELECT pid FROM pipes where return_volatile IS ? AND is_deterministic IS ? AND module_name IS ? AND func_name IS ?;''', 
				[int(return_volatile), int(is_deterministic), module_name, func_name])
		
		results = c.fetchall()
		if len(results) > 0:
			for old_pid, in results:
				old_pipe = self.get_pipe(old_pid)
				if (dill.dumps(old_pipe.func).hex() == dill.dumps(func).hex()
					and ((old_pipe.output_func is None and output_func is None)
						or (old_pipe.output_func is not None and output_func is not None and dill.dumps(old_pipe.output_func).hex() == dill.dumps(output_func).hex()))):
					return old_pipe
				
		if pid is None:
			pid = self._get_new_id()
		pipe = Pipe(pid, func, return_volatile, is_deterministic, module_name, func_name, output_func, description, tags, info, self)
		_sql_execute_commands(self.sql, _sql_insert_pipe(pipe))
		self.pipes_db[pid] = pipe
		self.fireRMSUpdateEvent([(RMSUpdateEvent.INSERT, pipe.get_full_id())])
		return pipe
	
	def register_file(self, file_path, *, description="", tags=set(), tid=None, fid=None, force=False):
		'''
		'''
		file_path = os.path.abspath(file_path)
		try:
			existing_fileresource = self.file_from_path(file_path)
		except:
			existing_fileresource = None
		
		if force or (existing_fileresource is None):
			info = {}
			if fid is None:
				fid = self._get_new_id()
			if not os.path.exists(file_path):
				raise Exception(f"{file_path} does not exist")
			md5 = _get_file_md5(file_path)
			fileresource = FileResource(fid, tid, file_path, md5, description, tags, info)
			sql_cmds = []
			events = []
			sql_cmds.extend(_sql_insert_fileresource(fileresource))
			if existing_fileresource is not None:
				cur_time = datetime.datetime.now()
				sql_cmds.append(('INSERT INTO file_info(fid, info_key, info_value) VALUES(?,?,?)''', [existing_fileresource.fid, 'overwritten', cur_time.isoformat()]))
				events.append((RMSUpdateEvent.MODIFY, existing_fileresource.get_full_id()))
			_sql_execute_commands(self.sql, sql_cmds)
			self.fileresources_db[fid] = fileresource
			events.append((RMSUpdateEvent.INSERT, fileresource.get_full_id()))
			self.fireRMSUpdateEvent(events)
			return fileresource
		else:
			return existing_fileresource
	#############################
	# Users search for RMSEntry #
	#############################
	#
	# If users know the specific ID, see get
	#
	def file_from_path(self, file_path):
		file_path = os.path.abspath(file_path)
		c = self.sql.cursor()
		#c.execute('''SELECT fid FROM files where file_patfile_pathh = ? ORDER BY (SELECT end_time FROM tasks WHERE tid = tid) DESC;''', [file_path])
		c.execute('''SELECT fid FROM files where file_path = ? ORDER BY (SELECT end_time FROM tasks WHERE tid = tid) DESC;''', [file_path])
		raw_results = c.fetchall()
		results = [fid for fid, in raw_results if "overwritten" not in self.get_fileresource(fid).info and "deprecated" not in self.get_fileresource(fid).info]
		if len(results) > 0:
			if len(results) > 1:
				raise Exception("Found more than one file.")
			fid = results[-1]
			return self.get_fileresource(fid)
		else:
			if len(raw_results) > 0:
				raise Exception("You have requested a deprecated / overwritten file") 
			else:
				raise Exception(f"File {file_path} is not registered.")
	
	def find_pipe(self, func):
		func_name = func.__name__
		module_name = func.__module__
		if func.__module__ == "__main__":
			func.__code__ = _modify_func_code(func.__code__)
		
		c = self.sql.cursor()
		
		c.execute('''SELECT pid FROM pipes where module_name IS ? AND func_name IS ?;''', 
				[module_name, func_name])
		results = c.fetchall()
		filtered_results = []
		if len(results) > 0:
			for old_pid, in results:
				old_pipe = self.get_pipe(old_pid)
				if dill.dumps(old_pipe.func).hex() == dill.dumps(func).hex():
					filtered_results.append(old_pid)

		if len(filtered_results) > 0:
			if len(filtered_results) > 1:
				print("Warning: more than one pipe found.")
			pid = filtered_results[0]
			return self.get_pipe(pid)
		raise Exception("Function not found")

	def find_tasks_by_io(self, iotype="io", fids=[], rids=[], pids=[], target_tids=None):
		'''
		Find tasks that have input and output
		
		If target tids are not supplied, all tasks are returned, which could take a long time to retrieve. 
		'''
		if target_tids is not None:
			s = ','.join(['\'' + tid + '\'' for tid in target_tids])
			tid_inclusion_string = f" AND tid IN ({s})"
		else: 
			tid_inclusion_string = ""
		cmds = []
		for fid in fids:
			if "i" in iotype:
				cmds.append(('''SELECT tid FROM tasks_args_file WHERE fid=?''', [fid]))
				cmds.append(('''SELECT tid FROM tasks_kwargs_file WHERE fid=?''', [fid]))
			if "o" in iotype:
				cmds.append(('''SELECT tid FROM tasks_outputfiles WHERE fid=?''', [fid]))
		for rid in rids:
			if "i" in iotype:
				cmds.append(('''SELECT tid FROM tasks_args_resource WHERE rid=?''', [rid]))
				cmds.append(('''SELECT tid FROM tasks_kwargs_resource WHERE rid=?''', [rid]))
			if "o" in iotype:
				cmds.append(('''SELECT tid FROM tasks_returnvalue WHERE rid=?''', [rid]))
		for pid in pids:
			if "i" in iotype:
				cmds.append(('''SELECT tid FROM tasks_args_pipe WHERE pid=?''', [pid]))
				cmds.append(('''SELECT tid FROM tasks_kwargs_pipe WHERE pid=?''', [pid]))
		sql_cmd = " UNION ".join([cmd[0] + tid_inclusion_string for cmd in cmds])
		sql_params = list(itertools.chain.from_iterable([cmd[1] for cmd in cmds]))
		tasks = [self.get_task(tid) for tid, in self.sql.cursor().execute(sql_cmd, sql_params)]
		return tasks
		
	def find_tasks_by_pipe(self, pids=[]):
		tids = set()
		for pid in pids:
			for tid, in self.sql.cursor().execute('''SELECT tid FROM tasks where pid=?;''', [pid]).fetchall():
				tids.add(tid)
		return [self.get_task(tid) for tid in tids]
	
	def find_tasks_by_pipe_and_args(self, pid=None, args=[], kwargs={}, any_input=[]):
		cmds = []
		cmds.append(('''SELECT tid FROM tasks where pid=?''',
					   [pid]))
		for arg_order, arg in enumerate(args):
			if isinstance(arg, Resource):
				cmds.append(('''SELECT tid FROM tasks_args_resource WHERE arg_order=? AND rid=?''',
								[arg_order, arg.rid]))
			elif isinstance(arg, FileResource):
				cmds.append(('''SELECT tid FROM tasks_args_file WHERE arg_order=? AND fid=?''',
								[arg_order, arg.fid]))
			elif isinstance(arg, Pipe):
				cmds.append(('''SELECT tid FROM tasks_args_pipe WHERE arg_order=? AND pid=?''',
								[arg_order, arg.pid]))
			else:
				cmds.append(('''SELECT tid FROM tasks_args_json WHERE arg_order=? AND arg_value=?''',
								[arg_order, json.dumps(arg)]))
		for k, arg in kwargs.items():
			if isinstance(arg, Resource):
				cmds.append(('''SELECT tid FROM tasks_kwargs_resource WHERE arg_key=? AND rid=?''',
								[k, arg.rid]))
			elif isinstance(arg, FileResource):
				cmds.append(('''SELECT tid FROM tasks_kwargs_file WHERE arg_key=? AND fid=?''',
								[k, arg.fid]))
			elif isinstance(arg, Pipe):
				cmds.append(('''SELECT tid FROM tasks_kwargs_pipe WHERE arg_key=? AND pid=?''',
								[k, arg.pid]))
			else:
				cmds.append(('''SELECT tid FROM tasks_kwargs_json WHERE arg_key=? AND arg_value=?''',
								[k, json.dumps(arg)]))
		for arg in any_input:
			if isinstance(arg, Resource):
				cmds.append(('''(SELECT tid FROM tasks_args_resource WHERE rid=? UNION SELECT tid FROM tasks_kwargs_resource WHERE rid=?)''',
								[arg.rid, arg.rid]))
			elif isinstance(arg, FileResource):
				cmds.append(('''(SELECT tid FROM tasks_args_file WHERE fid=? UNION SELECT tid FROM tasks_kwargs_file WHERE fid=?)''',
								[arg.fid]))
			elif isinstance(arg, Pipe):
				cmds.append(('''(SELECT tid FROM tasks_args_pipe WHERE pid=? UNION SELECT tid FROM tasks_kwargs_pipe WHERE pid=?)''',
								[arg.pid]))
			else:
				cmds.append(('''(SELECT tid FROM tasks_args_json WHERE arg_value=? UNION SELECT tid FROM tasks_kwargs_json WHERE arg_value=?)''',
								[json.dumps(arg)]))
			
		sql_cmd = " INTERSECT ".join([cmd[0] for cmd in cmds])
		sql_params = list(itertools.chain.from_iterable([cmd[1] for cmd in cmds]))
		
		tasks = []
		for tid, in self.sql.cursor().execute(sql_cmd, sql_params):
			task = self.get_task(tid)
			if len(task.args) == len(args) and len(task.kwargs) == len(kwargs):
				tasks.append(task)
		return tasks
	def find_connected_objs(self, args, distance=-1, rmsobj_inclusion_criteria=lambda rmsobj:True, rmsobj_continue_search_criteria=lambda rmsobj:True, target_rmsobjs=None):
		return self._find_objs_by_dfs(self._get_next_connected_objs, args, distance, rmsobj_inclusion_criteria, rmsobj_continue_search_criteria, target_rmsobjs)
	def find_upstream_objs(self, args, distance=-1, rmsobj_inclusion_criteria=lambda rmsobj:True, rmsobj_continue_search_criteria=lambda rmsobj:True, target_rmsobjs=None):
		return self._find_objs_by_dfs(self._get_next_upstream_objs, args, distance, rmsobj_inclusion_criteria, rmsobj_continue_search_criteria, target_rmsobjs)
	def find_downstream_objs(self, args, distance=-1, rmsobj_inclusion_criteria=lambda rmsobj:True, rmsobj_continue_search_criteria=lambda rmsobj:True, target_rmsobjs=None):
		return self._find_objs_by_dfs(self._get_next_downstream_objs, args, distance, rmsobj_inclusion_criteria, rmsobj_continue_search_criteria, target_rmsobjs)
	
	def _find_objs_by_dfs(self, find_next_obj_func, args, distance, rmsobj_inclusion_criteria, rmsobj_continue_search_criteria, target_rmsobjs = None):
		# rmsobj_pool is the one to store the nodes to visit
		# visited is the one to store the output objs 
		
		rmsobjs = list(map(self._as_rmsobj, args))
		if target_rmsobjs is not None:
			target_rmsobjs = list(map(self._as_rmsobj, target_rmsobjs))
		rmsobj_pool = list()
		visited = set()
		for rmsobj in rmsobjs:
			rmsobj_pool.append((rmsobj, distance))		
			#visited.add(rmsobj)
		
		while len(rmsobj_pool) > 0:
			rmsobj, d = rmsobj_pool.pop() # Since we pop at the end.... it should be DFS...
			if d == 0:
				continue
			elif d > 0:
				d -= 1
			for new_rmsobj in find_next_obj_func(rmsobj, target_rmsobjs):
				if new_rmsobj not in visited and rmsobj_inclusion_criteria(new_rmsobj):
					visited.add(new_rmsobj)
					if rmsobj_continue_search_criteria(new_rmsobj):
						rmsobj_pool.append((new_rmsobj, d))
		return visited
		
	def _get_next_upstream_objs(self, rmsobj, target_rmsobjs=None):
		if rmsobj.get_type() == RMSEntryType.FILERESOURCE:
			if rmsobj.tid is None:
				return []
			objs = [self.get_task(rmsobj.tid)]
		elif rmsobj.get_type() == RMSEntryType.RESOURCE:
			if rmsobj.tid is None:
				return []
			objs = [self.get_task(rmsobj.tid)]
		elif rmsobj.get_type() == RMSEntryType.TASK:
			objs = rmsobj.input_resources + rmsobj.input_fileresources + rmsobj.input_pipes
		elif rmsobj.get_type() == RMSEntryType.UNRUNTASK:
			objs = rmsobj.input_resources + rmsobj.input_fileresources + rmsobj.input_pipes + rmsobj.input_virtualresources
		elif rmsobj.get_type() == RMSEntryType.VIRTUALRESOURCE:
			objs = [unruntask for unruntask in self.unruntasks_db.values() if rmsobj in unruntask.output_virtualresources]
		elif rmsobj.get_type() == RMSEntryType.PIPE:
			objs = []
		else:
			raise Exception()
		if target_rmsobjs is not None:
			objs = [rmsobj for rmsobj in objs if rmsobj in target_rmsobjs]
		return objs
		
	def _get_next_downstream_objs(self, rmsobj, target_rmsobjs=None):
		target_tids = None if target_rmsobjs is None else [rmsobj.tid for rmsobj in target_rmsobjs if rmsobj.get_type() == RMSEntryType.TASK]
		if rmsobj.get_type() == RMSEntryType.FILERESOURCE:
			objs = self.find_tasks_by_io(iotype="i", fids=[rmsobj.fid], target_tids=target_tids)
		elif rmsobj.get_type() == RMSEntryType.RESOURCE:
			objs = self.find_tasks_by_io(iotype="i", rids=[rmsobj.rid], target_tids=target_tids) + [unruntask for unruntask in self.unruntasks_db.values() if rmsobj in unruntask.input_resources]
		elif rmsobj.get_type() == RMSEntryType.TASK:
			objs = rmsobj.output_resources + rmsobj.output_fileresources
		elif rmsobj.get_type() == RMSEntryType.UNRUNTASK:
			objs = rmsobj.output_resources + rmsobj.output_fileresources + rmsobj.output_virtualresources
		elif rmsobj.get_type() == RMSEntryType.VIRTUALRESOURCE:
			objs = [unruntask for unruntask in self.unruntasks_db.values() if rmsobj in unruntask.input_virtualresources]
		elif rmsobj.get_type() == RMSEntryType.PIPE:
			objs = self.find_tasks_by_io(iotype="i", pids=[rmsobj.pid], target_tids=target_tids) + [unruntask for unruntask in self.unruntasks_db.values() if rmsobj in unruntask.input_pipes]
		else:
			raise Exception()
		if target_rmsobjs is not None:
			objs = [rmsobj for rmsobj in objs if rmsobj in target_rmsobjs]
		return objs
	def _get_next_connected_objs(self, rmsobj, target_rmsobjs=None):
		return set(self._get_next_upstream_objs(rmsobj, target_rmsobjs) + self._get_next_downstream_objs(rmsobj, target_rmsobjs))
					
						
	############################
	# Users get RMSEntry by ID #
	############################
	#
	# Users get the RMSEntry by ID.
	# If the RMSEntry is already loaded in the memory (DB), use that copy.   
	# Otherwise, check from DB to load it to the memory
	#

	def has(self, arg):
		try:
			self.get(arg)
			return True
		except:
			return False
	def get(self, arg, refetch=False):
		if isinstance(arg, RMSEntry):
			return arg
		rmsid = arg
		if rmsid[0] == RMSEntryType.PIPE:
			return self.get_pipe(rmsid[1], refetch)
		elif rmsid[0] == RMSEntryType.RESOURCE:
			return self.get_resource(rmsid[1], refetch)
		elif rmsid[0] == RMSEntryType.FILERESOURCE:
			return self.get_fileresource(rmsid[1], refetch)
		elif rmsid[0] == RMSEntryType.TASK:
			return self.get_task(rmsid[1], refetch)
		elif rmsid[0] == RMSEntryType.UNRUNTASK:
			return self.get_unruntask(rmsid[1])
		elif rmsid[0] == RMSEntryType.VIRTUALRESOURCE:
			return self.get_virtualresource(rmsid[1])
		else:
			raise Exception()
		
	def get_pipe(self, pid, refetch=False):
		if pid in self.pipes_db and not refetch:
			return self.pipes_db[pid]		
		c = self.sql.cursor()
		c.execute('''SELECT * FROM pipes where pid = ?;''', [pid])
		results = c.fetchall()
		c.execute('''SELECT tag_value FROM pipe_tags where pid = ?;''', [pid])
		tags_results = c.fetchall()
		c.execute('''SELECT info_key, info_value FROM pipe_info where pid = ?;''', [pid])
		info_results = c.fetchall()
		if len(results) != 1:
			raise Exception()
		
		r = results[0]
		pid, func_str, return_volatile, is_deterministic, module_name, func_name, output_func_str, description = (
			r[0],
			r[1],
			bool(r[2]),
			bool(r[3]),
			r[4],
			r[5],
			r[6],
			r[7]
		)
		try:
			
			func = dill.loads(bytes.fromhex(func_str))
		except:
			func = _invalid_func
		if output_func_str is None:
			output_func = None
		else:
			try: 
				output_func = dill.loads(bytes.fromhex(output_func_str))
			except:
				output_func = _invalid_func
		tags = set(tag_value for tag_value, in tags_results)
		info = {k:v for k, v in info_results}
		if pid in self.pipes_db:
			pipe = self.pipes_db[pid]
			pipe.pid=pid
			pipe.func=func
			pipe.return_volatile=return_volatile
			pipe.is_deterministic=is_deterministic
			pipe.output_func=output_func
			pipe.module_name = module_name
			pipe.func_name = func_name
			pipe.description=description
			pipe.tags=tags
			pipe.info=info
		else:
			pipe = Pipe(pid, func, return_volatile, is_deterministic, module_name, func_name, output_func, description, tags, info, self)
			self.pipes_db[pipe.pid] = pipe
		return pipe
	
	def get_resource(self, rid, refetch=False):
		if rid in self.resources_db and not refetch:
			return self.resources_db[rid]
		c = self.sql.cursor()
		c.execute('''SELECT * FROM resources where rid = ?;''', [rid])
		results = c.fetchall()
		c.execute('''SELECT tag_value FROM resource_tags where rid = ?;''', [rid])
		tags_results = c.fetchall()
		c.execute('''SELECT info_key, info_value FROM resource_info where rid = ?;''', [rid])
		info_results = c.fetchall()
		c.execute('''SELECT tid FROM tasks_returnvalue where rid = ?;''', [rid])
		task_results = c.fetchall()
		
		if len(results) != 1:
			raise Exception(results)
		if len(task_results) > 1:
			raise Exception()
		elif len(task_results) == 1:
			tid, = task_results[0]
		else:
			tid = None
		r = results[0]
		rid, volatile, description = (
			r[0],
			bool(r[1]),
			r[2]
		)
		tags = set(tag_value for tag_value, in tags_results)
		info = {k:v for k, v in info_results}
		
		if rid in self.resources_db:
			resource = self.resources_db[rid]
			resource.rid=rid
			resource.tid=tid
			resource.volatile=volatile
			resource.description=description
			resource.tags=tags
			resource.info=info
		else:
			resource = Resource(rid, tid, volatile, description, tags, info, None, False)
			self.resources_db[resource.rid] = resource
		return resource

	def get_fileresource(self, fid, refetch=False):
		if fid in self.fileresources_db and not refetch:
			return self.fileresources_db[fid]
		c = self.sql.cursor()
		c.execute('''SELECT * FROM files where fid = ?;''', [fid])
		results = c.fetchall()
		c.execute('''SELECT tag_value FROM file_tags where fid = ?;''', [fid])
		tags_results = c.fetchall()
		c.execute('''SELECT info_key, info_value FROM file_info where fid = ?;''', [fid])
		info_results = c.fetchall()
		c.execute('''SELECT tid FROM tasks_outputfiles where fid = ?;''', [fid])
		task_results = c.fetchall()
		if len(results) != 1:
			raise Exception()
		if len(task_results) > 1:
			raise Exception()
		elif len(task_results) == 1:
			tid, = task_results[0]
		else:
			tid = None
		r = results[0]
		fid, file_path, md5, description = (
			r[0],
			r[1],
			r[2],
			r[3]
		)
		tags = set(tag_value for tag_value, in tags_results)
		info = {k:v for k, v in info_results}
		
		if fid in self.fileresources_db:
			fileresource = self.fileresources_db[fid]
			fileresource.fid=fid
			fileresource.tid=tid
			fileresource.file_path=file_path
			fileresource.md5=md5
			fileresource.description=description
			fileresource.tags=tags
			fileresource.info=info
		else:
			fileresource = FileResource(fid, tid, file_path, md5, description, tags, info)
			self.fileresources_db[fileresource.fid] = fileresource
		return fileresource

	def get_task(self, tid, refetch=False):
		
		if tid in self.tasks_db and not refetch:
			return self.tasks_db[tid]
		c = self.sql.cursor()
		c.execute('''SELECT * FROM tasks where tid = ?;''', [tid])
		results = c.fetchall()
		if len(results) != 1:
			raise Exception()

		c.execute('''SELECT tag_value FROM task_tags where tid = ?;''', [tid])
		tags_results = c.fetchall()
		c.execute('''SELECT info_key, info_value FROM task_info where tid = ?;''', [tid])
		info_results = c.fetchall()
		
		c.execute('''SELECT arg_order, arg_value FROM tasks_args_json where tid = ?;''', [tid])
		args_json = [(arg_order, json.loads(arg_value)) for arg_order, arg_value in c.fetchall()]
		c.execute('''SELECT arg_order, rid FROM tasks_args_resource where tid = ?;''', [tid])
		args_resource = [(arg_order, self.get_resource(rid)) for arg_order, rid in c.fetchall()]
		c.execute('''SELECT arg_order, fid FROM tasks_args_file where tid = ?;''', [tid])
		args_fileresource = [(arg_order, self.get_fileresource(fid)) for arg_order, fid in c.fetchall()]
		c.execute('''SELECT arg_order, pid FROM tasks_args_pipe where tid = ?;''', [tid])
		args_pipe = [(arg_order, self.get_pipe(pid)) for arg_order, pid in c.fetchall()]
		args = list(map(operator.itemgetter(1), sorted(itertools.chain.from_iterable([args_json, args_resource, args_fileresource, args_pipe]), key=operator.itemgetter(0))))

		c.execute('''SELECT arg_key, arg_value FROM tasks_kwargs_json where tid = ?;''', [tid])
		kwargs_json = {arg_key:json.loads(arg_value) for arg_key, arg_value in c.fetchall()}
		c.execute('''SELECT arg_key, rid FROM tasks_kwargs_resource where tid = ?;''', [tid])
		kwargs_resource = {arg_key:self.get_resource(rid) for arg_key, rid in c.fetchall()}
		c.execute('''SELECT arg_key, fid FROM tasks_kwargs_file where tid = ?;''', [tid])
		kwargs_fileresource = {arg_key:self.get_fileresource(fid) for arg_key, fid in c.fetchall()}
		c.execute('''SELECT arg_key, pid FROM tasks_kwargs_pipe where tid = ?;''', [tid])
		kwargs_pipe = {arg_key:self.get_pipe(pid) for arg_key, pid in c.fetchall()}
		kwargs = {**kwargs_json, **kwargs_resource, **kwargs_fileresource, **kwargs_pipe}
		
		c.execute('''SELECT rid FROM tasks_returnvalue where tid = ?;''', [tid])
		return_values = [self.get_resource(rid) for rid, in c.fetchall()]
		
		c.execute('''SELECT fid FROM tasks_outputfiles where tid = ? ORDER BY forder ASC;''', [tid])
		output_files = [self.get_fileresource(fid) for fid, in c.fetchall()]
		
		r = results[0]
		tid, pid, begin_time, end_time, description = (
			r[0], 
			r[1],
			dateutil.parser.isoparse(r[2]),
			dateutil.parser.isoparse(r[3]),
			r[4]
		)
		tags = set(tag_value for tag_value, in tags_results)
		info = {k:v for k, v in info_results}
		
		if tid in self.tasks_db:
			task = self.tasks_db[tid]
			task.tid=tid
			task.pid=pid
			task.args=args
			task.kwargs=kwargs
			task.return_values=return_values
			task.output_files=output_files
			task.begin_time=begin_time
			task.end_time=end_time
			task.description=description
			task.tags=tags
			task.info=info
		else:
			task = Task(tid, pid, args, kwargs, return_values, output_files, begin_time, end_time, description, tags, info)
			self.tasks_db[task.tid] = task
		return task
	def get_unruntask(self, uid):
		if uid in self.unruntasks_db:
			return self.unruntasks_db[uid]
		raise Exception()
	def get_virtualresource(self, vid):
		if vid in self.virtualresources_db:
			return self.virtualresources_db[vid]
		raise Exception()

	# Methods for loading RMSEntry from DB to memory
	def parse_row_as_pipe_param(self, row):
		return (row[0], dill.loads(bytes.fromhex(row[1])), bool(row[2]), json.loads(row[3]), dill.loads(bytes.fromhex(row[4])) if row[4] is not None else None, bool(row[5]), row[6], row[7], json.loads(row[8])) 
	def parse_row_as_fileresource_param(self, row):
		return (row[0], row[1], row[2], row[3], row[4], json.loads(row[5]))
		fileresource = FileResource(row[0], row[1], row[2], row[3], row[4], json.loads(row[5]))
		self.fileresources_db[row[0]] = fileresource
		return fileresource 

	def parse_row_as_resource_param(self, row):
		return (row[0], row[1], bool(row[2]), row[3], row[4], json.loads(row[5]))
	def parse_row_as_task_param(self, row):
		convert_funcs = {"r": lambda s: self.get_resource(s),
						 "f": lambda s: self.get_fileresource(s),
						 "i": lambda s: self.get_pipe(s),
						 "p": json.loads}
		
		args = [convert_funcs[arg[0]](arg[1:]) for arg in json.loads(row[2])]
		kwargs = {k:convert_funcs[arg[0]](arg[1:]) for k, arg in json.loads(row[3]).items()}
		return_values = [self.get_resource(arg) for arg in json.loads(row[4])]
		output_files = [self.get_fileresource(arg) for arg in json.loads(row[5])]
		return (row[0], row[1], args, kwargs, return_values, output_files, dateutil.parser.isoparse(row[6]), dateutil.parser.isoparse(row[7]), row[8], row[9], json.loads(row[10]))

	def save_resource_content(self, resource):
		_dump_resource_content(self.resource_dump_dir, resource)
	
	def get_auto_fetch_resource_content_info(self, resources, allow_non_deterministic=False):
		resources = list(map(self._as_rmsobj, resources))
		dry_run_resources = set()
		dry_run_tasks = set()
		for resource in resources:
			if resource not in dry_run_resources and not resource.has_content:
				if resource.tid is None:
					raise Exception("Fail. Unable to retrieve resource tid.")
				task = self.get_task(resource.tid)
				pipe = self.get_pipe(task.pid)
				if not allow_non_deterministic and not pipe.is_deterministic:
					raise Exception("Fail. Unable to rerun pipe because it is a non-deterministic pipe.")
				if not len(task.output_files) == 0:
					raise Exception("Fail. Re-retrieving the content may overwrite certain files.")
				if any(not os.path.exists(f.file_path) for f in task.input_fileresources):
					raise Exception("Fail. Some files are missing")
				
				required_tasks, required_resources = self.get_auto_fetch_resource_content_info(task.input_resources, allow_non_deterministic=allow_non_deterministic)
				dry_run_tasks.update(required_tasks)
				dry_run_resources.update(required_resources)
				dry_run_tasks.add(task)
				dry_run_resources.add(resource)
		return dry_run_tasks, dry_run_resources
							
	def auto_fetch_resource_content(self, resources, allow_non_deterministic=False):
		'''
		Automatically fetch resource content if not dumped previously. Rerun all necessary tasks to obtain the resources
		'''
		resources = list(map(self._as_rmsobj, resources))
		for resource in resources:
			if not resource.has_content:
				if resource.tid is None:
					raise Exception("Fail. Unable to retrieve resource tid.")
				task = self.get_task(resource.tid)
				pipe = self.get_pipe(task.pid)
				if not allow_non_deterministic and not pipe.is_deterministic:
					raise Exception("Fail. Unable to rerun pipe because it is a non-deterministic pipe.")
				if not len(task.output_files) == 0:
					raise Exception("Fail. Re-retrieving the content may overwrite certain files.")
				self.auto_fetch_resource_content(task.input_resources, allow_non_deterministic=allow_non_deterministic)
				if any(not os.path.exists(f.file_path) for f in task.input_fileresources):
					raise Exception("Fail. Some files are missing")
				
				raw_return_value = self._run_pipe(pipe, task.args, task.kwargs)
				resource.content = raw_return_value
				self.fireRMSUpdateEvent([(RMSUpdateEvent.CONTENTCHANGE, resource.get_full_id())])
	####################
	# Users run a task #
	####################
	def compute_output_files(self, pipe, func_args, func_kwargs):
		output_file_paths = pipe.output_func(*func_args, **func_kwargs) if pipe.output_func is not None else []
		output_file_paths = [os.path.abspath(path) for path in output_file_paths] 
		return output_file_paths
	
	
	
	def register_finished_task(self, raw_return_value, begin_time, end_time, ba, pipe, 
							task_description="", task_tags=[], task_info={},
							resource_description="", resource_tags=[], resource_info={},
							fileresource_description="", fileresource_tags=[], fileresource_info={}):
		'''
		Register a finished task, assuming the task has completed successfully. 
		'''
		func_args, func_kwargs = self._ba_rms_to_func_args_kwargs(ba)
		
		# Check for output files
		output_file_paths = pipe.output_func(*func_args, **func_kwargs) if pipe.output_func is not None else []
		output_file_paths = [os.path.abspath(path) for path in output_file_paths] 
		problematic_paths = [p for p in output_file_paths if not os.path.exists(p)]
		if len(problematic_paths) > 0:
			print("Warning: the following output files are not detected:")
			print(problematic_paths)
			output_file_paths = [p for p in output_file_paths if os.path.exists(p)]
			
		return_value_rid = self._get_new_id()
		output_file_fids = [self._get_new_id() for _ in output_file_paths]
		tid = self._get_new_id()
		
		# Create resource, fileresource and task
		new_resources = [Resource(return_value_rid, tid, pipe.return_volatile, resource_description, resource_tags, resource_info, raw_return_value)]
		
		new_fileresources = [FileResource(fid, tid, file_path, _get_file_md5(file_path), fileresource_description, fileresource_tags, fileresource_info) for fid, file_path in zip(output_file_fids, output_file_paths)]

		task = Task(tid, pipe.pid, ba.args, ba.kwargs, new_resources, new_fileresources, begin_time, end_time, task_description, task_tags, task_info)
		
		# Find files that are overwritten
		# Even if deprecated, it should be marked overwritten
		old_fids = []
		for fileresource in new_fileresources:
			c = self.sql.cursor()
			c.execute('''SELECT fid FROM files where file_path = ?;''', [fileresource.file_path])
			old_fids.extend([fid for fid, in c.fetchall()])
		old_fids = [fid for fid in old_fids if "overwritten" not in self.get_fileresource(fid).info]
		
		###################################
		# Create and execute SQL commands #
		###################################
		sql_cmds = [] # A list of sql, parameters
		
		# Task
		sql_cmds.extend(_sql_insert_task(task))
		# Resource
		for resource in new_resources:
			sql_cmds.extend(_sql_insert_resource(resource))
		# FileResource
		for fileresource in new_fileresources:
			sql_cmds.extend(_sql_insert_fileresource(fileresource))
		
		# Overwrite files handling
		for fid in old_fids:
			sql_cmds.append(('INSERT INTO file_info(fid, info_key, info_value) VALUES(?,?,?)''', [fid, 'overwritten', end_time.isoformat()]))

		
		
		_sql_execute_commands(self.sql, sql_cmds)
		
		
		# Update internal db
		for resource in new_resources:
			self.resources_db[resource.rid] = resource
		for fileresource in new_fileresources:
			self.fileresources_db[fileresource.fid] = fileresource
		self.tasks_db[task.tid] = task
		
		# Fire RMS update event
		self.fireRMSUpdateEvent(list(map(lambda rmsobj: (RMSUpdateEvent.INSERT, rmsobj.get_full_id()), [task] + task.output_fileresources + task.output_resources)))
		
		# Return the task
		return task
		
	def _ba_rms_to_func_args_kwargs(self, ba):	
		return [self._obtain_rms_run_content(arg) for arg in ba.args], {k:self._obtain_rms_run_content(arg) for k, arg in ba.kwargs.items()}
	
	
	def run(self, pipe, args=(), kwargs={}, 
		task_description="", task_tags=set(), task_info={}, 
		resource_description="", resource_tags=set(), resource_info={},
		fileresource_description="", fileresource_tags=set(), fileresource_info={}):
		'''
		A normal way to run pipe with the given parameters.
		Register the run as a task, and register the output resources and fileresources.
		Return the resource or resources like normal function run.
		
		'''
		ba = _get_func_ba(pipe.func, args, kwargs)
		
		# Check for existing tasks. Avoid running tasks with exactly the same parameters
		prev_tasks = self.find_tasks_by_pipe_and_args(pipe.pid, ba.args, ba.kwargs)
		if len(prev_tasks) > 0:
			return prev_tasks[0].return_values[0]
		
		# Conversion to func_args and func_kwargs
		func_args, func_kwargs = self._ba_rms_to_func_args_kwargs(ba)
		
		# Generate output files, this step ensures that the output func works 
		output_files = self.compute_output_files(pipe, func_args, func_kwargs)
		
		# Add task info
		if self.scriptID is not None:
			task_info = {"scriptid":self.scriptID, **task_info}
		begin_time = datetime.datetime.now()
		raw_return_value = pipe.func(*func_args, **func_kwargs)
		end_time = datetime.datetime.now()
		
		task = self.register_finished_task(raw_return_value, begin_time, end_time, ba, pipe, 
										task_description, task_tags, task_info,
										resource_description, resource_tags, resource_info,
										fileresource_description, fileresource_tags, fileresource_info)
		
		return task.return_values[0]
		
	def _run_pipe(self, pipe, args=(), kwargs={}):
		'''
		Run pipe with the given parameters, but do not register anything
		Returns the normal return values of the pipe function.
		
		'''

		ba = _get_func_ba(pipe.func, args, kwargs)
		func_args, func_kwargs = self._ba_rms_to_func_args_kwargs(ba)
		return pipe.func(*func_args, **func_kwargs)

	def replace_virtualresource(self, virtualresource, resource):
		'''
		Replace a virtual resource with the desired resource.
		
		Should change it to use replace_unruntask_rmsobj 
		'''
		virtualresource.replacement = resource
		for unruntask in self.find_downstream_objs([virtualresource], 1):
			original_kv = list(unruntask.ba.arguments.items())
			for k, v in original_kv:
				if unruntask.ba.signature.parameters[k].kind == inspect.Parameter.VAR_POSITIONAL:
					unruntask.ba.arguments[k] = [resource if (isinstance(i, RMSEntry) and i is virtualresource) else i for i in v]
				elif unruntask.ba.signature.parameters[k].kind == inspect.Parameter.VAR_KEYWORD:
					unruntask.ba.arguments[k] = {ik:(resource if (isinstance(i, RMSEntry) and i is virtualresource) else i) for ik, i in v.items()}
				else:
					if isinstance(v, RMSEntry):
						if v is virtualresource:
							unruntask.ba.arguments[k] = resource
		self.delete(virtualresource.get_full_id())
		
	def replace_unruntask(self, unruntask, task):
		'''
		Update all associated virtual resources  
		'''
		if unruntask.return_values is not None:
			if len(task.return_values) == len(unruntask.return_values):
				for vr, r in zip(unruntask.return_values, task.return_values):
					self.replace_virtualresource(vr, r)
		if unruntask.output_files is not None:
			if len(task.output_files) == len(unruntask.output_files):
				for vr, r in zip(unruntask.output_files, task.output_files):
					self.replace_virtualresource(vr, r)
		
		unruntask.replacement = task
		self.delete(unruntask.get_full_id())

	def run_unruntask(self, unruntask):
		pipe = self.get_pipe(unruntask.pid)
		ba = unruntask.ba
		prev_tasks = self.find_tasks_by_pipe_and_args(pipe.pid, ba.args, ba.kwargs)
		if len(prev_tasks) > 0:
			print("The task has been done before.")
			task = prev_tasks[0]
		else:
			func_args, func_kwargs = self._ba_rms_to_func_args_kwargs(ba)
			
			begin_time = datetime.datetime.now()
			raw_return_value = pipe.func(*func_args, **func_kwargs)
			end_time = datetime.datetime.now()
		
			task = self.register_finished_task(raw_return_value, begin_time, end_time, ba, pipe, 
											unruntask.task_description, unruntask.task_tags, unruntask.task_info,
											unruntask.resource_description, unruntask.resource_tags, unruntask.resource_info,
											unruntask.fileresource_description, unruntask.fileresource_tags, unruntask.fileresource_info)
		
		self.replace_unruntask(unruntask, task)
		return task.return_values[0]
	
	def run_unruntask_chain(self, unruntask):
		for vr in unruntask.input_virtualresources:
			prev_unruntasks = self.find_upstream_objs([vr], 1)
			if len(prev_unruntasks) > 1:
				assert False
			if len(prev_unruntasks) == 0:
				print("Cannot fetch virtual task: " + vr.get_id())
			prev_unruntask = next(iter(prev_unruntasks))
			self.run_unruntask_chain(prev_unruntask)
		if len(unruntask.input_virtualresources) > 0:
			raise ResourceNotReadyException()
		
		
		pipe = self.get_pipe(unruntask.pid)
		ba = unruntask.ba
		prev_tasks = self.find_tasks_by_pipe_and_args(pipe.pid, ba.args, ba.kwargs)
		if len(prev_tasks) > 0:
			print("The task has been done before.")
			task = prev_tasks[0]
		else:
			func_args, func_kwargs = self._ba_rms_to_func_args_kwargs(ba)
			
			begin_time = datetime.datetime.now()
			raw_return_value = pipe.func(*func_args, **func_kwargs)
			end_time = datetime.datetime.now()
		
			task = self.register_finished_task(raw_return_value, begin_time, end_time, ba, pipe, 
											unruntask.task_description, unruntask.task_tags, unruntask.task_info,
											unruntask.resource_description, unruntask.resource_tags, unruntask.resource_info,
											unruntask.fileresource_description, unruntask.fileresource_tags, unruntask.fileresource_info)
		
		self.replace_unruntask(unruntask, task)

		return task.return_values[0]
		
	
	def create_unruntask(self, pid, args=[], kwargs={}, return_values=[], output_files=[],
						task_description="", task_tags=set(), task_info={},
						resource_description="", resource_tags=set(), resource_info={}, 
						fileresource_description="", fileresource_tags=set(), fileresource_info={}
						):
		uid = self._get_new_id()
		pipe = self.get_pipe(pid)
		ba = _get_func_ba(pipe.func, args, kwargs, partial=True)
		ut = UnrunTask(uid, pid, ba, return_values, output_files,
					task_description, task_tags, task_info,
					resource_description, resource_tags, resource_info,
					fileresource_description, fileresource_tags, fileresource_info
					)
		
				
		self.unruntasks_db[uid] = ut
		return ut
	def create_unruntask_from_task(self, task, return_values=[], output_files=[]):
		uid = self._get_new_id()
		ba = _get_func_ba(self.get_pipe(task.pid).func, task.args, task.kwargs, partial=True)
		unruntask = UnrunTask(uid, task.pid, ba, return_values, output_files)
		self.unruntasks_db[uid] = unruntask
		return unruntask
	def create_virtualresource(self):
		vid = self._get_new_id()
		vr = VirtualResource(vid, None, None, None, None, None)
		self.virtualresources_db[vid] = vr
		return vr
	def create_unruntask_chain(self, args):
		rmsobjs = list(map(self._as_rmsobj, args))
		
		newobjs = []
		
		g = self.construct_digraph_from_rmsids(rmsobjs)
		virtual_rmsobjs_dict = {} 
		for rmsid in nx.topological_sort(g):
			rmsobj = self.get(rmsid)
			if rmsid[0] == RMSEntryType.RESOURCE:
				vr = self.create_virtualresource()
				virtual_rmsobjs_dict[rmsobj] = vr
			elif rmsid[0] == RMSEntryType.FILERESOURCE:
				vr = self.create_virtualresource()
				virtual_rmsobjs_dict[rmsobj] = vr
			else:
				pass
		newobjs.extend(virtual_rmsobjs_dict.values())
		
		
		for rmsid in nx.topological_sort(g):
			if rmsid[0] == RMSEntryType.TASK:
				rmsobj = self.get(rmsid)
				# Update return values and output_files
				return_values = []
				output_files = []
				for r in rmsobj.return_values:
					if r in virtual_rmsobjs_dict:
						return_values.append(virtual_rmsobjs_dict[r])
					else:
						vr = self.create_virtualresource()
						newobjs.append(vr)
						return_values.append(vr)
				for r in rmsobj.output_files:
					if r in virtual_rmsobjs_dict:
						output_files.append(virtual_rmsobjs_dict[r])
					else:
						vr = self.create_virtualresource()
						newobjs.append(vr)
						output_files.append(vr)
				
				unruntask = self.create_unruntask_from_task(rmsobj, output_files=output_files, return_values=return_values)
				original_kv = list(unruntask.ba.arguments.items())
				for k, v in original_kv:
					if unruntask.ba.signature.parameters[k].kind == inspect.Parameter.VAR_POSITIONAL:
						unruntask.ba.arguments[k] = [virtual_rmsobjs_dict[i] if (isinstance(i, RMSEntry) and i in virtual_rmsobjs_dict) else i for i in v]
					elif unruntask.ba.signature.parameters[k].kind == inspect.Parameter.VAR_KEYWORD:
						unruntask.ba.arguments[k] = {ik:(virtual_rmsobjs_dict[i] if (isinstance(i, RMSEntry) and i in virtual_rmsobjs_dict) else i) for ik, i in v.items()}
					else:
						if isinstance(v, RMSEntry):
							if v in virtual_rmsobjs_dict:
								unruntask.ba.arguments[k] = virtual_rmsobjs_dict[v]
								
				newobjs.append(unruntask)
			else:
				pass
		
		self.fireRMSUpdateEvent([(RMSUpdateEvent.INSERT, rmsobj.get_full_id()) for rmsobj in newobjs])
		return newobjs
					
	
	def mark_deprecated(self, rmsobj, mark_downstream=True):
		'''
		Mark a rmsobj as deprecated. 
		All downstream rmsobjs will be marked as deprecated too.
		
		
		'''
		target_rmsobjs = [rmsobj]
		if mark_downstream:
			target_rmsobjs.extend(self.find_downstream_objs([rmsobj]))
		sql_cmds = []
		cur_time = datetime.datetime.now()
		for rmsobj in target_rmsobjs:
			if rmsobj.get_type() == RMSEntryType.FILERESOURCE:
				fid = rmsobj.get_id()
				sql_cmds.append(('INSERT INTO file_info(fid, info_key, info_value) VALUES(?,?,?)''', [fid, 'deprecated', cur_time.isoformat()]))
			elif rmsobj.get_type() == RMSEntryType.RESOURCE:
				rid = rmsobj.get_id()
				sql_cmds.append(('INSERT INTO resource_info(rid, info_key, info_value) VALUES(?,?,?)''', [rid, 'deprecated', cur_time.isoformat()]))
			elif rmsobj.get_type() == RMSEntryType.TASK:
				tid = rmsobj.get_id()
				sql_cmds.append(('INSERT INTO task_info(tid, info_key, info_value) VALUES(?,?,?)''', [tid, 'deprecated', cur_time.isoformat()]))
			else:
				raise Exception() 
		_sql_execute_commands(self.sql, sql_cmds)
		
		full_ids = []
		for rmsobj in target_rmsobjs:
			self.get(rmsobj.get_full_id(), refetch=True) # Refetch is required to update the copy in the memory
			full_ids.append(rmsobj.get_full_id())

		self.fireRMSUpdateEvent([(RMSUpdateEvent.MODIFY, full_id) for full_id in full_ids])
		
		
	##########################
	# Users edit an RMSEntry #
	##########################
	def update(self, full_id, **kwargs):
		'''
		This methods directly update the RMSEntry by field name in the database
		'''
		# Not designed for modifying IDs
		sql_cmds = [] 
		sql_cmds.append(_sql_update(full_id, **kwargs))
		_sql_execute_commands(self.sql, sql_cmds)
		new_rmsobj = self.get(full_id, refetch=True) # Refetch is required to update the copy in the memory
		self.fireRMSUpdateEvent([(RMSUpdateEvent.MODIFY, full_id)])
		return new_rmsobj
	
	def update_unruntask_arguments(self, full_id, updated_ba_arguments):
		self.get(full_id).ba.arguments.update(updated_ba_arguments)
		self.fireRMSUpdateEvent([(RMSUpdateEvent.MODIFY, full_id)])

	###########################
	# Users delete RMSEntries #
	###########################
	
	def _get_depenedent_rmsids(self, rmsids):
		dependent_rmsids = set()
		for rmsid in rmsids:
			if rmsid[0] == RMSEntryType.RESOURCE:
				dependent_rmsids.update([task.get_full_id() for task in self.find_tasks_by_io("io", rids=[rmsid[1]])])
			elif rmsid[0] == RMSEntryType.FILERESOURCE:
				dependent_rmsids.update([task.get_full_id() for task in self.find_tasks_by_io("io", fids=[rmsid[1]])])
			elif rmsid[0] == RMSEntryType.TASK:
				dependent_rmsids.update([rmsobj.get_full_id() for rmsobj in self.get_task(rmsid[1]).output_fileresources + self.get_task(rmsid[1]).output_resources])
			elif rmsid[0] == RMSEntryType.PIPE:
				dependent_rmsids.update([task.get_full_id() for task in self.find_tasks_by_pipe([rmsid[1]])])
# 			elif rmsid[0] == RMSEntryType.UNRUNTASK:
# 				pass
# 			elif rmsid[0] == RMSEntryType.VIRTUALRESOURCE:
# 				pass
			else:
				raise Exception("The type is not supported: " + str(rmsid[0]))
		return dependent_rmsids
	def _get_delete_sql_cmds(self, rmsids):
		
		sql_cmds = []
		for rmsid in rmsids:
			if rmsid[0] == RMSEntryType.RESOURCE:
				sql_cmds.append([f"DELETE FROM resources WHERE rid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM resource_info WHERE rid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM resource_tags WHERE rid=?", [rmsid[1]]])
			elif rmsid[0] == RMSEntryType.FILERESOURCE:
				sql_cmds.append([f"DELETE FROM files WHERE fid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM file_info WHERE fid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM file_tags WHERE fid=?", [rmsid[1]]])
	
			elif rmsid[0] == RMSEntryType.TASK:
				sql_cmds.append([f"DELETE FROM tasks WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_args_json WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_args_pipe WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_args_file WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_args_resource WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_kwargs_json WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_kwargs_pipe WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_kwargs_file WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_kwargs_resource WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_returnvalue WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM tasks_outputfiles WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM task_info WHERE tid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM task_tags WHERE tid=?", [rmsid[1]]])
			elif rmsid[0] == RMSEntryType.PIPE:
				sql_cmds.append([f"DELETE FROM pipes WHERE pid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM pipe_info WHERE pid=?", [rmsid[1]]])
				sql_cmds.append([f"DELETE FROM pipe_tags WHERE pid=?", [rmsid[1]]])
			else:
				raise Exception()

		return sql_cmds
	def delete(self, *rmsids):
		'''
		Delete an entry. Use it with care as the action cannot be undone.
		
		The delete function first looks for all dependencies of the current RMS objects to be deleted. 
		If any of the RMS object dependency breaks, an exception is raised
		
		Support for virtual RMS objects (UNRUNTASK / VIRTUALRESOURCE) may be problematic.
		'''
		virtual_rmsids = [rmsid for rmsid in rmsids if rmsid[0] == RMSEntryType.UNRUNTASK or rmsid[0] == RMSEntryType.VIRTUALRESOURCE]
		rmsids = [rmsid for rmsid in rmsids if not (rmsid[0] == RMSEntryType.UNRUNTASK or rmsid[0] == RMSEntryType.VIRTUALRESOURCE)]	
				
		rmsids = set(rmsids)
		dependent_rmsids = self._get_depenedent_rmsids(rmsids)
		if not dependent_rmsids.issubset(rmsids):
			raise Exception("Dependency of the following rmsids will break:\n" + "\n".join(["- " + str(rmsid) for rmsid in (dependent_rmsids - rmsids)]) + "\nwhere deleted rmsids are:\n" + "\n".join(["- " + str(rmsid) for rmsid in rmsids]))
		
		
		sql_cmds = []
		sql_cmds.extend(self._get_delete_sql_cmds(rmsids))
		_sql_execute_commands(self.sql, sql_cmds)
		for rmsid in rmsids:
			try: 
				if rmsid[0] == RMSEntryType.RESOURCE:
					self.resources_db.pop(rmsid[1])
				elif rmsid[0] == RMSEntryType.FILERESOURCE:
					self.fileresources_db.pop(rmsid[1])
				elif rmsid[0] == RMSEntryType.TASK:
					self.tasks_db.pop(rmsid[1])
				elif rmsid[0] == RMSEntryType.PIPE:
					self.pipes_db.pop(rmsid[1])
				else:
					raise Exception()
			except KeyError:
				pass
		for rmsid in virtual_rmsids:
			if rmsid[0] == RMSEntryType.UNRUNTASK:
				self.unruntasks_db.pop(rmsid[1])
			elif rmsid[0] == RMSEntryType.VIRTUALRESOURCE:
				self.virtualresources_db.pop(rmsid[1])
			else:
				raise Exception()
		self.fireRMSUpdateEvent(list(map(lambda rmsid: (RMSUpdateEvent.DELETE, rmsid), list(rmsids) + list(virtual_rmsids))))
		
		
	####################################################
	# Import
	####################################################
			
		
	def rmsimport(self, namespace=None, moduleobj=None, varname = None, altname=None, return_first_module=False):
		'''
		Perform a fake import
	
		'''
		def is_bound_method(func):
			return inspect.ismethod(func)
		pipe_results = self.sql.cursor().execute(f"SELECT pid FROM pipes;").fetchall()
		pipes = [self.get_pipe(pid) for pid, in pipe_results]
		
		if namespace is None:
			namespace = dict()
		if moduleobj is None:
			moduleobj = "builtins"

		# There are two ways to add bound_method. 
		# 1. If the class is already registered, set the bound method as the class attr
		# 2. If the class is not registered, set the class as another virtual module 
		bound_method_classes = defaultdict(list)
		for p in pipes:
			if inspect.ismethod(p.func):
				if inspect.isclass(p.func.__self__):
					bound_method_classes[p.func.__self__].append(p)
		
	
		# Import the module
		if varname is None:
			output_varnames = []
			output_pipes = []
			for pipe in pipes:
				if pipe.func.__module__ == moduleobj and not inspect.ismethod(pipe.func): #(inspect.isclass(pipe.func) or inspect.isfunction(pipe.func)):
					output_varnames.append(pipe.func.__name__)
					output_pipes.append(pipe)
			d = {v:p for v, p in zip(output_varnames, output_pipes)}
			if altname is None:
				vm = self.vm
				for i in moduleobj.split("."):
					if not hasattr(vm, i):
						setattr(vm, i, VirtualModule())
					vm = getattr(vm, i)
				for v, p in d.items():
					setattr(vm, v, p)
				
				# Deal with bound method that is not in output pipes
				for k, bound_methods in bound_method_classes.items():
					if k.__module__ in moduleobj:
						for p in output_pipes:
							if k is p.func:
								break
						else:
							i = k.__name__
							if not hasattr(vm, i):
								setattr(vm, i, VirtualModule())
							for bound_method in bound_methods:
								setattr(getattr(vm, i), bound_method.func.__name__, bound_method)
				first_vm_name = moduleobj.split(".")[0]
				namespace[first_vm_name] = getattr(self.vm, first_vm_name)
				if return_first_module:
					returnvalue = namespace[first_vm_name]
				else:
					returnvalue = vm
			else:
				# Deal with bound method that is not in output pipes
				to_add = {}
				for k, bound_methods in bound_method_classes.items():
					if k.__module__ in moduleobj:
						for p in output_pipes:
							if k is p.func:
								break
						else:
							i = k.__name__
							if i not in to_add:
								to_add[i] = VirtualModule()
							for bound_method in bound_methods:
								setattr(to_add[i], bound_method.func.__name__, bound_method)
				namespace[altname] = types.SimpleNamespace(**d, **to_add)
				returnvalue = namespace[altname]

				
		# Import the variable
		else:
			output_pipes = []
			# wild card case
			if varname == "*":
				tmp_returnvalues = {}
				for pipe in pipes:
					if pipe.func.__module__ == moduleobj and not inspect.ismethod(pipe.func):#(inspect.isclass(pipe.func) or inspect.isfunction(pipe.func)):
						namespace[pipe.func.__name__] = pipe
						output_pipes.append(pipe)
						tmp_returnvalues[pipe.func.__name__] = namespace[pipe.func.__name__]
				to_add = {}
				for k, bound_methods in bound_method_classes.items():
					if k.__module__ in moduleobj:
						for p in output_pipes:
							if k is p.func:
								break
						else:
							i = k.__name__
							if i not in to_add:
								to_add[i] = VirtualModule()
							for bound_method in bound_methods:
								setattr(to_add[i], bound_method.func.__name__, bound_method)
				for k, p in to_add.items():
					namespace[k] = p
					tmp_returnvalues[k] = namespace[k]
				if len(tmp_returnvalues) < 1:
					raise Exception(f"Cannot find anything to import in {moduleobj}")
				returnvalue = tmp_returnvalues
				
			# normal case
			else:
				tmp_returnvalues = [] 
				for pipe in pipes:
					if pipe.func.__module__ == moduleobj and not inspect.ismethod(pipe.func):#(inspect.isclass(pipe.func) or inspect.isfunction(pipe.func)):
						if pipe.func.__name__ == varname:
							if altname is None:
								altname = varname
							namespace[altname] = pipe
							output_pipes.append(pipe)
							tmp_returnvalues.append(pipe)
				to_add = {}
				
				for k, bound_methods in bound_method_classes.items():
					if k.__name__ == varname and k.__module__ in moduleobj:
						for p in output_pipes:
							if k is p.func:
								break
						else:
							i = k.__name__
							if i not in to_add:
								to_add[i] = VirtualModule()
							for bound_method in bound_methods:
								setattr(to_add[i], bound_method.func.__name__, bound_method)
				for k, p in to_add.items():
					namespace[k] = p
					tmp_returnvalues.append(namespace[k])
				if len(tmp_returnvalues) < 1:
					raise Exception(f"Cannot find {varname} in {moduleobj}")
				if len(tmp_returnvalues) > 1:
					assert False
				returnvalue = tmp_returnvalues[0]
				
		for pipe in output_pipes:
			if inspect.isclass(pipe.func):
				if pipe.func in bound_method_classes:
					for bound_method in bound_method_classes[pipe.func]:
						setattr(pipe, bound_method.func.__name__, bound_method)
							
		return returnvalue
	########################
	# Users exports RMS DB #
	########################
	
	def construct_digraph_from_rmsids(self, rmsobjs):
		rmsobjs = list(map(self._as_rmsobj, rmsobjs))
		g = nx.DiGraph()
		g.add_nodes_from(map(lambda rmsobj: rmsobj.get_full_id(), rmsobjs))
		for rmsobj in rmsobjs:
			# instead of finding downstream, finding upstream is much more efficient
			for upstream_rmsobj in self.find_upstream_objs([rmsobj], 1):
				if upstream_rmsobj.get_full_id() in g:
					g.add_edge(upstream_rmsobj.get_full_id(), rmsobj.get_full_id())
		return g
	
	def reconstruct_digraph(self, rids=[], fids=[], tids=[]):
		'''
		Reconstructs the pipeline that yield the rids and fids
		
		'''
		g = nx.DiGraph()
		all_rids = set(); all_rids.update(rids)
		all_fids = set(); all_fids.update(fids)
		all_tids = set(); all_tids.update(tids)
		resources = [self.get_resource(rid) for rid in rids]
		fileresources = [self.get_fileresource(fid) for fid in fids]
		tasks = [self.get_task(tid) for tid in tids]
		while len(resources) > 0 or len(fileresources) > 0 or len(tasks) > 0:
			if len(tasks) > 0:
				task = tasks.pop()
				g.add_node((task.get_type(), task.get_id()))
				for resource in task.input_resources:
					g.add_edge((resource.get_type(), resource.get_id()), (task.get_type(), task.get_id()))
					if resource.rid not in all_rids:
						resources.append(resource)
						all_rids.add(resource.rid)
				for fileresource in task.input_fileresources:
					g.add_edge((fileresource.get_type(), fileresource.get_id()), (task.get_type(), task.get_id()))
					if fileresource.fid not in all_fids:
						fileresources.append(fileresource)
						all_fids.add(fileresource.fid)
			else:
				if len(resources) > 0:
					resource = resources.pop()
					if resource.tid is None:
						continue
					task = self.get_task(resource.tid)
					cid = (resource.get_type(), resource.get_id())
				elif len(fileresources) > 0:
					fileresource = fileresources.pop()
					if fileresource.tid is None:
						continue
					task = self.get_task(fileresource.tid)
					cid = (fileresource.get_type(), fileresource.get_id())
				else:
					raise Exception()
				g.add_node(cid)
				g.add_edge((task.get_type(), task.get_id()), cid)
				if task.tid not in all_tids:
					tasks.append(task)
					all_tids.add(task.tid)
		return g

	###
	# Currently the codes conversion to normal codes, RMS codes, function are all based on similar mechanism.
	# I need to edit it   
	# 
	###


		
	def convert_to_normal_codes(self, rids=[], fids=[], tids=[]):
		import textwrap
		
		g = self.reconstruct_digraph(rids, fids, tids)
		graph_node_mapping = {}
		
		next_dummy_var_id = 1 
		next_dummy_func_id = 1
		rid_var_dict = {}
		funcs = dict()
		redefined_pipes = {}
		def obtain_pipename(pipe):
			nonlocal next_dummy_func_id
			if pipe.func.__module__ == "__main__":
				if pipe.pid not in redefined_pipes:
					if pipe.func.__name__ == "<lambda>":
						new_name = "f"
					else:
						new_name = pipe.func.__name__
					while new_name in funcs:
						new_name = new_name + str(next_dummy_func_id)
						next_dummy_func_id += 1
					funcs[new_name] = (dill.dumps(pipe.func).hex(), pipe.info["sourcecode"] if "sourcecode" in pipe.info else None)
					redefined_pipes[pipe.pid] = new_name
				pipename = redefined_pipes[pipe.pid]
			elif pipe.func.__module__ == "builtins":
				pipename = pipe.func.__name__
			else:
				modules.add(pipe.func.__module__)
				pipename = pipe.func.__module__ + "." + pipe.func.__name__
			return pipename
		
		def convert_arg(arg):
			if isinstance(arg, Resource):
				return rid_var_dict[arg.rid]
			elif isinstance(arg, FileResource):
				return json.dumps(arg.file_path)
			elif isinstance(arg, Pipe):
				#return f'rmsp.get_pipe("{arg.pid}")'
				return obtain_pipename(arg)
			else:
				return repr(arg)
				#return json.dumps(arg)
		codes = []
		modules = set()
		for node in nx.topological_sort(g):
			if node[0] == RMSEntryType.TASK:
				task = self.get_task(node[1])
				# Add description as comment
				if task.description is not None:
					for description_line in task.description.splitlines():
						codes.append("# " + description_line)
				
				# Add actual code 
				args = list(map(convert_arg, task.args))
				kwargs = {k:convert_arg(arg) for k, arg in task.kwargs.items()}
				func_param = ', '.join(args + [k + "=" + arg for k, arg in kwargs.items()])

				for resource in task.return_values:
					rid_var_dict[resource.rid] = "v" + str(next_dummy_var_id)
					next_dummy_var_id += 1
					
				left_str = ", ".join([rid_var_dict[resource.rid] for resource in task.return_values])
				pipe = self.get_pipe(task.pid)
				pipename = obtain_pipename(pipe)
				codes.append(f"{left_str} = {pipename}({func_param})")
				
				graph_node_mapping[node] = textwrap.fill(f"{left_str} = {pipename}({func_param})", 60)
		if len(funcs) > 0:
			modules.add("dill")
		
		pre_codes = []
		pre_codes.append("# The codes are auto-generated from the RMS. Please refer to the original pipeline for details")
		pre_codes.append("###########")
		pre_codes.append("# Imports #")
		pre_codes.append("###########")
		for module in modules:
			pre_codes.append(f"import {module}")
		pre_codes.append("")
		pre_codes.append("########################")
		pre_codes.append("# Functions definition #")
		pre_codes.append("########################")
		for func_name, (hex_str, sourcecode) in funcs.items():
			pre_codes.append(f"{func_name} = dill.loads(bytes.fromhex(\"{hex_str}\"))")
			if sourcecode is not None:
				pre_codes.append("\n".join(map(lambda s: "# " + s, sourcecode.split("\n"))))
		pre_codes.append("")
		pre_codes.append("#############")
		pre_codes.append("# Pipelines #")
		pre_codes.append("#############")
		
		
		
		graph_node_mapping.update({(RMSEntryType.RESOURCE, k):v for k,v in rid_var_dict.items()})
		graph_node_mapping.update({(RMSEntryType.PIPE, k):v for k,v in redefined_pipes.items()})
		graph_node_mapping.update({node:self.get(node).file_path for node in g.nodes if node[0] == RMSEntryType.FILERESOURCE})
		return "\n".join(pre_codes + codes), g, graph_node_mapping
	
		
	def convert_to_rms_codes(self, rids=[], fids=[], tids=[]):
		g = self.reconstruct_digraph(rids, fids, tids)
# 	def convert_to_rms_codes(self, rmsids):
# 		g = self.construct_digraph_from_rmsids(rmsids)
		next_dummy_var_id = 1 
		next_dummy_func_id = 1
		rid_var_dict = {}
		def convert_arg(arg):
			if isinstance(arg, Resource):
				return rid_var_dict[arg.rid]
			elif isinstance(arg, FileResource):
				return f'rmsp.file_from_path("{arg.file_path}")'#f'rmsp.get_fileresource("{arg.fid}")'
			elif isinstance(arg, Pipe):
				return f'rmsp.get_pipe("{arg.pid}")'
			else:
# 				return str(arg)				
				return repr(arg)
		codes = []
		modules = set()
		funcs = dict()
		redefined_pipes = {}
		for node in nx.topological_sort(g):
			if node[0] == RMSEntryType.TASK:
				task = self.get_task(node[1])
				# Add description as comment
				if task.description is not None:
					for description_line in task.description.splitlines():
						codes.append("# " + description_line)
				
				for resource in task.input_resources:
					if resource.rid not in rid_var_dict:
						print("Add", resource.rid )
						rid_var_dict[resource.rid ] = "v" + str(next_dummy_var_id)
						next_dummy_var_id += 1
						codes.append(f"{rid_var_dict[resource.rid]} = rmsp.get_resource(\"{resource.rid}\")")

				# Add actual code 
				args = list(map(convert_arg, task.args))
				kwargs = {k:convert_arg(arg) for k, arg in task.kwargs.items()}
				func_param = ', '.join(args + [k + "=" + arg for k, arg in kwargs.items()])

				for resource in task.return_values:
					rid_var_dict[resource.rid] = "v" + str(next_dummy_var_id)
					next_dummy_var_id += 1
				left_str = ", ".join([rid_var_dict[resource.rid] for resource in task.return_values])
				left_resources = [resource.rid for resource in task.return_values]
				codes.append(f"{left_str} = rmsp.get_pipe(\"{task.pid}\")({func_param})" + f" # {self.get_pipe(task.pid).func_name}: {','.join(left_resources)}")
			elif node[0] == RMSEntryType.RESOURCE:
				print("GG", node[1])
				if node[1] not in rid_var_dict:
					print("Add", node[1])
					rid_var_dict[node[1]] = "v" + str(next_dummy_var_id)
					next_dummy_var_id += 1
					codes.append(f"{rid_var_dict[node[1]]} = rmsp.get_resource(\"{node[1]}\")")
			else:
				print(node)
		if len(funcs) > 0:
			modules.add("dill")
			
		print("# The codes are auto-generated from the RMS. Please refer to the original pipeline for details")
		print("###########")
		print("# Imports #")
		print("###########")
		print(f"from rpms import newrms")
		print(f'rmsp = newrms.ResourceManagementSystem("/local/storage/kl945/RMS/New_RMS.db", "/local/storage/kl945/RMS/")')
		print()
		print("#############")
		print("# Pipelines #")
		print("#############")
		for code in codes:
			print(code)
		print()
		
	def wrap_as_function(self, fname, rids=[], fids=[], tids=[]):
		g = self.reconstruct_digraph(rids, fids, tids)
	
		next_dummy_var_id = 1 
		next_dummy_func_id = 1
		rid_var_dict = {}
		parameter_var_dict = {}
		file_dict = {}
		
		
		def convert_arg(arg):
			if isinstance(arg, Resource):
				return rid_var_dict[arg.rid]
			elif isinstance(arg, FileResource):
				return file_dict[arg.get_id()]
			elif isinstance(arg, Pipe):
				return f'rmsp.get_pipe("{arg.pid}")'
			else:
				pp = "z" + str(len(parameter_var_dict) + 1)
				parameter_var_dict[pp] = arg
				return pp
				#return repr(arg)
			
		for node, in_degree in g.in_degree:
			if in_degree == 0:
				if node[0] is RMSEntryType.FILERESOURCE:
					file_dict[node[1]] = "y" + str(len(file_dict) + 1)
					pass
				elif node[0] is RMSEntryType.RESOURCE:
					pass
				elif node[0] is RMSEntryType.TASK:
					pass
				else:
					raise Exception()
				
		codes = []
		modules = set()
		funcs = dict()
		redefined_pipes = {}
		for node in nx.topological_sort(g):
			if node[0] == RMSEntryType.TASK:
				task = self.get_task(node[1])
				# Add description as comment
				if task.description is not None:
					for description_line in task.description.splitlines():
						codes.append("# " + description_line)
	
				# Add actual code 
				args = list(map(convert_arg, task.args))
				kwargs = {k:convert_arg(arg) for k, arg in task.kwargs.items()}
				func_param = ', '.join(args + [k + "=" + arg for k, arg in kwargs.items()])
	
				for resource in task.return_values:
					rid_var_dict[resource.rid] = "v" + str(next_dummy_var_id)
					next_dummy_var_id += 1
				for fileresource in task.output_fileresources:
					file_dict[fileresource.fid] = "y" + str(len(file_dict) + 1)
				left_str = ", ".join([rid_var_dict[resource.rid] for resource in task.return_values])
				pipe = self.get_pipe(task.pid)
				if pipe.func.__module__ == "__main__":
					if pipe.pid not in redefined_pipes:
						if pipe.func.__name__ == "<lambda>":
							new_name = "f"
						else:
							new_name = pipe.func.__name__
						while new_name in funcs:
							new_name = new_name + str(next_dummy_func_id)
							next_dummy_func_id += 1
						funcs[new_name] = dill.dumps(pipe.func).hex()
						redefined_pipes[pipe.pid] = new_name
					pipename = redefined_pipes[pipe.pid]
				elif pipe.func.__module__ == "builtins":
					pipename = pipe.func.__name__
				else:
					modules.add(pipe.func.__module__)
					pipename = pipe.func.__module__ + "." + pipe.func.__name__
				codes.append(f"{left_str} = {pipename}({func_param})")
				
				
				if pipe.output_func is not None:
					left_str = ", ".join([file_dict[fileresource.fid] for fileresource in task.output_fileresources])
					modules.add(pipe.output_func.__module__)
					pipename = pipe.output_func.__module__ + "." + pipe.output_func.__name__
					codes.append(f"{left_str} = {pipename}({func_param})")
					
	
		if len(funcs) > 0:
			modules.add("dill")
	
		print("# The codes are auto-generated from the RMS. Please refer to the original pipeline for details")
		print("###########")
		print("# Imports #")
		print("###########")
		for module in modules:
			print(f"import {module}")
		print()
		print("########################")
		print("# Functions definition #")
		print("########################")
		for func_name, hex_str in funcs.items():
			print(f"{func_name} = dill.loads(bytes.fromhex(\"{hex_str}\"))")
		print()
		print("#############")
		print("# Pipelines #")
		print("#############")
		file_var_names = [f"{file_var_name}={repr(self.get_fileresource(fid).file_path)}" for fid, file_var_name in file_dict.items()]
		var_names = [f"{pp}={repr(arg)}" for pp, arg in parameter_var_dict.items()]
		func_input_parameter_str = ", ".join(file_var_names + var_names)
		print(f"def {fname}({func_input_parameter_str}):")
		for code in codes:
			print("\t" + code)
		print()
							

	
######################################################
# Experimental template
######################################################
class TemplateTask():
	def __init__(self, tid, func, output_func, return_volatile, arguments, return_values, output_files):
		self.tid = tid
		self.func = func
		self.output_func = output_func
		self.return_volatile = return_volatile
		self.arguments = arguments
		self.return_values = return_values
		self.output_files = output_files
class TemplateResource():
	def __init__(self, tid):
		self.tid = tid
class TemplateFunction():
	def __init__(self, func):
		self.func = func


def create_wrapped_func_codes(name, funcs, return_str):
	'''
	Create a function on the fly, by specifying the funcs. Function results are named as f{fidx}_r{ridx}. 
	:param funcs: A list of tuple (func, input_dict, output_value_count), where input_dict represents direct input into the function. 
	Keys of input_dict are argument names, and values are the results from other functions (e.g. f0_r0)
	
	'''
	def convert_param_default(d):
		if isinstance(d, str):
			return f"\"{d}\""
		elif isinstance(d, int):
			return f"{d}"
		elif isinstance(d, float):
			return f"{d}"
		else:
			return f"dill.loads(bytes.fromhex(\"{dill.dumps(parameter.default).hex()}\"))"
	def empty_default_string(parameter):
		if parameter.kind == inspect.Parameter.VAR_POSITIONAL:
			return "=[]"
		elif parameter.kind == inspect.Parameter.VAR_KEYWORD:
			return "={}"
		else:
			return ""
	# Parse parameters
	pipe_param_names = set()
	values_map = [dict() for _ in range(len(funcs))]
	pipe_parameters = []
	for func_idx, (func, input_dict, output_value_count) in enumerate(funcs):
		try:
			ba = inspect.signature(func)
		except ValueError:
			s = inspect.signature(func.__init__)
			ba = s.replace(parameters=[p for p in s.parameters.values() if p.name != "self"])
		parameters = ba.parameters
		for param_name in parameters:
			new_param_name = param_name
			param_dup_id = 0
			while new_param_name in pipe_param_names:
				param_dup_id += 1
				new_param_name = param_name + "_" + str(param_dup_id)
			pipe_param_names.add(new_param_name)
			if param_name in input_dict:
				values_map[func_idx][param_name] = input_dict[param_name] #f"f{input_dict[param_name][0]}_r{input_dict[param_name][1]}" 
			else:
				values_map[func_idx][param_name] = new_param_name
			if param_name in input_dict:
				continue
			if new_param_name == param_name:
				pipe_parameters.append(parameters[param_name])
			else:
				pipe_parameters.append(parameters[param_name].replace(name=new_param_name))
		
	modules = set()
	codes = []
	# Create the codes
	for func_idx, (func, input_dict, output_value_count) in enumerate(funcs):
		if func.__module__ == "builtins":
			pipename = func.__name__
		else:
			modules.add(func.__module__)
			pipename = func.__module__ + "." + func.__name__
		try:
			ba = inspect.signature(func)
		except ValueError:
			s = inspect.signature(func.__init__)
			ba = s.replace(parameters=[p for p in s.parameters.values() if p.name != "self"])
		parameters = ba.parameters
		args = []
		kwargs = {}
		for param_name, parameter in parameters.items():
			if parameter.kind == inspect.Parameter.POSITIONAL_ONLY:
				args.append(values_map[func_idx][param_name])
			elif parameter.kind == inspect.Parameter.VAR_POSITIONAL:
				args.append("*" + values_map[func_idx][param_name])
			elif parameter.kind == inspect.Parameter.VAR_KEYWORD:
				args.append("**" + values_map[func_idx][param_name])
			else:
				kwargs[param_name] = values_map[func_idx][param_name]
		func_param = ', '.join(args + [k + "=" + arg for k, arg in kwargs.items()])
		left_str = ", ".join(f"f{func_idx}_r{output_value_idx}" for output_value_idx in range(output_value_count))
		codes.append(f"{left_str} = {pipename}({func_param})")
	
	method_params_str = ",".join([parameter.name + (empty_default_string(parameter) if parameter.default == inspect._empty else ("=" + convert_param_default(parameter.default))) for parameter in sorted(pipe_parameters, key = lambda param: param.kind)])
	
	func_setup_codes = []
	func_setup_codes.append(f"def {name}({method_params_str}):")
	modules.add("dill")
	for module in modules:
		func_setup_codes.append(f"\timport {module}")
	for code in codes:
		func_setup_codes.append("\t" + code)
	func_setup_codes.append("\treturn " + return_str)
	final_codes = "\n".join(func_setup_codes)
	return final_codes
