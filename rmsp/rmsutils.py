'''
Created on Aug 7, 2021

@author: kl945
'''

import os
import shutil
import glob
import uuid
import sqlite3
from .rmscore import ResourceManagementSystem
import re
import types
import tempfile
import zipfile
from collections import Counter

##############
# Virtual RMS
##############
def create_virtual_rms():
	'''
	Create a fake RMS that does not use any database.
	The virtual system could be utilized to test you codes before actually run and record your jobs in the actual RMS database   
	'''
	rms = types.SimpleNamespace()
	rms.database_id = "VirtualRMSDatabase" 
	rms.file_from_path = lambda a: a
	rms.register_file = lambda a: a
	rms.register_pipe = lambda f, *args, **kwargs: f
	def f(moduleobj, varname=None, altname=None, return_first_module=True):
		as_statement = "" if altname is None else " as " + altname
			
		if varname is None:
			
			exec("import " + moduleobj + as_statement)
			if altname is not None:
				return locals()[altname]
			else:
				if return_first_module:
					return locals()[moduleobj.split(".")[0]]
				else:
					exec("dummy = " + moduleobj)
					return locals()["dummy"]
		else:
			exec("from " + moduleobj + " import " + varname + as_statement)
			if altname is not None:
				return locals()[altname]
			else:
				return locals()[varname]
	rms.rmsimport = f
	return rms
	
#######
# Imports
#######

def convert_import_str(import_str, rmsvar="rms"):
	'''
	Convert normal python imports into RMS imports
	'''
	pattern = re.compile(r"^(?:from\s+(.+)\s+)?import\s+([^\s,]+(?:\s*,\s*[^\s,]+)*)(?:\s+as\s+(.+))?$")
	lines = import_str.split("\n")
	failed_lines = []
	converted_lines = []
	for line in lines:
		line = line.strip()
		if len(line) == 0:
			continue
		m = pattern.match(line)
		if m is None:
			failed_lines.append(line)
			continue
		g1, g2, g3 = m.groups()
		altname=g3
	
		
		if g1 is None:
			# Import module
			if altname is None:
				converted_lines.append(f"{g2.split('.')[0]} = {rmsvar}.rmsimport(moduleobj='{g2}', return_first_module=True)")
			else:
				converted_lines.append(f"{altname} = {rmsvar}.rmsimport(moduleobj='{g2}', return_first_module=False)")
		else:
			for variable in g2.split(","):
				variable = variable.strip()
				converted_lines.append(f"{variable} = {rmsvar}.rmsimport(moduleobj='{g1}', varname='{variable}')")
	return "\n".join(converted_lines)
#####################################
# Backup and Restore
#####################################	

def backup(dst, dbfile, resource_dump_dir, keep_all_files=False):
	'''
	Note that RMS does not support backuping the running environment. 
	
	Here RMS will backup (1) the database itself; (2) Scripts (?); (3) files that are registered.
	
	'''
	rms = ResourceManagementSystem(dbfile, resource_dump_dir)
	
	
	shutil.copy2(dbfile, f"{dst}/{os.path.basename(dbfile)}")
	files_no_longer_exist = []
	n = 0
	os.makedirs(f"{dst}/files/", exist_ok=True)
	if keep_all_files:
		fr_results = rms.sql.cursor().execute(f'''SELECT fid FROM files;''').fetchall()
	else:
		fr_results = rms.sql.cursor().execute(f'''SELECT fid FROM files where tid IS NULL;''').fetchall()
	for fid, in fr_results:
		fp = rms.get_fileresource(fid).file_path
		if not os.path.exists(fp):
			files_no_longer_exist.append(fp)
			continue
		
		try:
			shutil.copy2(fp, f"{dst}/files/{fid}")
		except:
			files_no_longer_exist.append(fp)
		
		# Support MD5 check
	print(f"Number of backuped files: {n}")
	print(f"Number of missing files: {len(files_no_longer_exist)}")
	
				
def restore(src, dst, path_mode="original"):
	'''
	If path_mode is not original, files may not be auto-generated from the same pipeline 
	(give that output path belongs to parameters)
	
	'''
	dbfiles = glob.glob(f"{src}/*.db")
	if len(dbfiles) != 1:
		raise Exception("Cannot find DB file to restore")
	dbfile = dbfiles[0]
	new_dbfile = f"{dst}/{os.path.basename(dbfile)}" 
	if os.path.exists(new_dbfile):
		raise Exception("A DB file with the same name already exists")
	
	
	
	all_files = glob.glob(f"{src}/files/*")
	rms = ResourceManagementSystem(dbfile, dst)
	for f in all_files:
		fid = os.path.basename(f)
		original_path = rms.get_fileresource(fid).file_path
		if os.path.exists(original_path):
			raise Exception(f"File exists for {original_path}")
	
	
	shutil.copy2(dbfile, new_dbfile)
	rms = ResourceManagementSystem(f"{dst}/{os.path.basename(dbfile)}", dst)
	for f in all_files:
		fid = os.path.basename(f)
		original_path = rms.get_fileresource(fid).file_path
		d = os.path.dirname(original_path)
		os.makedirs(d, exist_ok=True)
		shutil.copy2(f, original_path)
		
##############################################
# SQL database function
############################################	
def create_new_db(f):
	sql = sqlite3.connect(f)
	cmds = [
"""CREATE TABLE IF NOT EXISTS metainfo (
	infokey text NOT NULL,
	infovalue text NOT NULL
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks (
	tid text PRIMARY KEY,
	pid text NOT NULL,
	begin_time text NOT NULL, 
	end_time text NOT NULL,
	description text,
	FOREIGN KEY (pid) REFERENCES pipes(pid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_args_json (
	tid text NOT NULL,
	arg_order integer NOT NULL,
	arg_value text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_args_resource (
	tid text NOT NULL,
	arg_order integer NOT NULL,
	rid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (rid) REFERENCES resources(rid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_args_file (
	tid text NOT NULL,
	arg_order integer NOT NULL,
	fid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (fid) REFERENCES files(fid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_args_pipe (
	tid text NOT NULL,
	arg_order integer NOT NULL,
	pid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (pid) REFERENCES pipes(pid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_kwargs_json (
	tid text NOT NULL,
	arg_key text NOT NULL,
	arg_value text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_kwargs_resource (
	tid text NOT NULL,
	arg_key text NOT NULL,
	rid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (rid) REFERENCES resources(rid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_kwargs_file (
	tid text NOT NULL,
	arg_key text NOT NULL,
	fid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (fid) REFERENCES files(fid)
);
""",
"""
CREATE TABLE IF NOT EXISTS tasks_kwargs_pipe (
	tid text NOT NULL,
	arg_key text NOT NULL,
	pid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (pid) REFERENCES pipes(pid)
);
""",
"""CREATE TABLE IF NOT EXISTS tasks_returnvalue (
	tid text NOT NULL,
	rid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (rid) REFERENCES resources(rid)
);
""",
"""CREATE TABLE IF NOT EXISTS tasks_outputfiles (
	tid text NOT NULL,
	forder int NOT NULL,
	fid text NOT NULL,
	FOREIGN KEY (tid) REFERENCES tasks(tid),
	FOREIGN KEY (fid) REFERENCES files(fid)
);
""",
"""
CREATE TABLE IF NOT EXISTS pipes (
	pid text PRIMARY KEY,
	func text NOT NULL,
	return_volatile integer NOT NULL,
	is_deterministic integer NOT NULL,
	module_name text,
	func_name text,
	output_func text,
	description text
);
""",
"""
CREATE TABLE IF NOT EXISTS resources (
	rid text PRIMARY KEY,
	volatile integer,
	description text
);
""",
"""
CREATE TABLE IF NOT EXISTS files (
	fid text PRIMARY KEY,
	file_path text,
	md5 text,
	description text
);
""",
"""
CREATE TABLE IF NOT EXISTS task_tags (
	tid text NOT NULL,
	tag_value text,
	FOREIGN KEY (tid) REFERENCES tasks(tid)
);
""",
"""
CREATE TABLE IF NOT EXISTS task_info (
	tid text NOT NULL,
	info_key text,
	info_value text,
	FOREIGN KEY (tid) REFERENCES tasks(tid)
);
""",
"""
CREATE TABLE IF NOT EXISTS resource_tags (
	rid text NOT NULL,
	tag_value text,
	FOREIGN KEY (rid) REFERENCES resources(rid)
);
""",
"""
CREATE TABLE IF NOT EXISTS resource_info (
	rid text NOT NULL,
	info_key text,
	info_value text,
	FOREIGN KEY (rid) REFERENCES resources(rid)
);
""",
"""
CREATE TABLE IF NOT EXISTS pipe_tags (
	pid text NOT NULL,
	tag_value text,
	FOREIGN KEY (pid) REFERENCES pipes(pid)
);
""",
"""
CREATE TABLE IF NOT EXISTS pipe_info (
	pid text NOT NULL,
	info_key text,
	info_value text,
	FOREIGN KEY (pid) REFERENCES pipes(pid)
	
);
""",
"""
CREATE TABLE IF NOT EXISTS file_tags (
	fid text NOT NULL,
	tag_value text,
	FOREIGN KEY (fid) REFERENCES files(fid)
);
""",
"""
CREATE TABLE IF NOT EXISTS file_info (
	fid text NOT NULL,
	info_key text,
	info_value text,
	FOREIGN KEY (fid) REFERENCES files(fid)
	
);
""",
]
	c = sql.cursor()
	c.execute("BEGIN")
	try:
		for cmd in cmds:
			c.execute(cmd)
		c.execute("COMMIT")
	except sql.Error as e:
		print("Encounter error when creating DB")
		print(e)
		c.execute("ROLLBACK")
	
	if len(c.execute('SELECT * FROM metainfo WHERE infokey = ?', ['dbid']).fetchall()) == 0:
		c.execute("BEGIN")
		c.execute('INSERT INTO metainfo(infokey, infovalue) VALUES(?, ?)', ["dbid", uuid.uuid4().hex])
		c.execute("COMMIT")

	


#####################################
# Export
#####################################	

def export_files(self, o, *target_files, directorymode='common', rootdirectory='.'):
	'''
	Export files from a database. Briefly, zip the target files with a info file.
	
	Directory mode: Can be common, root, or basename
	'''
	if os.path.exists(o):
		raise Exception(f"File exists: {o}.")
		
	dbid, = self.sql.cursor().execute("SELECT infovalue FROM metainfo WHERE infokey = 'dbid'").fetchone()
	frs = [self.file_from_path(f) for f in target_files]
	abspaths = [fr.file_path for fr in frs]
	
	
	
	
	target_file_dict = None
	if directorymode == 'common':
		common_path = os.path.commonpath(abspaths)
		if not os.path.isdir(common_path):
			common_path = os.path.dirname(common_path)
		target_file_dict = {p: os.path.relpath(p, common_path) for p in abspaths}
	elif directorymode == 'root':
		target_file_dict = {p: os.path.relpath(p, rootdirectory) for p in abspaths}
	elif directorymode == 'basename':
		target_file_dict = {p: os.path.basename(p) for p in abspaths}
	else:
		raise Exception("Unknown directorymode")
	# Create the metainfo file
	metainfo_file = tempfile.NamedTemporaryFile(mode='w+', suffix=".txt", delete=False).name
	with open(metainfo_file, 'w') as fw:
		fw.write(f"## RMS_DB_ID: {dbid}\n")
		fw.write(f"#File\tMD5\tRMS_File_ID\n")
		for fr in frs:
			fw.write(f"{target_file_dict[fr.file_path]}\t{fr.md5}\t{fr.fid}\n")

	# Also add the metainfo file to the dict
	target_file_dict[metainfo_file] = 'RMS_metainfo.txt'
	
	# Check for duplicated alt file names
	cter = Counter([v for k, v in target_file_dict.items()])
	dup_alt_file_names = list(filter(lambda v: v > 1, cter.values()))
	if len(dup_alt_file_names) > 0:
		raise Exception("Duplicated file names detected. If you are using directorymode 'basename', consider using other directorymode")
	
	
	
	# Output the zip file
	with zipfile.ZipFile(o, "w") as zw:
		for target_file, alt_file_name in target_file_dict.items():
			zw.write(target_file, alt_file_name)
	
	# remove the temp metainfo file
	os.unlink(metainfo_file)

def _read_meta_info(metainfo_file):
	from biodata.baseio import BaseReader
	entries = {}
	with BaseReader(metainfo_file) as br:
		s = br.read()
		dbid = re.match(r"## RMS_DB_ID:\s([a-f0-9]{32})", s).group(1)
		br.read()
		for s in br:
			fname, fmd5, fid = s.split("\t")
			entries[fid] = fname, fmd5, fid 
	return dbid, entries 
		
		
		
def export_files_to_directory(self, o, *target_files, directorymode='common', rootdirectory='.',):
	'''
	Export files from a database to the target directory. 
	
	Directory mode: Can be common, root, or basename
	
	'''
		
	dbid, = self.sql.cursor().execute("SELECT infovalue FROM metainfo WHERE infokey = 'dbid'").fetchone()
	frs = [self.file_from_path(f) for f in target_files]
	abspaths = [fr.file_path for fr in frs]
	
	# Compile relative path for target_files
	target_file_dict = None
	if directorymode == 'common':
		common_path = os.path.commonpath(abspaths)
		if not os.path.isdir(common_path):
			common_path = os.path.dirname(common_path)
		target_file_dict = {p: os.path.relpath(p, common_path) for p in abspaths}
	elif directorymode == 'root':
		target_file_dict = {p: os.path.relpath(p, rootdirectory) for p in abspaths}
	elif directorymode == 'basename':
		target_file_dict = {p: os.path.basename(p) for p in abspaths}
	else:
		raise Exception("Unknown directorymode")
	# Check for duplicated alt file names
	cter = Counter([v for k, v in target_file_dict.items()])
	dup_alt_file_names = list(filter(lambda v: v > 1, cter.values()))
	if len(dup_alt_file_names) > 0:
		raise Exception("Duplicated file names detected. If you are using directorymode 'basename', consider using other directorymode")
	
	# Read the old RMS metainfo file
	f = os.path.abspath(o + "/" + "RMS_metainfo.txt")
	if os.path.exists(f):
		old_dbid, old_entries = _read_meta_info(f)
		if old_dbid != dbid:
			raise Exception(f"The DB IDs are different: {old_dbid} and {dbid}")
	else:
		old_entries = {}
	
	# Build the current entries
	entries = {}
	for fr in frs:
		entries[fr.fid] = target_file_dict[fr.file_path], fr.md5, fr.fid
		
	# Check for inconsistency in old and current entries
	for fname, fmd5, fid in entries.values():
		if fid in old_entries:
			ofname, ofmd5, ofid = old_entries[fid]
			if fname != ofname or fmd5 != ofmd5:
				raise Exception(f"Inconsistent file: {fid}")
	# Create the metainfo file
	metainfo_file = tempfile.NamedTemporaryFile(mode='w+', suffix=".txt", delete=False).name
	with open(metainfo_file, 'w') as fw:
		fw.write(f"## RMS_DB_ID: {dbid}\n")
		fw.write(f"#File\tMD5\tRMS_File_ID\n")
		
		for fname, fmd5, fid in old_entries.values():
			fw.write(f"{fname}\t{fmd5}\t{fid}\n")
		for fname, fmd5, fid in entries.values():
			if fid in old_entries:
				continue
			fw.write(f"{fname}\t{fmd5}\t{fid}\n")

	
	# Also add the metainfo file to the dict
	target_file_dict[metainfo_file] = 'RMS_metainfo.txt'
	
	# Update the target file dict
	target_file_dict = {target_file: os.path.abspath(o + "/" + alt_file_name) for target_file, alt_file_name in target_file_dict.items()}
	
	# Check if file exists
	existing_files = []
	for fr in frs:
		if fr.fid in old_entries:
			target_file_dict.pop(fr.file_path)
		else:
			if os.path.exists(target_file_dict[fr.file_path]):
				existing_files.append(target_file_dict[fr.file_path])
				
	if len(existing_files) > 0:
		raise Exception("File(s) exist.\n" + "\n".join(existing_files))
	print(f"Checking {len(target_files)} files")
	print(f"There are {len(old_entries)} old entries in last export.")
	print(f"Exporting {len(target_file_dict) - 1} entries...")
	
	for src, dst in target_file_dict.items():
		os.makedirs(os.path.dirname(dst), exist_ok=True)
		shutil.copyfile(src, dst)
	shutil.copyfile(src, dst)
	
	# remove the temp metainfo file
	os.unlink(metainfo_file)
	print(f"Export completes.")

