import glob
import json
import os
import importlib
import inspect

def _load_func(path, funcname):
	spec = importlib.util.spec_from_file_location("__main__", path)
	module = importlib.util.module_from_spec(spec)
	spec.loader.exec_module(module)
	func = getattr(module, funcname)
	return func

class InputFileType():
	pass
class OutputFileType():
	pass

class RMSTemplateBook:
	def __init__(self, name, content):
		self.name = name
		self.content = content
		

class RMSTemplateLibrary:
	
	def __init__(self, rms, libpath):
		self.rms = rms
		rmsbooks = {}
		rmsbookpaths = {} # Separated from book to avoid leaking path information
		for manifest_json_path in glob.glob(f"{libpath}/**/manifest.json"):
			with open(manifest_json_path, "rt") as f:
				manifest_json = json.load(f)
			
			bookpath = os.path.dirname(manifest_json_path)
			bookname = os.path.basename(bookpath)
			rmsbooks[bookname] = RMSTemplateBook(bookname, manifest_json)
			rmsbookpaths[bookname] = bookpath 
		self.rmsbooks = rmsbooks
		self.rmsbookpaths = rmsbookpaths
			
	def get_books(self):
		return self.rmsbooks

	def get_section(self, bookname, chaptername, bookmark):
		c = self.rmsbooks[bookname].content["chapter"][chaptername]
		for idx in bookmark:
			c = c["content"][idx]
		return c
	
	def get_doc(self, bookname, chaptername, bookmark):
		section = self.get_section(bookname, chaptername, bookmark)
		if "doc" in section:
			with open(self.rmsbookpaths[bookname] + "/" + section["doc"]["source"], "rt") as f:
				content = f.read()
			return {"type": section["doc"]["type"], "content": content}
		else:
			return None
	
	def load_template_func(self, bookname, chaptername, bookmark):
		bookpath = self.rmsbookpaths[bookname]
		section = self.get_section(bookname, chaptername, bookmark)
		sourcepath = section["source"]
		funcname = section["name"]
		return _load_func(bookpath + "/" + sourcepath, funcname)

	
	def get_func_signature(self, bookname, chaptername, bookmark):
		s = inspect.signature(self.load_template_func(bookname, chaptername, bookmark))
		s = s.replace(parameters=tuple(s.parameters.values())[1:])
		return s
	
	def simulate(self, bookname, chaptername, bookmark, args=[], kwargs={}):
		func = self.load_template_func(bookname, chaptername, bookmark)
		return self.rms.simulate_template(func, args, kwargs)
		
	def run(self, bookname, chaptername, bookmark, args=[], kwargs={}):
		func = self.load_template_func(bookname, chaptername, bookmark)
		return self.rms.run_template(func, args, kwargs)
	
	def execute_builder(self):
		self.rms.execute_builder()
# class RMSTemplate:
# 	pass
# class RMSLibrary():
# 	def __init__(self, libname, libpath, manifest_json):
# 		self.libname = libname
# 		self.libpath = libpath
# 		self.manifest_json = manifest_json
	
# def extract_libraries(rmslib_path):
# 	rms_libraries = []
# 	for manifest_json_path in glob.glob(f"{rmslib_path}/**/manifest.json"):
# 		with open(manifest_json_path, "rt") as f:
# 			manifest_json = json.load(f)
# 		libpath = os.path.dirname(manifest_json_path)
# 		libname = os.path.basename(libpath)
# 		rms_libraries[libname] = RMSLibrary(libname, libpath, manifest_json)
# 	return rms_libraries


	
	