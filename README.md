# Resource Management System (RMS) Manual

RMS is an all-rounded python-based workflow management tool for computational analysis. It features a comprehensive logging system with a python API that could be incorporated into jupyter notebook easily (mainly for programmers) and a graphical user interface (mainly for non-programmers). The multiple front-end design allows both non-programmers and programmers to interact using the same system. 

## Features

The entire analysis stored in RMS is basically a single acyclic directed graph with Resources/Fileresources as nodes and Tasks as edges. All nodes have at most one incoming edge but can have unlimited number of outgoing edges. All actions are stored in the database. One can quickly re-fetch a previously generated results. 

While RMS is originally designed for logging python function, to further extend its use on data analysis, we also provide a set of easy-to-use command line functions. 

### Avoiding duplicated process

RMS record most common python functions and objects, and avoid duplicated jobs. Minimal efforts are needed from the users to avoid repeated jobs.  

### Effortless incorporation to existing pipelines

Rebuilding an entire analysis into the new system is usually a big hurdle, but not in RMS. RMS provides a simple solution - you can keep using most of your codes in the main body. The only thing to do is to change the import functions.



### Easy but powerful search

Many systems allow you to find out an upstream file required to generate the results. However, not all systems allow more complicated search. For example, can we search for the results based on some input file 1 and input file 2, processed through either pipeline X with the parameter A or pipeline Y with parameter B? Can we find out peak files using peak calling software J with alignment based on alignment software K? RMS enables these searches so that you can always retrieve your results quickly

### Easy Backup / Restore

Backup / Restore is an essential feature to protect a database system from data loss. The core RMS files include all the pipelines used, which are stored in an SQL database. It also allows users to select and backup various files or resource content (For example, only backup the raw file and key result file, and remove any intermediate files). Restoring a database requires. 

### Integrity check

Since RMS uses the absolute path to link to a file, users could easily access the file as for any downstream analysis or visualization. Users can also move or delete any unused intermediate files. Just like Python culture, nothing prevents you from modifying a file if you really do so. We encourage users to be responsible for their own actions. However, RMS will still do a quick check (based on file size) before using the file. An optional integrity check based on MD5 is also available. 



## Installation

To install Resource Management System, use the following:

```python
pip install rmsp
```



## Quick Start

### Try-it-out for the first time

This section is designed for lazy people to try RMS without knowing any details. The example codes below can be run without any extra dependency. 

#### Create RMS database

You only need to run this once when you set up a new database.

```python
DBPATH = "/path/to/db/"
DBNAME = "test.db"

from rmsp import rmsutils
rmsutils.create_new_db(f"{DBPATH}{DBNAME}")
```



#### Connect to RMS database and setup multi-processing run

You need run this every time at the beginning of your script

```python
DBPATH = "/path/to/db/"
DBNAME = "test.db"
DBRESDIR = "Resources/"
from rmsp import rmscore
rms = rmscore.ResourceManagementSystem(f"{DBPATH}{DBNAME}", f"{DBPATH}{DBRESDIR}")
from rmsp import rmsbuilder
rmspool = rmsbuilder.RMSProcessWrapPool(rms, 8)
rmsb = rmsbuilder.RMSUnrunTasksBuilder(rmspool)
from rmsp import rmsutils


```

#### Example of running a function

```python
def add_two_numbers(i, j):
	return i + j
add_two_numbers_pipe = rms.register_pipe(add_two_numbers)

result_1_plus_2 = add_two_numbers_pipe(1, 2)
result_pr_plus_4 = add_two_numbers_pipe(result_1_plus_2, 4)


```





## Overview

RMS represents the whole framework using different `RMSEntry`.

An `RMSEntry` can be in any of the following: 

- Pipe
- Task
- Resource
- Fileresource
- Unruntask
- Virtualresource

All RMSEntries, except `unruntask` and `virtualresource`, are stored in the database upon creation. `Unruntask` and `Virtualresource` are only stored in memory. They are not shared across different RMS instances, and they are lost when the RMS instance is close. 

#### RMS Entry Types

##### Pipe

A `pipe` stores all the core information of a python function. Users have to register a python function as `pipe` so that the running process can be tracked. 

##### Task

A `task` stores the information of executing a `pipe`. This includes the pipe ID, the arguments and the execution time. 

Users cannot register a `task` directly. All `tasks` are generated when running RMS pipes.

##### Resource

A `resource` is basically a wrapper to a python object. While you can check the original python object by using `resource.content`, you should never modify the content of a resource object. An exception to this is the generator, where it will be consumed instantly after the content is used. 

Users cannot register a `resource` directly. All `resources` are generated when running RMS pipes. 

##### Fileresource

A `Fileresource` stores the information of a file. RMS will store the file path of the file using the absolute path (but not real path and relative path). Upon registering a file, RMS will also store the md5 of the file. 

User can register any file on the file system. Registering the same file path will not create a new entry, unless users specified `force=True` when registering a file. 

If a file is generated in certain pipes, it will be automatically registered and linked to the corresponding task. If an existing registered file is overwritten by running the `pipe`, the old `fileresource` entry will be marked as `overwritten`, while a new `fileresource` will be created. If the old `fileresource` is used in running new `pipes`, an exception will be thrown.

Note that RMS do not actively track if files are overwritten by external program. Users can initiate integrity check in RMS routine check using the md5. See rmsutils.

##### Unruntask

An unruntask is a temporary holder for holding all required information to run a task. Once a task based on this unruntask completes, the unruntask will be automatically removed from RMS. The corresponding `task` can be found using `unruntask.replacement` after completion.

##### Virtualresource

A `virtualresource` is a temporary holder for any `resource` or `fileresource` objects to be generated when running the pipes. Once a task leading to the `virtualresource` completes, the `virtualresource` will be automatically removed from RMS. The corresponding `resource` or `fileresource` can be found using `virtualresource.replacement` after completion. 



#### Shared RMS Entry Properties

##### Description

The *description* of an RMSEntry is a field used by users to annotate.

##### info

The *info* of an RMSEntry is a field used by RMS to indicate different status. This includes:

- overwritten: If a task overwrites an already registered file, the RMSEntry representing the old file will have a status *overwritten*. Also, an RMS Entry will be created to represent the new file. In the database, there could be multiple RMSEntry pointing to the same path but there must be at most one RMSEntry without the `overwritten` state.
- obsolete: Upon changing any input of a task, the task and all downstream RMSEntry will be marked as *obsolete* because these entries are no longer up-to-date. Only if these RMSEntry is updated, the obsolete status will be removed. See also the substitution section. 
- sourcecode (Pipe only): If a function registered as Pipe is defined in `__main__`, the source code of the function will be stored as sourcecode. 
- outputfunc_sourcecode (Pipe only): Similar to sourcecode, but this is the source code for output_func. 







## User Guide - Programmatic access

The user guide here targets advanced programmers who want to gain full access to all features of RMS. If you are not familiar with programming but want to use little amount of programming to do repetitive jobs, please see the section User Guide - GUI.

### Initialization

#### Creating the DB

RMS stores all the RMS entries in an sqlite3 database. You need to create a new database the first time you use RMS. Example codes are shown below: 

```python
DBPATH = "/path/to/db/"
DBNAME = "test.db"

from rmsp import rmsutils
rmsutils.create_new_db(f"{DBPATH}{DBNAME}")
```

Here "test.db" will be created in your destinated folder

#### Connecting to the DB

Every time you use RMS, you need to initialize RMS with the following commands:

```python
DBPATH = "/path/to/db/"
DBNAME = "test.db"
DBRESDIR = "Resources/"
from rmsp import rmscore
rms = rmscore.ResourceManagementSystem(f"{DBPATH}{DBNAME}", f"{DBPATH}{DBRESDIR}")


```

The DBRESDIR is used to store the user-dumped *Resource* content.

#### Setting up multiprocessing support

In addition to the normal connection steps, you will need to add a few lines to create multiprocessing support for RMS as well. 

```python
DBPATH = "/path/to/db/"
DBNAME = "test.db"
DBRESDIR = "Resources/"
from rmsp import rmscore
rms = rmscore.ResourceManagementSystem(f"{DBPATH}{DBNAME}", f"{DBPATH}{DBRESDIR}")

from rmsp import rmsbuilder
rmspool = rmsbuilder.RMSProcessWrapPool(rms, 8)
rmsb = rmsbuilder.RMSUnrunTasksBuilder(rmspool)

```



#### Starting the GUI

(GUI may not be available at beta version)

You can initialize the GUI in the same console. 

```python
from rmsp import rmsgui
rmsgui.start_GUI(rms)
```

For jupyter user, you need to run the following prior to starting a GUI:

```python
%gui qt5
```

X11 may be required for running the GUI.

### Registering functions as `Pipes`

Before running anything in RMS, you need to register a python function first. After a successful registration, RMS will create a `Pipe` entry in the database. There are several important parameters when registering a function to RMS. 

##### Simple function

In the simplest case, one can just register the function with default parameter. Bound method is currently not supported.

```python
def add_two_numbers(i, j):
	return i + j
add_two_numbers_pipe = rms.register_pipe(add_two_numbers)

```

##### Function from a package

```python
import biodata.bed
read_all_pipe = rms.register_pipe(biodata.bed.BED3Reader.read_all)
```



##### Generator

For generator function in python, one should set `return_volatile` as `True`. The returned *Resource* from this task will be marked as volatile automatically. 

```python
def generate_number(n):
	for i in range(n):
		yield i
generate_number_pipe = rms.register_pipe(generate_number, return_volatile=True)
```



##### Functions with output files

For function that outputs a file, we must add an *output function* to indicate what output files are expected. The output function should have the same arguments as the original function, and should return a list of output files. Note that the order of output files could be important, when one use existing tasks to generate templates. See also: (TO-BE-ADDED). After the pipe has complete, all output files indicated from the output function will be automatically registered as *FileResources*. 

```python
def write_hello_world(output_file1, output_file2):
	with open(output_file1, "w") as f:
		f.write("Hello World 1!")
	with open(output_file2, "w") as f:
		f.write("Hello World 2!")

def OUTPUT_write_hello_world(output_file1, output_file2):
	return [output_file1, output_file2]

write_hello_world_pipe = rms.register_pipe(write_hello_world, output_func=OUTPUT_write_hello_world)
```



##### Function with unknown behavior

For function with returned values or output files  that cannot be repeated by running the function with the same parameter, one should set the parameter `is_deterministic` as `False`. 

```python
def get_random_number():
	import random
	return random.random()

get_random_number_pipe = rms.register_pipe(get_random_number, is_deterministic=False)
```

We also recommend setting`is_deterministic` as `False` for functions that involve retrieving files from the internet (such as `urlretrieve`).

##### Registering a function multiple times

If you register the same function using the same parameters, the already registered pipe is returned instead of creating a new pipe entry. This does not hold, however, when you are registering function defined in `__main__`. While RMS is trying hard to make the same function defined in different `__main__` the same, we still do not guarantee to find a matching old pipe.  

##### Independent to other functions

A custom registered function must not depend on any function. 



### Importing a `Pipe` from existing library

Importing `pipe` is similar to importing normal python function. To simplify the conversion, you can convert normal python imports into RMS imports using rmsutils. Note that you MUST register the library function first.

Example of RMS import:

```python
GenomicCollection = rms.rmsimport(moduleobj='genomictools', varname='GenomicCollection')
bb = rms.rmsimport(moduleobj='biodata.bed', return_first_module=False)

```

The rmsutils provide the following function to convert normal python imports into the above

```python
print(rmsutils.convert_import_str(
'''
# Normal python import
from genomictools import GenomicCollection
import biodata.bed as bb
'''
))
```



Note that the following does not work:

```python
from biodata.bed import *
```

We previously made this import viable. However, this may create serious security issue and we decided to remove such features. 



### Running a `Pipe`

After a function is registered, there are two methods to run the pipe. No matter which method is used, upon successful completion of the pipe, a `task`, all output `resources` and `fileresources` entries will be automatically registered to the database. 

#### Conventional way

The conventional way, resembling normal python function calling, is to call the pipe directly with arguments. The command will block until the Pipe has finished. 

```python
result_1_plus_2 = add_two_numbers_pipe(1, 2)
result_pr_plus_4 = add_two_numbers_pipe(result_1_plus_2, 4)
```

The following codes also work exactly the same. 

```python
result_1_plus_2 = rms.run(add_two_numbers_pipe, args=[1, 2])
result_pr_plus_4 = rms.run(add_two_numbers_pipe, args=[result_1_plus_2, 4])
```



Now the `result_1_plus_2` is a `Resource` that stores the content of the task.  

```python
result_1_plus_2.content
# 3
```

As you may have noticed, you can also use `Resource` object directly on running other pipes. 

```python
result_pr_plus_4.content
# 7
```



#### Running Pipe using RMSBuilder

Create a virtual RMS interface using RMSBuilder. Most commands are similar to running a task using the conventional way. The key difference of using a builder is that when users run the pipe, a list of `unruntasks` and `virtualresources` are created rather than executing the pipes instantly. 

First create the builder.

```python
from rmsp import rmsbuilder
rmspool = rmsbuilder.RMSProcessWrapPool(rms, 8)
rmsb = rmsbuilder.RMSUnrunTasksBuilder(rmspool)

```

The 

```python
add_two_numbers_builder_pipe = rmsb.register_pipe(add_two_numbers)
result_1_plus_2_vr = add_two_numbers_builder_pipe(1, 2)
result_3_plus_4_vr = add_two_numbers_builder_pipe(3, 4)
result_r1_plus_r2_vr = add_two_numbers_builder_pipe(result_1_plus_2_vr, result_3_plus_4_vr)
# All unruntasks are on hold until explict call to execution.
```

To start the execution, users need to call `execute_builder`. These `unruntasks` will then be run in parallel in a pool.

```python
rmsb.execute_builder()
```

To access the result after completion, one should use 

```python
result_r1_plus_r2 = result_r1_plus_r2_vr.replacement
result_r1_plus_r2.content
```





### Registering FileResource

To keep track of the files in RMS, all input files should be registered as a *FileResource*. For any input function, if you want to use a file parameter, use either `rms.register_file` or `rms.file_from_path` on the file path.

An example of function that copy a file

```python
def copy_a_file(input_file, output_file):
	import shutil
	# Do some other fancy stuff in this function
	shutil.copy2(input_file, output_file)
    
def OUTPUT_copy_a_file(input_file, output_file):
	return [output_file]

copy_a_file_pipe = rms.register_pipe(copy_a_file, output_func=OUTPUT_copy_a_file)
```

For files that are not generated through RMS, (e.g. downloaded from other sources) one can register. 

```python
# Suppose the input file was obtained from somewhere else
input_rmsfile_object = rms.register_file('/path/to/source/file')
intermediate_file_path = '/path/to/intermediate/file'
copy_a_file_pipe(input_rmsfile_object, intermediate_file_path)

# Since the intermediate file was generated in RMS, we only need to use file_from_path
intermediate_file_object = rms.file_from_path(intermediate_file_path)
output_file_path = '/path/to/output/file'
copy_a_file_pipe(input_rmsfile_object, output_file_path)
```



In the following example, although you can still get the desired result, the input file path is mistaken as a string parameter instead of a file parameter. 

```python
# An incorrect example
input_file_path = '/path/to/source/file'
intermediate_file_path = '/path/to/intermediate/file'
copy_a_file_pipe(input_file_path, intermediate_file_path)
```





### Finding RMS entry

This is one of the key advantage for programmer to use RMS - they can search their RMS entry with ease. 

Here we demonstrate several examples to find the related RMS Entries. 

(The example will be shown later)



### Rerunning

Human error is unavoidable. There are always cases that a wrong file is used or a wrong parameter is set. Therefore, it is better to have a system to let users rerun the task with the wrong files. 

You should mark the wrong files with "deprecated status". 

## RMS utilities

### Creation of new database

```python
DB_PATH = "/path/to/db/test.db"

from rmsp import rmsutils
rmsutils.create_new_db(DB_PATH)
```

### Exports



### Backup and Restore

##### Backup

To backup all the files, you can use the rmsutils

```python
from rmsp import rmsutils
rmsutils.backup('target_path', 'a.db', 'resource_dump_dir')
```

##### Restore

The files will be placed at the same path as before. However, 

```python
from rmsp import rmsutils
rmsutils.restore('src_path', 'target_path')
```



## User Guide - GUI

The GUI is useful to visualize all your tasks, files and resources. It allows instant analysis to certain variables. The template system also allows you to run simple analysis if you are unfamiliar with coding.  Please refer to rmsp-gui for details

## FAQ / Important Notes

This section includes some of the important notes that one should keep in mind when using RMS. 

- RMS does not store any information about the environment. It does not keep track of any external executables. Rather, it basically relies on the conda environment. Hence when doing the backup / restore, make sure you also backup the environment yourself. 


