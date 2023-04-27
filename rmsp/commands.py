def run_template_bash(template, output_func=None, conda_env=None, conda_root=None, init_cmd=None, **kwargs):
	'''
	Note that version, template, output_func, conda_env, conda_root, init_cmd are keywords and cannot be used in template
	
	:param conda_env: The name or the folder of conda envrionment
	:param conda_root: e.g. /home/username/miniconda3/
	:param init_cmd: The initial command to run before going into the conda run command. Useful when you need to set / unset environment variables. For example, 'unset MPLBACKEND" 
	
	'''
	import subprocess
	full_cmd = template.format(**kwargs)
	
	if conda_env is None:
		r = subprocess.run(full_cmd, shell=True, executable='/bin/bash')
		
	else:		
		full_cmd = full_cmd.replace("\\", "\\\\").replace('"', '\\"')
		full_cmd = f'/bin/bash -c "{full_cmd}"'
		conda_run_cmd = "conda run -n " + conda_env + " " + full_cmd
		if conda_root is not None:
			conda_init_cmd = f'if __conda_setup="$(\'{conda_root}/bin/conda\' \'shell.bash\' \'hook\' 2> /dev/null)"; then eval "$__conda_setup"; else if [ -f "{conda_root}/etc/profile.d/conda.sh" ]; then . "{conda_root}/etc/profile.d/conda.sh"; else export PATH="{conda_root}/bin:$PATH"; fi; fi; unset __conda_setup; '
		else:
			conda_init_cmd = ""
		if init_cmd is None:
			init_cmd = ""
		else:
			if not init_cmd.endswith(";"):
				init_cmd = init_cmd + ";"
		
		r = subprocess.run(conda_init_cmd + init_cmd + conda_run_cmd, shell=True, executable='/bin/bash')
	if r.returncode != 0:
		raise Exception()

def OUTPUT_run_template_bash(template, output_func=None, conda_env=None, conda_root=None, init_cmd=None, **kwargs):
	if output_func is None:
		return []
	else:
		return output_func(**kwargs)
