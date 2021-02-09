def get_all_queries():
	return ["j1", "j1rev", "j2", "j2rev", "q1", "q6", "q9"]

def get_all_flavors():
	return ["vector", "hyper"]

def get_binary_path():
	return "@PROJECT_BINARY_DIR@"

def get_source_path():
	return "@PROJECT_SOURCE_DIR@"


def get_main_executable():
	return get_binary_path() + "/voila"

def get_explore_executable():
	return get_binary_path() + "/explorer"

def run_timeout(cmd, timeout):
	# inspired by https://stackoverflow.com/questions/36952245/subprocess-timeout-failure
	import os
	import signal
	from subprocess import Popen, PIPE, TimeoutExpired
	from time import monotonic as timer

	start = timer()
	with Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, preexec_fn=os.setsid) as process:
		try:
			output, err = process.communicate(timeout=timeout)
			success = process.returncode == 0
			# print(err)
			return (success, output, err)
		except TimeoutExpired:
			print('Timeout for {}'.format(cmd))
			os.killpg(process.pid, signal.SIGINT) # send signal to the process group
			output = process.communicate()[0]
			return None

def syscall(cmd, time_out_seconds=5*60):
	print(cmd)
	# os.system(cmd)

	output = run_timeout(cmd, None) # time_out_seconds
	return output


def run(flavor=None, hot_runs=None, queries=None, no_run=None, scale_factor=None,):
	assert(flavor is not None)

	cmd = get_main_executable()
	
	if flavor is not None:
		cmd = "{} --flavor={}".format(cmd, flavor)
	if hot_runs is not None:
		cmd = "{} --hot_runs={}".format(cmd, hot_runs)
	if queries is not None:
		cmd = "{} --queries={}".format(cmd, queries)
	if no_run is not None and no_run:
		cmd = "{} --no-run".format(cmd)
	if scale_factor is not None:
		cmd = "{} --scale_factor={}".format(cmd, scale_factor)

	return syscall(cmd)