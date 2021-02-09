#! /bin/env python3

import subprocess
import build_config
	

def main():
	for name in ["run_explore_pipeline_flavor.py", "print_base_flavors.py", "run_perf_baseline.py"]:
		cmd="{path}/{name}".format(
			path=build_config.get_binary_path(), name=name)
		print("Run: " + cmd)
		subprocess.run(cmd, shell=True)

if __name__ == '__main__':
	main()