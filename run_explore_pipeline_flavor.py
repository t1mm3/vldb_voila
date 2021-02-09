#! /bin/env python3

import multiprocessing
import subprocess
import build_config

def get_tag():
	return "explore_per_pipeline_base_flavor.db"

def powers2_until(max):
	i = 1
	r = []
	while i < max:
		r.append(i)
		i = 2*i
	return r

def main():
	max_num_cores = multiprocessing.cpu_count()

	threads = [max_num_cores]
	# threads = list(set(powers2_until(max_num_cores) + [max_num_cores]))
	queries = ["q3", "q9"]
	runs = 5

	scale_factors = [100]

	for scale in scale_factors:
		for t in threads:
			for q in queries:
				cmd="{explore} -s {scale} --mode {db} --pipeline --num_threads {t} -q {q} -r {runs}".format(
					explore=build_config.get_explore_executable(),
					db=get_tag(), t=t, q=q, scale=scale, runs=runs)
				print(cmd)
				subprocess.run(cmd, shell=True)

if __name__ == '__main__':
	main()