#!/bin/python3

import build_config
import sys
import getopt

num_timeout = 0
num_fail = 0
num_success = 0

def test_tpch(no_run=None):
	queries = build_config.get_all_queries()
	flavors = build_config.get_all_flavors()
	scale_factor = 1

	global num_timeout, num_fail, num_success

	for flavor in flavors:
		for query in queries:
			r = build_config.run(flavor=flavor, queries=query,
				scale_factor=scale_factor, no_run=no_run)
			if r is None:
				print("Timed out")
				num_timeout = num_timeout + 1
			else:
				(success, stdout, stderr) = r
				if success:
					num_success = num_success + 1
				else:
					print("Failed")
					num_fail = num_fail + 1


def main():
	test_tpch()

	print("""=== Summary ===

		{failed} failed, {timeout} timed out, {success} succeeded.

		""".format(failed=num_fail, timeout=num_timeout, success=num_success))

	if num_fail == 0 and num_timeout == 0:
		sys.exit(0)
	else:
		sys.exit(1)

if __name__ == "__main__":
	main()