#!/bin/env python3
import build_config
import re

modules = [
	("cg_vector.cpp", "Direct Vector", 0),
	("cg_hyper.cpp", "Direct Hyper", 0),
	("cg_fuji_vector.cpp", "FUJI Vector", 1),
	("cg_fuji_scalar.cpp", "FUJI Scalar", 1),
	("cg_fuji_avx512.cpp", "FUJI AVX-512", 1)]

line_ingore = ["debug", "DEBUG", "Debug",
	"trace", "Trace", "TRACE"]

def comment_remover(text):
	def replacer(match):
		s = match.group(0)
		if s.startswith('/'):
			return " " # note: a space and not an empty string
		else:
			return s
	pattern = re.compile(
		r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
		re.DOTALL | re.MULTILINE
	)
	return re.sub(pattern, replacer, text)

def ignore_lines(text):
	lines = text.split("\n")
	r = []
	for line in lines:
		ok = True
		for p in line_ingore:
			if p in line:
				ok = False
				break

		stripped_line = line.strip()
		if stripped_line == "":
			ok = False

		if not ok:
			continue

		r.append(line)
	return r

def get_metrics(filename):
	with open(filename, 'r') as file:
		data = file.read()
		data = comment_remover(data)
		data = ignore_lines(data)

		return len(data)

def main():
	print("\\begin{tabular}{l | r }")
	print("\\toprule")
	print("Back-end Module & LOC \\\\")

	old_group = -1
	for (filename, name, group) in modules:
		(loc) = get_metrics(build_config.get_source_path() + "/" + filename)

		if group != old_group:
			print("\\midrule")
			old_group = group

		print("{} & {}\\\\".format(name, loc))

	print("\\bottomrule")
	print("\\end{tabular}")

if __name__ == '__main__':
	main()
