#!/bin/env python
from lxml import etree as ET
import sys
import getopt

def intrinsic(c, prev, cpuid, descr, descr_code, name, tech, restype, category, params):
	# print("cpuid {} name {} restype {} params {}".format(cpuid, name, restype, params))

	cpuids = "{"
	first = True 
	for cid in cpuid:
		cpuids = """{p}{s}"{i}" """.format(p=cpuids, s="" if first else ",",
			i=cid)
		first = False

	cpuids = cpuids + "}"

	c.write("""
{prev}IntrinsicTableInstallEntry {{ {cpuids}, "{name}", "{tech}", "{restype}", "{category}", {{""".format(
		prev=prev,cpuids=cpuids, name=name, tech=tech, restype=restype,category=category))
	first = True
	for (nme, tpe) in params:

		c.write(""" {p}IntrinsicTableInstallEntry::Arg {{ "{tpe}", "{nme}" }}""".format(
			p=" " if first else ",", tpe=tpe, nme=nme))
		first = False
	c.write("""} }""")

def generate(h, c, h_file, files, name):
	c.write("""#include "{header}"
""".format(header=h_file))
	c.write("std::vector<IntrinsicTableInstallEntry> get_intrinsics_{name}() {{ return {{".format(name=name));
	first = True
	for file in files:
		parser = ET.XMLParser(recover=True)
		tree = ET.parse(file,parser)
		for intr in tree.xpath('//intrinsic'):
			# print(ET.tostring(intr))

			#print(intr.attrib)

			cpuid = []
			descr = ""
			descr_code = ""
			category = ""
			params = []
			for l in intr.iter():

				e = l.tag
				t = l.text

				if e == "CPUID":
					cpuid.append(t)
				elif e == "description":
					attr = l.attrib
					if "code" in attr and attr["code"] == "true":
						descr_code = t
					else:
						descr = t
				elif e == "category":
					category = t
				elif e == "parameter":
					attr = l.attrib
					params.append((attr["varname"], attr["type"]))

			intrinsic(c, " " if first else ",",
				cpuid, descr, descr_code, intr.get("name"), intr.get("tech"),
				intr.get("rettype"), category, params)
			first = False
	c.write("};")
	c.write("}")

def main(argv):
	try:
		opts, args = getopt.getopt(argv,"",["c=","h=","f=","n="])
	except getopt.GetoptError:
		help()
		sys.exit(2)
	for opt, arg in opts:
		if opt in ("--c"):
			c_file = arg
		elif opt in ("--h"):
			h_file = arg
		elif opt in ("--n"):
			name = arg
		elif opt in ("--f"):
			files = [arg]

	with open(h_file, "w") as h:
		h.write("""
#ifndef H_INTRINSICS_{name}
#define H_INTRINSICS_{name}

#include "intrinsics_base.hpp"

std::vector<IntrinsicTableInstallEntry> get_intrinsics_{name}();

#endif
			""".format(name=name))
		with open(c_file, "w") as c:
			generate(h, c, h_file, files, name)

if __name__ == "__main__":
	main(sys.argv[1:])

