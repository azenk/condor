#!/usr/bin/env python

import sys,os

# Add parent to python path
sys.path.insert(0,"..")
from condor import *
from optparse import OptionParser

def main():
	p = OptionParser()
	(options,args) = p.parse_args()
	
	dag = CondorDAG()

	a1 = CondorJobNode("a1",dag)
	a1.setExecutable("/bin/sleep")
	a1.setArguments(["30"])
	a1.setInitialdir(args[0])
	a1.write(os.path.join(args[0],"a1.condor"))
	
	a2 = CondorJobNode("a2",dag)
	a2.setExecutable("/bin/date")
	a2.setInitialdir(args[0])
	a2.write(os.path.join(args[0],"a2.condor"))

	b1 = CondorJobNode("b1",dag)
	b1.setExecutable("/bin/date")
	b1.setInitialdir(args[0])
	b1.write(os.path.join(args[0],"b1.condor"))

	b2 = CondorJobNode("b2",dag)
	b2.setExecutable("/bin/date")
	b2.setInitialdir(args[0])
	b2.write(os.path.join(args[0],"b2.condor"))

	c1 = CondorJobNode("c1",dag)
	c1.setExecutable("/bin/date")
	c1.setInitialdir(args[0])
	c1.write(os.path.join(args[0],"c1.condor"))

	a1.addChild(b1)
	a1.addChild(b2)

	a2.addChild(b2)
	
	c1.addParent(b1)
	c1.addParent(b2)

	dag.write(os.path.join(args[0],"/tmp/testjob/testdag.dag"))
	pass

if __name__ == "__main__":
	main()
