#!/usr/bin/env python

import sys

# Add parent to python path
sys.path.insert(0,"..")
from condor import *

def main():
	a1 = CondorJobNode("a1")
	a2 = CondorJobNode("a2")
	b1 = CondorJobNode("b1")
	b2 = CondorJobNode("b2")
	c1 = CondorJobNode("c1")

	a1.addChild(b1)
	a1.addChild(b2)

	a2.addChild(b2)
	
	c1.addParent(b1)
	c1.addParent(b2)

	dag = CondorDAG()

	dag.addNode(a1)
	dag.addNode(a2)
	dag.addNode(b1)
	dag.addNode(b2)
	dag.addNode(c1)

	print dag.script()
	pass

if __name__ == "__main__":
	main()
