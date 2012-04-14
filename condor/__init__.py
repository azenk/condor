#!/usr/bin/env python

class CondorJob:
	""" Defines a condor job """

	def __init__(self):
		""" Constructor """
		pass


	def script(self):
		""" Returns the condor script as a string """
		s = ""
		return s

	def write(self,jobfile):
		""" Writes job out to jobfile """
		f = open(jobfile,"w")
		f.write(self.script())
		f.close()

	def submit(self):
		""" Submits job to condor """
		pass

class CondorJobNode(CondorJob):
	""" Represents a condor job node for use in a CondorDAG """

	children = []

	def pre(self,executable):
		""" sets the pre-executable for this node """
		pass

	def post(self,executable):
		""" sets the post-executable for this node """
		pass

	def addChild(self,child):
		""" Add a child Job Node """
		self.children.append(child)
		

class CondorDAG:
	""" Condor Job Graph, suitable for use with DAGman """

	def __init__(self):
		""" Constructor """
		pass

	def script(self):
		""" Generates DAG Script """
		s = ""
		return s

	def write(self,dagfile):
		""" Writes dagfile """
		f = open(dagfile,"w")
		f.close()
		
