#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, rmin, rmax, con):
    #Implement RangeQuery Here.
	cur = con.cursor()
	'''	
	for j in range(0,5):
		rs='SELECT * FROM RangeRatingsPart'+str(j)
		cur.execute(rs)
		rows=cur.fetchall()
		print rows
	'''	
	cur.execute('SELECT * FROM RangeRatingsMetadata WHERE minrating <= '+str(rmin)+' AND maxrating >= '+str(rmin)+' OR minrating <= '+str(rmax)+' AND maxrating >= '+str(rmax));
	#cur.execute('SELECT * FROM RangeRatingsMetadata')
	rows1 = cur.fetchall()
	#print "here"
	print rows1
	start = rows1[0][0]
	end = rows1[1][0]
	#print start, end
	cur.execute('SELECT * FROM RoundRobinRatingsMetadata')
	rtemp=cur.fetchall()
	#print rtemp
	const2=""
	cont=""	
	f = open('RangeQueryOut.txt','w')
	

	for j in range(start, end+1):
		print j
		
		pname = j
		#rs = 'SELECT * FROM RangeRatingsPart'+str(pname)
		rs = 'SELECT * FROM RangeRatingsPart'+str(pname)+' WHERE Rating >= '+str(rmin)+' AND Rating <= '+str(rmax)+' ORDER BY Rating'
		#output = 'COPY ({0}) TO STDOUT WITH CSV HEADER'.format(rs)
		cur.execute(rs)
		rows = cur.fetchall()
		rows = list(rows)
		#rows.pop(0)
		print rows
		
			#cur.copy_expert(output,f)
		for row in rows:
			row=list(row)
			row.insert(0,"RangeRatingsPart"+str(pname))
			cont = cont + str(row).replace('[','').replace(']','').replace('\'','').replace('(','').replace(')','') + '\n' 
	
	for i in range(0, rtemp[0][0]):
		print i			
		q = 'SELECT * FROM RoundRobinRatingsPart'+str(i)+' WHERE Rating >= '+str(rmin)+' AND Rating <= '+str(rmax)+' ORDER BY Rating'
		cur.execute(q)
		rr=cur.fetchall()
		print "here"
		print rr			
		rr = list(rr)
		
		for rrr in rr:
			rrr = list(rrr)			
			rrr.insert(0,"RoundRobinRatingsPart"+str(i))
			const2 = const2+str(rrr).replace('(','').replace(')','').replace('\'','').replace('[','').replace(']','')+'\n'
	
		
	print const2
	print cont
	f.write(const2)
	f.write(cont)
	f.close()	
	
    #pass #Remove this once you are done with implementation

def PointQuery(ratingsTableName, rval, con):
    #Implement PointQuery Here.
	cur = con.cursor()
	f = open('PointQueryOut.txt','w')
	cont=""
	const2=""
	cur.execute('SELECT * FROM RangeRatingsMetadata WHERE minrating <= '+str(rval)+' AND maxrating >= '+str(rval));
	rows=cur.fetchone()
	print rows
	pname = rows[0]
	print pname
	cur.execute('SELECT * FROM RangeRatingsPart'+str(pname)+' WHERE Rating = '+str(rval)+' ORDER BY Rating')
	rows = cur.fetchall()
	print rows
	cur.execute('SELECT * FROM RoundRobinRatingsMetadata')
	rtemp=cur.fetchall()
	for r in rows:
		cont=cont+"RangeRatingsPart"+str(pname)+', '+str(r).replace('[','').replace(']','').replace('\'','').replace('(','').replace(')','') + '\n'
	for i in range(0, rtemp[0][0]):
		q = 'SELECT * FROM RoundRobinRatingsPart'+str(i)+' WHERE Rating = '+str(rval)+' ORDER BY Rating'
		cur.execute(q)
		r1 = cur.fetchall()
		for r2 in r1:		
			const2=const2+"RoundRobinRatingsPart"+str(i)+', '+str(r2).replace('[','').replace(']','').replace('\'','').replace('(','').replace(')','') + '\n'
	print const2
	print cont
	f.write(const2)
	f.write(cont)
	f.close()
	
    #pass # Remove this once you are done with implementation


