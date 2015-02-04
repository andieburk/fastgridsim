import numpy

'''
Created on July 24, 2011
@author: Dilum Bandara
@version: 0.1
@license: Apache License v2.0

   Copyright 2012 H. M. N. Dilum Bandara, Colorado State University

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''




def GRLC(values):
    '''
    Calculate Gini index, Gini coefficient, Robin Hood index, and points of 
    Lorenz curve based on the instructions given in 
    www.peterrosenmai.com/lorenz-curve-graphing-tool-and-gini-coefficient-calculator
    Lorenz curve values as given as lists of x & y points [[x1, x2], [y1, y2]]
    @param values: List of values
    @return: [Gini index, Gini coefficient, Robin Hood index, [Lorenz curve]] 

	note by Andrew Burkimsher, July 2012:
	the complexity of this algorithm is quadratic. Efficient linear methods exist
	for calculating these values. this implementation is intractable for lists longer
	than about 10,000 elements. Numpy linear implementations from alternative
	providers are vastly faster.
	'''
    n = len(values)
    assert(n > 0), 'Empty list of values'
    sortedValues = sorted(values) #Sort smallest to largest

    #Find cumulative totals
    cumm = [0]
    for i in range(n):
        cumm.append(sum(sortedValues[0:(i + 1)]))

    #Calculate Lorenz points
    LorenzPoints = [[], []]
    sumYs = 0.0           #Some of all y values
    robinHoodIdx = -1   #Robin Hood index max(x_i, y_i)
    for i in range(1, n + 2):
        x = 100.0 * (i - 1)/n
        y = 100.0 * (cumm[i - 1]/float(cumm[n]))
        LorenzPoints[0].append(x)
        LorenzPoints[1].append(y)
        sumYs += y
        maxX_Y = x - y
        if maxX_Y > robinHoodIdx: robinHoodIdx = maxX_Y   
    
    giniIdx = 100.0 + (100.0 - 2.0 * sumYs)/n #Gini index

    return [giniIdx, giniIdx/100.0, robinHoodIdx, LorenzPoints]


"""
	Provides an uncategorized collection of possibly useful utilities.
	
	:copyright: 2005-2009 Alan G. Isaac, except where another author is specified.
	:license: `MIT license`_
	:author: Alan G. Isaac (and others where specified)
	
	.. _`MIT license`: http://www.opensource.org/licenses/mit-license.php
"""


#all the Gini calculations include the 1/N correction
#speed comparison:
"""
using python
	1    0.000    0.000    1.734    1.734 GiniCoef.py:101(calc_gini4)
	1    0.000    0.000    0.100    0.100 GiniCoef.py:112(calc_gini5)
	1    1.093    1.093    2.025    2.025 GiniCoef.py:122(calc_gini3)
	1    0.000    0.000    3.430    3.430 GiniCoef.py:133(calc_gini2)
	1    0.000    0.000    3.379    3.379 GiniCoef.py:146(calc_gini)
	
	fairly convincingly better using pure numpy in calc_gini5 
	
"""

def calc_gini4(x): #follow transformed formula
	"""Return computed Gini coefficient.
		:contact: aisaac AT american.edu
		"""
	xsort = sorted(x) # increasing order
	y = numpy.cumsum(xsort)
	B = sum(y) / (y[-1] * len(x))
	return 1 + 1./len(x) - 2*B



def calc_gini5(x):
	x = numpy.array(x)
	x.sort()
	lenx = len(x)
	y = numpy.cumsum(x)
	B = numpy.sum(y) / (y[-1] * lenx)
	gini = 1 + 1./lenx - 2*B
	return gini


def calc_gini3(x): #follow transformed formula
	"""Return computed Gini coefficient.
		:contact: aisaac AT american.edu
		"""
	yn, ysum = 0.0, 0.0
	for xn in sorted(x):
		yn += xn
		ysum += yn
	B = ysum / (len(x) * yn)
	return 1 + 1./len(x) - 2*B

def calc_gini2(x): #follow transformed formula
	"""Return computed Gini coefficient.
		
		:note: follows tranformed formula, like R code in 'ineq'
		:see: `calc_gini`
		:contact: aisaac AT american.edu
		"""
	x = sorted(x)  # increasing order
	n = len(x)
	G = sum(xi * (i+1) for i,xi in enumerate(x))
	G = 2.0*G/(n*sum(x)) #2*B
	return G - 1 - (1./n)

def calc_gini(x):
	"""Return computed Gini coefficient.
		
		:note: follows basic formula
		:see: `calc_gini2`
		:contact: aisaac AT american.edu
		"""
	x = sorted(x)  # increasing order
	N = len(x)
	B = sum( xi * (N-i) for i,xi in enumerate(x) ) / (N*sum(x))
	return 1 + (1./N) - 2*B


def test():
	import random
	
	#nums = numpy.array([random.random() for r in xrange(100000)])
	nums = numpy.random.random_sample(1000000)
	#print GRLC(nums)[0]


	print calc_gini(nums)
	print calc_gini2(nums)
	print calc_gini3(nums)
	print calc_gini4(nums)
	print calc_gini5(nums)





if __name__ == "__main__":
	
	import cProfile
	
	cProfile.run('test()')
	
	

	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	"""
	import matplotlib.pyplot as plt
	#Example
	sample1 = [1, 2, 1.2, 2, 3, 1.9, 2.2, 4.5]
	sample2 = [812, 841, 400, 487, 262, 442, 972, 457, 491, 461, 430, 465, 991, \
				479, 427, 456]

	#result = GRLC(sample1)
	result = GRLC(sample2)
	print 'Gini Index', result[0]  
	print 'Gini Coefficient', result[1]
	print 'Robin Hood Index', result[2]
	print 'Lorenz curve points', result[3]

	#Plot
	plt.plot(result[3][0], result[3][1], [0, 100], [0, 100], '--')
	plt.xlabel('% of pupulation')
	plt.ylabel('% of values')
	plt.show()
	"""