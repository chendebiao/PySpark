from pyspark import SparkContext, SparkConf
import os
import sys
import numpy as np 

#datasets path on shared group directory on Ukko2. Uncomment the one which you would like to work on.
#dataset = "/wrk/group/grp-ddi-2020/datasets/data-1-sample.txt"
dataset = "/wrk/group/grp-ddi-2020/datasets/data-1.txt"
#dataset = "/wrk/group/grp-ddi-2020/datasets/data-2-sample.txt"
#dataset = "/wrk/group/grp-ddi-2020/datasets/data-2.txt"


conf = (SparkConf()
        .setAppName("chchen")           ##change app name to your username
        .setMaster("spark://10.251.52.13:7077")
        .set("spark.executor.memory", "16g")
        .set("spark.cores.max", "10")  ##dont be too greedy ;)
        .set("spark.rdd.compress", "true")
        # .set("spark.driver.maxResultSize","50g")
        .set("spark.broadcast.compress", "true"))
sc = SparkContext(conf=conf)

data = sc.textFile(dataset)
data = data.map(lambda s: float(s))

def select_pivot(data):
    '''To select the median of data

    Select the the median of data using Numpy as the pivot in quick_select_nth().

    Args:
        data (pyspark.rdd.PipelinedRDD): The data input.
    Returns:
        np.median(data) (float): the median of input
    '''
    # Convert PipelinedRDD to np.array
    data = np.array(data)
    # Return the result(median of input)
    return np.median(data)

def quick_select_nth(data, n, count):
    '''Find the n^th greatest number of a dataset

    Select the n^th greatest number using Quick Select Algorithm.

    Args:
        data (pyspark.rdd.PipelinedRDD): The dataset used (range).
        n (int): The n of n^th greatest number
        count(int): The count of data

    Returns:
        pivot: the n^th greatest number
    '''

    if count > 999:
        # Take 999 as the input of select_pivot(), to set the pivot for quick select.
        pivot = select_pivot(data.take(999))
    else:
        # If the count of data is less than 999, we can just take the first element as pivot, it's doesn't matter on the calculate time(less than 10s in total).
        pivot = data.first()

    # Take all the elements blow / above / equal to pivot.
    below = data.filter(lambda x: x < pivot).map(lambda s: float(s))
    above = data.filter(lambda x: x > pivot).map(lambda s: float(s))
    equal = data.filter(lambda x: x == pivot).map(lambda s: float(s))
    
    # Calculate the total number of below and below or equal
    num_less = below.count()
    num_lessoreq = num_less + equal.count()

    # If the total number of below is higher than n-1, that means the median is in the below set, we need to find the nth element in below set.
    # If the total number of below is not lower than n-1, that means the median is in the above set, we need to find the n-num_lessoreqth element in above set.
    # Otherwise, BINGO, we gotcha the nth element!
    if n-1 < num_less:
        return quick_select_nth(below, n, num_less)
    elif n-1 >= num_lessoreq:
        return quick_select_nth(above, n-num_lessoreq, count-num_lessoreq)
    else:
        return pivot

count = data.count()
n = round(count / 2)

if (count % 2 != 0):
        # If total count is Odd, we need to find the nth element, which is the median
        median = quick_select_nth(data, n, count)
        print "Total count is odd, the median is:"+ str(median)
else:   
        # If total count is Evenm we need to find the nth and the n+1th elements, the median is the average.                   
        median_left = quick_select_nth(data, n, count)
        median_right = quick_select_nth(data, n+1, count)
        median = (median_left + median_right) / 2
        print "The total is Even, median is the mean of the two middle numbers"
        print "Left_median : " + str(median_left)
        print "Right_median : " + str(median_right)
        print "Median : "+ str(median)

