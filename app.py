import sys, ast
from pyspark import SparkConf, SparkContext

OutputPath = 'gs://dataproc-staging-us-west1-949914310960-nyxpajdc/output/'
InputPath = 'gs://dataproc-staging-us-west1-949914310960-nyxpajdc/'

def top(input_files, n):
    words_rdd = None
    for file in input_files:
        temp_rdd = sc.textFile(file).flatMap(lambda line: line.split(" "))
        if words_rdd is not None:
            words_rdd = temp_rdd.union(words_rdd)
        else:
            words_rdd = temp_rdd
    pair_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    flipped = pair_rdd.map(lambda x: (x[1], x[0]))
    sorted_rdd = flipped.sortByKey(False)
    sc.parallelize(sorted_rdd.take(n)).saveAsTextFile(OutputPath)


def search(input_files, term):
    combined = None
    for file in input_files:
        current = searchInFile(file, term)
        if combined is not None:
            combined = current.union(combined)
        else:
            combined = current
    combined.saveAsTextFile(OutputPath)


def searchInFile(file, term):
    words_rdd = sc.textFile(file).flatMap(lambda line: line.split(" "))
    pair_rdd = words_rdd.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    value = pair_rdd.lookup(term)[0]
    return sc.parallelize([(term, (file[63:], value))])


if __name__ == "__main__":
    input_files = ast.literal_eval(sys.argv[3])
    input_files = [InputPath + x for x in input_files]
    conf = SparkConf()
    sc = SparkContext.getOrCreate(conf=conf)
    opt = sys.argv[1]
    if opt == 'top':
        n = int(sys.argv[2])
        top(input_files, n)
    elif opt == 'search':
        search(input_files, sys.argv[2])
    else:
        print('please enter a valid option: top or search')
