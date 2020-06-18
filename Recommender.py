import sys
import re
from pyspark import SparkContext

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print >> sys.stderr, "spark-submit MainPy2.py [HDFS file]\n" \
              + "[searcher] [number to get] [Medium (Video, Book, etc.] [Genre (as gen:'War')] [Rating (as rat:4 for over 4 stars) or top (for top in genre)\n" \
              + "[recommender] [customerID] [Medium] [Genre] [Rating]\n"
        exit(-1)
    sc = SparkContext()
    act = sys.argv[2]
    if act == 'recommender':
        cID = sys.argv[3].strip("'")
        toDisplay = 5
    else:
        toDisplay = sys.argv[3]
    item = sys.argv[4]
    if len(sys.argv) > 5:
        if sys.argv[5].startswith('gen:'):
            gen = sys.argv[5].strip("'")
            if len(sys.argv) is 7:
                rating = sys.argv[6]
            else:
                rating = ''
        else:
            gen = ''
            rating = sys.argv[5]
    else:
        rating = ''
        gen = ''

    baseRDD = sc.textFile(sys.argv[1]).map(lambda line: line.split(';\t')).filter(lambda line: line[3].strip() == item)
    if act == 'searcher':
        if rating.startswith('rat:'):
            baseRDD = baseRDD.filter(lambda line: float(line[8].split(':')[4]) >= float(rating[4:]))
            if gen.startswith('gen:'):
                baseRDD = baseRDD.filter(lambda line: gen[4:] in line[7])
        elif rating.startswith('top'):
            baseRDD = baseRDD.filter(lambda line: int(line[4]) > 0).sortBy(lambda line: int(line[4]))
            if gen.startswith('gen:'):
                baseRDD = baseRDD.filter(lambda line: gen[4:] in line[7])
    elif act == 'recommender':
        baseRDD = sc.textFile(sys.argv[1]).map(lambda line: line.split(';\t')).filter(lambda line: cID in line[9]).filter(lambda line: line[3].strip() == item).filter(lambda line: int(line[4]) > 0)
        similar = baseRDD.filter(lambda line: not 'NULL' in line[5]).map(lambda line: (line[1], line[5])).flatMapValues(lambda line: line.split()).map(lambda line: (line[1], line[0]))
        baseRDD = baseRDD.keyBy(lambda line: line[1]).subtractByKey(similar).map(lambda line: (line[1][4],line[1])).sortBy(lambda line: int(line[0]))
        if gen.startswith('gen:'):
            baseRDD = baseRDD.filter(lambda line: gen[4:] in line[1][7])        
    else:
        print("Operation not supported.")

    textFile = open("output", "w")
    for item in baseRDD.take(int(toDisplay)):
        print(item)
        textFile.write(str(item))
    sc.stop()
