from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("wordscount")
sc = SparkContext(conf=conf)

rdd = sc.textFile("test.txt")
print(rdd.collect())
print("-------word count -------------")
print(rdd.count())

word = rdd.map(lambda line: str(line)).flatMap(lambda x: x.split(" "))
countword = word.count()


print("-------word count -------------")
print(countword)
wresults = word.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
total_words = sum(wresults.collectAsMap().values())
wresults.collect()
mapresult = wresults.map(lambda word: (
    word[0], word[1], word[1]/float(total_words)))

chars = rdd.map(lambda line: str(line)).flatMap(lambda line: list(line))
countC = chars.count()
total_count = sum(countC.collectAsMap().values())

countChar = chars.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
cresults = countChar.map(lambda word: (
    word[0], word[1], word[1]/float/(total_count)))


# words_count = allwords.map(lambda word: (word, 1)).reduceByKey(lambda x,y: x + y)
final = sum(wresults.values())
print("----------------char results -----------------------------")
print(cresults)
print("---------------------------------------------------------------")


# print("the word count is :" + (allwords))
