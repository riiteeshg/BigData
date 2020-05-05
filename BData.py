
import string
import sys
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Wordcountapp")
sc = SparkContext(conf = conf)

print("-----------------------------------------------------------")
print("-----------------------------------------------------------")
print("-----------------------------------------------------------")

filetoanalyzeA = sc.textFile("Encrypted-1.txt")
filetoanalyzeB = sc.textFile("Encrypted-2.txt")
filetoanalyzeC = sc.textFile("Encrypted-3.txt")

alphab = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
dictionary = enchant.Dict('en-US')

def diffLetters(letter, compareTo='e'):
    return string.ascii_lowercase.index(compareTo.lower()) - string.ascii_lowercase.index(letter.lower())


def shiftText(text, amount):
    newString = ""
    for chat in text:
        if char.lower() not in alphab:
                newString += char
        else :
            newIndex = string.ascii_lowercase.index(char.lower()) + amount
            if nexIndex > 25 :
                newIndex -= 26
            if newIndex < 0 :
                newIndex += 26
            newString += alphab[newIndex]

def mostCommon(cArray, index = 0):
    try:
        char = cArray[index][0]
        if str(char) not in alphab:
            return char
    except Exception as e:
        print(e)
        sys.exit(0)
    return None



def get_words(file):
    words = file.map(lambda line : str(line)).flatMap(lambda line: line.split())
    words_count = words.map(lambda word: (word,1)).reduceByKey(lambda x,y: x+y)
    total_words = sum(words_count.collectAsMap().values())
    print("word count :" + str(total_words))


def get_chars(file):
    chars = file.flatMap(lambda line:line)
    char_count = chars.map(lambda char:(char, 1)).reduceByKey(lambda x,y: x+y)
    allchars = char_count.collectAsMap()
    totalchar = sum(allchars)



print("--total words --") 
print(get_words(total_words))


word_result = wordCount.map(lambda word: (word[0], word[1], word[1]/float(total_words)))
word_result.saveAsTextFile("results.txt")
print(word_result)
chars = rdd.map(lambda line: str(line)).flatMap(lambda line: list(line))

total_chars= chars.count()
charCount = chars.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
char_result = charCount.map(lambda word:(word[0], word[1], word[1]/float(total_chars)))

print("---chars----")
print(total_chars)

def get_shift(sss):
    shift = (alphab.find(s) - alphab.find("E"))
    print("The shift of the Cypher file : " + str(shift))
    print("The shift of the Cypher file : " + str(shift))
    return shift

def convert(file, path):
    decrypted = words.map(lambda w : decryption(w, shift = shift))
    decrypted.mapPartitions(check_words).mean()
    # getting the first non null value then saving at text file 
    file.coalesce(1).map(lambda line: decryption(line, shift)).saveAsTextFile(path)



def results(file, path):
    get_chars(file)
    get_words(file)
    get_shift(sss)
    convert(name,path)
    print (" ")