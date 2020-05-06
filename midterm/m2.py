from pyspark import SparkConf, SparkContext

import aspell

conf = SparkConf().setMaster('local').setAppfile('CesarsCypher_Riteshgautam.py')
sc = SparkContext(conf=conf)

#import files
fileToAnalyzeA = sc.textFile('Encrypted-1.txt')
fileToAnalyzeB = sc.textFile('Encrypted-2.txt')
fileToAnalyzeC = sc.textFile('Encrypted-3.txt')
letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def get_words(file):
    # separate words
    words = file.map(lambda line: str(line)).flatMap(lambda line: line.split())
    # get total number of words
    wCount = words.map(lambda word: (
        word, 1)).reduceByKey(lambda x, y: x + y)
    wtotal = sum(wCount.collectAsMap().values())
    print("The word count for this document is : " + str(wtotal))
    # get most used words
    wFreq = wCount.top(50, lambda x: x[1])
    words_top = str(wFreq[0])
    print("The most common word frequency for this file is : " + words_top)
    return words


def get_chars(file):
    # separate characters
    chars = file.flatMap(lambda line: line)
    # get total number of chars
    char_count = chars.map(lambda char: (
        char, 1)).reduceByKey(lambda x, y: x + y).filter(lambda x: x[0].isalpha())
    cCount = char_count.collectAsMap()
    total_chars = sum(cCount.values())
    # get most used characters
    cFreq = char_count.top(50, lambda x: x[1])
    print("Character frequencies are ", cFreq)

    most_freq_char = cFreq[0][0]  # take the first tuple and then the character

    return most_freq_char


def get_shift(shft):
    shift = letters.find(shft) - letters.find("E")
    print("The shift of the Cypher for the file is : " + str(shift))
    print("In this file, character 'E', is represented by : " + shft)
    return shift


def decrypt(key, shift):
    return "".join([letters[(letters.find(shft) - shift)]
                    if shft in letters else shft
                    for shft in key])


# initalize speller for each partition
# speller not serrializable so we have to do it by partition
def word_check(words):
    a = aspell.Speller('lang', 'en')
    for word in words:
        yield a.check(word)


def covert(file, outfile, words, shift):
    decrypted = words.map(lambda w: decrypt(w, shift=shift))
    # find the fraction of decrypted words that are in the dictionary
    decrypted.mapPartitions(word_check).mean()
    # save as text file
    file.coalesce(1).map(lambda line: decrypt(
        line, shift)).saveAsTextFile(outfile)


def results(file, outfile):
    most_freq_char = get_chars(file)
    words = get_words(file)
    shift = get_shift(most_freq_char)
    covert(file, outfile, words, shift)
    print(" ")


results(fileToAnalyzeA, "results1.txt")
results(fileToAnalyzeB, "results2.txt")
results(fileToAnalyzeC, "results3.txt")
