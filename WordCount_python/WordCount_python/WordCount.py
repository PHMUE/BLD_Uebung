text_file = "this is a test" #sc.textFile("hdfs://...")
counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)
print(counts) #counts.saveAsTextFile("hdfs://...")