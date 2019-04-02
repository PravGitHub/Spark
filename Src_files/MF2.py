from pyspark import SparkContext


def MF_map(value):
    value = value.split("\t")
    data = []
    if len(value) == 2 and value[1].strip() != "":
        person = int(value[0])
        friends = value[1].split(",")

        for friend in friends:
            frnd = int(friend)
            if person < frnd:
                key = str(person)+"_"+str(frnd)
            else:
                key = str(frnd)+"_"+str(person)

            data.append((key, value[1]))

    return data


def MF_reduce(values1, values2):
    count = 0
    if values1 is not None and values2 is not None:
        set1 = values1.split(",")
        set2 = values2.split(",")

    for friend in set1:
        if friend in set2:
            count += 1

    return count


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    data = sc.textFile("/home/asdf/Downloads/Data/soc-LiveJournal1Adj.txt", 1)
    mapped = data.flatMap(MF_map)

    res = mapped.reduceByKey(MF_reduce)
    res.saveAsTextFile("/home/asdf/Downloads/Data/MFOutput")
    sc.stop()
