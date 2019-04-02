from pyspark import SparkContext


def MF_map(value):
    user = int(value[0])

    keys = []
    if value[1][0] != [""]:
        friends = [int(x) for x in value[1][0]]

        for friend in friends:
            frnd = int(friend)
            if user < frnd:
                key = str(user)+"_"+str(frnd)
            else:
                key = str(frnd)+"_"+str(user)

            keys.append((key, value[1]))

    return keys


def MF_reduce(values1, values2):
    count = 0
    if values1[0] is not None and values2[0] is not None:
        set1 = [int(x) for x in values1[0]]
        set2 = [int(x) for x in values2[0]]

    for friend in set1:
        if friend in set2:
            count += 1

    return [count, values1[1], values2[1]]


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    friends = sc.textFile("/home/asdf/Downloads/Data/soc-LiveJournal1Adj.txt", 1)
    friends = friends.map(lambda l: l.split("\t")).filter(lambda l: len(l)==2).map(lambda l: (l[0], l[1].split(",")))

    data = sc.textFile("/home/asdf/Downloads/Data/userdata.txt", 1)
    data = data.map(lambda l: l.split(",")).filter(lambda l: len(l) == 10).map(lambda l: (l[0], (l[1], l[2], l[3])))

    joined = friends.join(data).repartition(1)

    mapped = joined.flatMap(MF_map)
    res = mapped.reduceByKey(MF_reduce)
    fin = res.sortBy(lambda l: l[1][0], ascending=False).map(lambda l: (l[1][0], l[0].split("_"), l[1][1], l[1][2]))

    fin.saveAsTextFile("/home/asdf/Downloads/Data/q2_Output")

    for el in fin.take(10):
        print(el)

    sc.stop()

