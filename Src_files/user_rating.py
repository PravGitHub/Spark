from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()

    business = sc.textFile("/home/asdf/Downloads/Data/business.csv", 1).map(lambda line: line.split("::"))
    business = business.filter(lambda line: "Colleges & Universities" in line[2]).map(lambda line: (line[0], 0))

    review = sc.textFile("/home/asdf/Downloads/Data/review.csv", 1).map(lambda line: line.split("::"))
    review = review.map(lambda line: (line[2], (line[1], line[3])))
    res = review.join(business, 1).map(lambda line: line[1][0]).distinct()
    res.saveAsTextFile("/home/asdf/Downloads/Data/user_rating_Output")
    sc.stop()

