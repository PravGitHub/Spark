from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import Row


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    sqlContext = SQLContext(sc)

    business = sc.textFile("/home/asdf/Downloads/Data/business.csv", 1).map(lambda line: line.split("::"))
    business = business.filter(lambda line: "NY" in line[1]).filter(lambda line: len(line) == 3)

    review = sc.textFile("/home/asdf/Downloads/Data/review.csv", 1).map(lambda line: line.split("::"))
    review = review.filter(lambda line: len(line) == 4)
    rev = review.map(lambda line: Row(review_id=line[0], user_id=line[1], business_id=line[2], stars=float(line[3])))
    rev_df = sqlContext.createDataFrame(rev)
    rev_df.registerTempTable("review")

    rev_res = sqlContext.sql("select business_id, avg(stars) as avg from review group by business_id")
    rev_res.registerTempTable("review_res")
    business = business.map(lambda line: Row(b_id=line[0], address=line[1], categories=line[2]))
    business_df = sqlContext.createDataFrame(business)
    business_df.registerTempTable("business")


    res = sqlContext.sql("select b.b_id, b.address, b.categories, r.avg from business b inner join review_res r on "
                         "b.b_id=r.business_id").distinct()


    res.repartition(1).sort('avg', ascending=False)
    r = res.rdd.map(lambda line: list(line)).repartition(1).filter(lambda l: len(l) == 4).sortBy(lambda a: a[3], ascending=False)

    r.saveAsTextFile("/home/asdf/Downloads/Data/q4_output")

    for el in r.take(10):
        print(el)

    sc.stop()
