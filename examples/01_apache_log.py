from pyspark import SparkConf, SparkContext
from pyspark.storagelevel import StorageLevel

if __name__ == '__main__':
    conf = SparkConf().setAppName("apache_log").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    file_rdd = sc.textFile("hdfs://master:8020/input/apache.log")

    # 分割文件
    context_rdd = file_rdd.map(lambda x: x.split(" "))

    # 缓存文件
    context_rdd.persist(StorageLevel.DISK_ONLY)

    # 计算当前网站访问的PV
    print(context_rdd.count())

    # 当前访问的UV
    # 先取出所有用户
    users_rdd = context_rdd.map(lambda x: x[1])
    # 去重、计数
    print(users_rdd.distinct().count())

    # ip
    # 取出所有ip
    ips_rdd = context_rdd.map(lambda x: x[0])
    # 去重、打印
    print(ips_rdd.distinct().collect())
    # 哪个页面访问量最高
    urls_rdd = context_rdd.map(lambda x: x[-1])
    url_with_one_rdd = urls_rdd.map(lambda x: (x, 1))
    urls_reduce_rdd = url_with_one_rdd.reduceByKey(lambda a, b: a+b)
    urls_sorted_rdd = urls_reduce_rdd.sortBy(lambda x: x[1], False, 1)
    urls_list_rdd = urls_sorted_rdd.map(lambda x: x[0])
    print(urls_list_rdd.first())
    context_rdd.unpersist()
