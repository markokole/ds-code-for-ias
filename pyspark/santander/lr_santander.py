from pyspark.sql import SparkSession

if __name__ == "__main__":

    # create spark session
    spark = SparkSession\
            .builder\
            .appName("santander")\
            .getOrCreate()

    log4jLogger = spark._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    list_arg = sys.argv[1].split(';')
    input_file = list_arg[0]

    # load file into df
    #file = "s3a://hdp-hive-s3/santander/train.csv"
    train_df = spark.read.csv(input_file, header=True, inferSchema=True)

    train_df = train_df.drop("ID_code")

    columns = train_df.columns

    ##NB: not needed!! target is already numeric
    #label
    #from pyspark.ml.feature import StringIndexer
    #label = StringIndexer(inputCol="target", outputCol="targetIndex").fit(train_df)
    #train_df = label.transform(train_df)

    #features
    from pyspark.ml.feature import VectorAssembler
    features = VectorAssembler(). \
        setInputCols(columns). \
        setOutputCol("features")

    train_df = features.transform(train_df)

    train_df = train_df.select(["target", "features"])

    (train, test) = train_df.randomSplit([0.7, 0.3])

    ## training
    from pyspark.ml.classification import LogisticRegression

    reg = 0.01
    maxiter = 10
    # log regularization rate
    log.warn("reg {}".format(reg))
    log.warn("maxiter {}".format(maxiter))

    # Train a logistic regression model.
    lr = LogisticRegression(regParam=reg, labelCol="target", featuresCol="features", maxIter=maxiter)
    model = lr.fit(train)

    # predict
    prediction = model.transform(test)

    # evaluate the accuracy of the model using the test set
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator

    evaluator = MulticlassClassificationEvaluator(metricName='accuracy').setLabelCol("target")
    accuracy = evaluator.evaluate(prediction)

    log.warn('Regularization rate is {}'.format(reg))
    log.warn("Accuracy is {}".format(accuracy))
