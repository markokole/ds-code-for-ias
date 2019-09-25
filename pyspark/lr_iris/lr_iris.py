import sys
from pyspark.sql import SparkSession

if __name__ == "__main__":

    # create spark session
    spark = SparkSession\
            .builder\
            .appName("iris")\
            .getOrCreate()

    log4jLogger = spark._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    input_file = "s3a://hdp-hive-s3/test/iris.csv"
    output_dir = "s3a://hdp-hive-s3/test/git_iris_out"

    # load file into df
    iris_df = spark.read.csv(input_file, header=True, inferSchema=True)

    from pyspark.sql.functions import monotonically_increasing_id
    iris_df = iris_df.withColumn("row_id", monotonically_increasing_id())

    # label
    from pyspark.ml.feature import StringIndexer
    label = StringIndexer(inputCol="species", outputCol="speciesIndex").fit(iris_df)
    iris_df = label.transform(iris_df)

    # features
    from pyspark.ml.feature import VectorAssembler
    features = VectorAssembler().\
      setInputCols(["sepal_length", "sepal_width", "petal_length", "petal_width"]).\
      setOutputCol("features")
    iris_df = features.transform(iris_df)

    iris_df = iris_df.select(['row_id', 'features', 'speciesIndex'])

    # split dataset
    (train, test) = iris_df.randomSplit([0.7, 0.3])

    test_row_id = test.select('row_id')
    test = test.drop('row_id')
    train = train.drop('row_id')

    ## training
    from pyspark.ml.classification import LogisticRegression

    reg = 0.01
    maxiter = 10
    # log regularization rate
    log.warn("reg {}".format(reg))
    log.warn("maxiter {}".format(maxiter))

    # Train a logistic regression model.
    lr = LogisticRegression(regParam=reg, labelCol="speciesIndex", featuresCol="features", maxIter=maxiter)
    model = lr.fit(train)

    # predict
    prediction = model.transform(test)

    # evaluate the accuracy of the model using the test set
    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    evaluator = MulticlassClassificationEvaluator(metricName='accuracy').setLabelCol("speciesIndex")
    accuracy = evaluator.evaluate(prediction)

    log.warn('Regularization rate is {}'.format(reg))
    log.warn("Accuracy is {}".format(accuracy))

    prediction.select(["speciesIndex", "prediction"]).write.csv(output_dir)

    log.warn("Output saved to {}".format(output_dir))
