import sys
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
    output_folder = list_arg[1]

    #input_file = "s3a://hdp-hive-s3/santander/train.csv"

    # load file into df
    train_df = spark.read.csv(input_file, header=True, inferSchema=True)

    train_df = train_df.drop("ID_code")

    columns = train_df.columns[1:]

    #features
    from pyspark.ml.feature import VectorAssembler
    features = VectorAssembler(). \
        setInputCols(columns). \
        setOutputCol("features")

    train_df = features.transform(train_df)

    train_df = train_df.select(["target", "features"])

    (train, test) = train_df.randomSplit([0.8, 0.2])

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

    #Area under the receiver operating characteristic (ROC) curve
    from pyspark.mllib.evaluation import BinaryClassificationMetrics

    input_auroc = prediction.select(['prediction', 'target']).withColumn('target_d', prediction['target'].cast('double')).drop('target').rdd
    metrics = BinaryClassificationMetrics(input_auroc)
    areaUnderROC = metrics.areaUnderROC
    log.warn("Area under the precision-recall curve: {}".format(areaUnderROC))

    results = [[accuracy, areaUnderROC]]

    from pyspark.sql.types import StructType, StructField, DoubleType
    schema = StructType([
        StructField("accuracy", DoubleType(), True),
        StructField("areaUnderROC", DoubleType(), True)])

    results_df = spark.createDataFrame(results, schema)

    #print(results_df.show())

    log.warn(results_df.show())

    results_df.coalesce(1).write.csv(output_folder, header = 'true')















    #from pyspark.sql.types import FloatType
    #df = spark.createDataFrame([accuracy], FloatType())

    #df.coalesce(1).write.csv(output_folder)

    log.warn("Output saved to {}".format(output_folder))
