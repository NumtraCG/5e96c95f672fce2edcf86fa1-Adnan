from pyspark.sql.functions import col, dayofmonth, month, year
import json
from pyspark.sql.functions import col, when
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark.sql.functions import mean, stddev, min, max, col


class CleanseData:
    # def __init__(self,df):
    #     #print()

    def replaceByMean(self, feature, df, mean_=-1):

        meanValue = df.select(mean(col(feature.name)).alias(
            'mean')).collect()[0]["mean"]
        df.fillna(meanValue, subset=[feature.name])
        df.withColumn(feature.name, when(col(feature.name) == " ",
                                         meanValue).otherwise(col(feature.name).cast("Integer")))
        return df

    def replaceByMax(self, feature, df, max_=-1):
        maxValue = df.select(max(col(feature.name)).alias('max')).collect()[
            0]["max"]
        df.fillna(maxValue, subset=[feature.name])
        df = df.withColumn(feature.name,
                           when(col(feature.name) == " ", maxValue).otherwise(col(feature.name)))
        return df

    def replaceByMin(self, feature, df, min_=-1):
        minValue = df.select(min(col(feature.name)).alias('min')).collect()[
            0]["min"]
        df.fillna(minValue, subset=[feature.name])
        df = df.withColumn(feature.name,
                           when(col(feature.name) == " ", minValue).otherwise(col(feature.name)))
        return df

    def replaceByStandardDeviation(self, feature, df, stddev_=-1):
        stddevValue = df.select(stddev(col(feature.name)).alias(
            'stddev')).collect()[0]["stddev"]
        df.fillna(stddevValue, subset=[feature.name])
        df = df.withColumn(feature.name,
                           when(col(feature.name) == " ", stddevValue).otherwise(col(feature.name)))
        return df

    def replaceDateRandomly(self, feature, df):
        fillValue = df.where(col(feature.name).isNotNull()
                             ).head(1)[0][feature.name]
        df.fillna(str(fillValue), subset=[feature.name])
        df = df.withColumn(feature.name,
                           when(col(feature.name) == " ", fillValue).otherwise(col(feature.name)))
        # print("CleanseData:replaceDateRandomly Schema : ", df.#printSchema())
        return df

    def replaceNullValues(self, fList, df):
        featuresList = df.schema.fields
        for featureObj in fList:
            for feat in featuresList:
                if featureObj["feature"] in feat.name:
                    featureName = feat
                    if "mean" in featureObj["replaceby"]:
                        df = self.replaceByMean(featureName, df)
                    elif "max" in featureObj["replaceby"]:
                        df = self.replaceByMax(featureName, df)
                    elif "min" in featureObj["replaceby"]:
                        df = self.replaceByMin(featureName, df)
                    elif "stddev" in featureObj["replaceby"]:
                        df = self.replaceByStandardDeviation(featureName, df)
                    elif "random" in featureObj["replaceby"]:
                        df = self.replaceDateRandomly(featureName, df)
        return df

    def getTypedValue(self, type, value):
        if isinstance(type, StringType):
            return value
        elif isinstance(type, DoubleType):
            if isinstance(value, str):
                if value == '0.0':
                    return 0.0
                else:
                    return float(value)
            return value
        elif isinstance(type, IntegerType):
            if isinstance(value, str):
                if value == '0.0':
                    return 0.0
                elif "." in value:
                    return int(float(value))
                elif value.isalpha():
                    return value
                else:
                    return int(value)
            return value
        return value


def ExtractDateTransform(df, params):
    print("ExtractDate:ExtractDateTransform Started parameters : \n",
          params, "\n", df.show())
    transform_params = params
    dfReturn = df
    feature = transform_params['feature']
    dfReturn = dfReturn.fillna({feature: ''})
    # dfReturn = dfReturn.withColumn(feature+'_weekofyear', weekofyear(col(feature)))
    # dfReturn = dfReturn.withColumn(feature+'dayofyear', dayofyear(col(feature)))
    dfReturn = dfReturn.withColumn(
        feature+'dayofmonth', dayofmonth(col(feature)))
    # dfReturn = dfReturn.withColumn(feature+'dayofweek', dayofweek(col(feature)))
    dfReturn = dfReturn.withColumn(feature+'month', month(col(feature)))
    dfReturn = dfReturn.withColumn(feature+'year', year(col(feature)))
    dfReturn = dfReturn.drop(feature)
    # print(dfReturn.printSchema())
    print("ExtractDate:ExtractDateTransform Output Dataframe :", dfReturn.head())
    return dfReturn


Feature_Transformations_Methods = {
    "String Indexer": StringIndexerTransform,
    "Extract Date": ExtractDateTransform,

}


class TransformationMain:
    # TODO: change df argument in run with following
    def run(inStages, inStagesData, stageId, spark, config):
        configObj = json.loads(config)
        featureData = configObj["FE"]
        transformationDF = inStagesData[inStages[0]]
        transformationDF = CleanseData().replaceNullValues(featureData, transformationDF)
        for transformation in featureData:
            feature = transformation["feature"]
            if transformation["transformation"] != '' and transformation["selected"].lower() == "true" and not (feature.__contains__("_transform")):
                transformationDF = Feature_Transformations_Methods["%s" % transformation["transformation"]](
                    transformationDF, transformation)

        return transformationDF
