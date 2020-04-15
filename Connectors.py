
import datetime


class DBFSConnector:

    def fetch(inStages, inStagesData, stageId, spark, config):
        return spark.read.\
            options(header='true' if eval(config)["is_header"] == "Use Header Line" else 'false',
                    inferschema='true',
                    delimiter=eval(config)["delimiter"])\
            .csv(eval(config)['url'])

    def put(inStages, inStagesData, stageId, spark, config):
        inStagesData[inStages[0]].write\
            .format('csv') \
            .options(header='true' if eval(config)["is_header"] == "Use Header Line" else 'false',
                     delimiter=eval(config)["delimiter"]) \
            .save(("%s %s") % (datetime.datetime.now().strftime("%Y-%m-%d %H.%M.%S")+"_", eval(config)['url']))
