import traceback
import sys
from operations import TopOperation
from operations import JoinOperation
from operations import AggregationOperation
from operations import FormulaOperation
from operations import FilterOperation
from connectors import DBFSConnector
from connectors import CosmosDBConnector
from datatransformations import TranformationsMainFlow
from automl import tpot_execution
from core import PipelineNotification
from clustering.ClusteringMain import Clustering
import json

try: 
	PipelineNotification.PipelineNotification().started_notification('5e7a182edd2cef6591905210','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	abcxyzzzNEwClusterApplication_DBFS = DBFSConnector.DBFSConnector.fetch([], {}, "5e7a182edd2cef6591905210", spark, "{'url': '/Demo/MovieRatingsTrain.csv', 'file_type': 'Delimeted', 'dbfs_token': 'dapid40af94a6c7d8d818acf548df4c773f8', 'dbfs_domain': 'eastus.azuredatabricks.net', 'delimiter': ',', 'is_header': 'Use Header Line'}")

	PipelineNotification.PipelineNotification().completed_notification('5e7a182edd2cef6591905210','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7a182edd2cef6591905210','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7a182edd2cef6591905211','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	abcxyzzzNEwClusterApplication_AutoFE = TranformationsMainFlow.TramformationMain.run(["5e7a182edd2cef6591905210"],{"5e7a182edd2cef6591905210": abcxyzzzNEwClusterApplication_DBFS}, "5e7a182edd2cef6591905211", spark,json.dumps( {"FE": [{"transformationsData": {}, "feature": "UserId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "465.06", "stddev": "264.69", "min": "1", "max": "943", "missing": "0"}}, {"transformationsData": {}, "feature": "MovieId", "transformation": "", "type": "numeric", "replaceby": "mean", "selected": "True", "stats": {"count": "2587", "mean": "432.85", "stddev": "337.75", "min": "1", "max": "1656", "missing": "0"}}, {"transformationsData": {}, "feature": "Rating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.52", "stddev": "1.15", "min": "1.0", "max": "5.0", "missing": "0"}, "transformation": ""}, {"transformationsData": {"feature_label": "Timestamp"}, "feature": "Timestamp", "type": "date", "selected": "True", "replaceby": "random", "stats": {"count": "", "mean": "", "stddev": "", "min": "", "max": "", "missing": "0"}, "transformation": "Extract Date"}, {"transformationsData": {}, "feature": "AvgRating", "type": "real", "selected": "True", "replaceby": "mean", "stats": {"count": "2587", "mean": "3.53", "stddev": "0.44", "min": "1.51", "max": "4.67", "missing": "0"}, "transformation": ""}, {"feature": "Timestamp_dayofmonth", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "16.05", "stddev": "9.06", "min": "1", "max": "31", "missing": "0"}}, {"feature": "Timestamp_month", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "7.0", "stddev": "4.34", "min": "1", "max": "12", "missing": "0"}}, {"feature": "Timestamp_year", "transformation": "", "transformationsData": {}, "type": "numeric", "generated": "True", "selected": "True", "stats": {"count": "2587", "mean": "1997.45", "stddev": "0.5", "min": "1997", "max": "1998", "missing": "0"}}]}))

	PipelineNotification.PipelineNotification().completed_notification('5e7a182edd2cef6591905211','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7a182edd2cef6591905211','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)
try: 
	PipelineNotification.PipelineNotification().started_notification('5e7a184edd2cef6591905213','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
	abcxyzzzNEwClusterApp_Cluster = Clustering.run(["5e7a182edd2cef6591905211"],{"5e7a182edd2cef6591905211": abcxyzzzNEwClusterApplication_AutoFE}, "5e7a184edd2cef6591905213", spark,json.dumps( {"autoClustering": 1, "defaultclusters": 1, "model": "Bisecting KMeans", "run_id": "cb3c758564df4dc39afcc1fd5b9973ef", "ProjectName": "TESTClusteringA", "PipelineName": "abcxyzzzNEwClusterApp", "pipelineId": "5e7a182edd2cef659190520f", "userid": "5e1eb97a7d1a8956f654a15f", "runid": "", "url_ResultView": "http://137.116.116.173:3200", "experiment_id": "2162989224512177"}))

	PipelineNotification.PipelineNotification().completed_notification('5e7a184edd2cef6591905213','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify')
except Exception as ex: 
	PipelineNotification.PipelineNotification().failed_notification(ex,'5e7a184edd2cef6591905213','5e1eb97a7d1a8956f654a15f','http://137.116.116.173:3200/pipeline/notify','http://137.116.116.173:3200/logs/getProductLogs')
	sys.exit(1)

