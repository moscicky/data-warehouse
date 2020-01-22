
gsutil cp -r gs://inf132275/warehouse-input .

hadoop fs -mkdir -p input

hadoop fs -copyFromLocal warehouse-input/* input/

gsutil cp -r gs://inf132275/etl.jar .

gsutil cp -r gs://inf132275/SparkShellLoad.scala .

#spark-shell -i SparkShellLoad.scala

#:paste do spark-shell'a z SparkShellLoad.scala

#spark-shell
#:paste

# val schema = new SchemaCreator(spark)
# schema.createAll()

spark-submit --class warehouse.executables.TimeTableEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.SourceTableEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.AirPollutionTypeEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.CrimeTypeEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.LocationEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.OutcomeTypeEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.CrimeTableEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true

spark-submit --class warehouse.executables.AirQualityTableEtl \
--master yarn --num-executors 5 --driver-memory 512m \
--executor-memory 512m --executor-cores 1 etl.jar input true