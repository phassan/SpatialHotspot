# Statistically Significant Spatial Hot-spot Analysis Using Apache Spark
The Hot Spot Analysis tool calculates the Getis-Ord statistic of [NYC Taxi Trip](https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page) datasets. The final output is a text file containing 50 top hot spots in the descending order of the z scores.

The Hot Spot Analysis tool is a maven-scala project.

## INSTALLATION
  ```
  cd SpatialHotspot
  mvn package
  ```
## EXECUTION
  ```
  ./bin/spark-submit \
    --class com.spatial.hotspot.Driver \
	--master <master-url> \
	--deploy-mode <deploy-mode> \
	--conf <key>=<value> \
	spatial_hotspot.jar \
	/path/to/input \
	/path/to/output 
  ```

