import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window


object ReturnTrips {
  def compute(trips: Dataset[Row], dist: Double, spark: SparkSession): Dataset[Row] = {
    import spark.implicits._


    val earthRadius = 6371.0
    val distanceThreshold = dist / 1000.0  // convert to km


    // Haversine formula
    val haversine = (lat1: Column, lon1: Column, lat2: Column, lon2: Column) => {
      val dLat = toRadians(abs(lat2 - lat1))
      val dLon = toRadians(abs(lon2 - lon1))
      val hav = pow(sin(dLat * 0.5), 2) +
                pow(sin(dLon * 0.5), 2) * cos(toRadians(lat1)) * cos(toRadians(lat2))
      lit(2 * earthRadius) * asin(sqrt(hav))
    }


    val minimalTrips = trips
      .select(
        $"tpep_pickup_datetime",
        $"tpep_dropoff_datetime",
        $"pickup_latitude",
        $"pickup_longitude",
        $"dropoff_latitude",
        $"dropoff_longitude"
      ) 
      .filter(
        $"pickup_longitude" =!= 0 && $"pickup_latitude" =!= 0 &&
        $"dropoff_longitude" =!= 0 && $"dropoff_latitude" =!= 0
      )
    
    val bucketSize = 0.0013


    // Create bucket columns:
    //   A = dropoff (lat, lon)
    //   B = pickup  (lat, lon)
    val withBuckets = minimalTrips
      .withColumn("a_dropoff_lat_bucket", floor($"dropoff_latitude"  / bucketSize))
      .withColumn("a_dropoff_lon_bucket", floor($"dropoff_longitude" / bucketSize))
      .withColumn("b_pickup_lat_bucket",  floor($"pickup_latitude"   / bucketSize))
      .withColumn("b_pickup_lon_bucket",  floor($"pickup_longitude"  / bucketSize))
      // Partition by A's lat/lon
      .repartition(32, $"a_dropoff_lat_bucket", $"a_dropoff_lon_bucket")
      .cache()


    def neighbors(c: Column) = array(c - 1, c, c + 1)

    // Expand B side by ±1 on time, lat, lon
    val bExploded = withBuckets
      .withColumn("b_pickup_lat_bucket_expl",
        explode(neighbors($"b_pickup_lat_bucket"))
      )
      .withColumn("b_pickup_lon_bucket_expl",
        explode(neighbors($"b_pickup_lon_bucket"))
      )
    val a = withBuckets.alias("a")
    val b = bExploded.alias("b")

    // just matching A’s (time, lat, lon) to B’s exploded neighbors
    val joined = a.join(
      b,
      expr("""
        a.a_dropoff_lat_bucket  = b.b_pickup_lat_bucket_expl
        AND a.a_dropoff_lon_bucket  = b.b_pickup_lon_bucket_expl
      """),
      joinType = "inner"
    )


    val result = joined.filter(
      ($"a.tpep_dropoff_datetime" < $"b.tpep_pickup_datetime") &&
      ($"b.tpep_pickup_datetime" < $"a.tpep_dropoff_datetime" + expr("INTERVAL 8 HOURS")) &&
      (haversine($"a.dropoff_latitude", $"a.dropoff_longitude", $"b.pickup_latitude", $"b.pickup_longitude") < distanceThreshold) &&
      (haversine($"a.pickup_latitude", $"a.pickup_longitude", $"b.dropoff_latitude", $"b.dropoff_longitude") < distanceThreshold)
    )
    result

  }
}