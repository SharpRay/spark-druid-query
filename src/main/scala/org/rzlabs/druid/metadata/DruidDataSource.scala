package org.rzlabs.druid.metadata

import com.clearspring.analytics.stream.cardinality.ICardinality
import org.apache.spark.sql.types._
import org.joda.time.Interval
import org.rzlabs.druid.client.{Aggregator, ColumnDetails, MetadataResponse, TimestampSpec}

object DruidDataType extends Enumeration {
  val String = Value("STRING")
  val Long = Value("LONG")
  val Float = Value("FLOAT")
  val HyperUnique = Value("hyperUnique")
  val ThetaSketch = Value("thetaSketch")

  def sparkDataType(t: String): DataType = sparkDataType(DruidDataType.withName(t))

  def sparkDataType(t: DruidDataType.Value): DataType = t match {
    case String => StringType
    case Long => LongType
    case Float => FloatType
    case HyperUnique => BinaryType
    case ThetaSketch => BinaryType
  }
}

sealed trait DruidColumn {
  val name: String
  val dataType: DruidDataType.Value
  val size: Long // in bytes

  /**
   * Come from the segment metadata query.
   * Assume the worst for time dim and metrics (only string dimension has cardinality),
   * this is only used during query costing.
   */
  val cardinality: Long

  def isDimension(excludeTime: Boolean = false): Boolean
}

object DruidColumn {

  def apply(name: String,
            c: ColumnDetails,
            numRows: Long,
            timeTicks: Long): DruidColumn = {
    if (name == DruidDataSource.INNER_TIME_COLUMN_NAME) {
      DruidTimeDimension(name, DruidDataType.withName(c.`type`), c.size, Math.min(timeTicks, numRows))
    } else if (c.cardinality.isDefined) {
      DruidDimension(name, DruidDataType.withName(c.`type`), c.size, c.cardinality.get)
    } else {
      DruidMetric(name, DruidDataType.withName(c.`type`), c.size, numRows)
    }
  }
}

case class DruidDimension(name: String,
                          dataType: DruidDataType.Value,
                          size: Long,
                          cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean = false) = true
}

case class DruidMetric(name: String,
                       dataType: DruidDataType.Value,
                       size: Long,
                       cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean = false) = false
}

case class DruidTimeDimension(name: String,
                              dataType: DruidDataType.Value,
                              size: Long,
                              cardinality: Long) extends DruidColumn {
  def isDimension(excludeTime: Boolean = false) = !excludeTime
}

trait DruidDataSourceCapability {
  def druidVersion: String
  def supportsBoundFilter: Boolean = false
}

object DruidDataSourceCapability {

  private def versionCompare(druidVersion: String, oldVersion: String): Int = {
    def compare(v1: Int, v2: Int) = (v1 - v2) match {
      case 0 => 0
      case v if v > 0 => 1
      case _ => -1
    }
    druidVersion.split("\\.").zip(oldVersion.split("\\.")).foldLeft(Option.empty[Int]) {
      (l, r) => l match {
        case None | Some(0) => Some(compare(r._1.toInt, r._2.toInt))
        case res @ (Some(-1) | Some(1)) => res
      }
    }.get
  }

  def supportsBoundFilter(druidVersion: String): Boolean =
    if (druidVersion == null) false else
    versionCompare(druidVersion, "0.9.0") >= 0

  def supportsQueryGranularityMetadata(druidVersion: String): Boolean =
    if (druidVersion == null) false else
    versionCompare(druidVersion, "0.9.1") >= 0

  def supportsTimestampSpecMetadata(druidVersion: String): Boolean =
    if (druidVersion == null) false else
    versionCompare(druidVersion, "0.9.2") >= 0
}

case class DruidDataSource(name: String,
                           intervals: List[Interval],
                           columns: Map[String, DruidColumn],
                           size: Long,
                           numRows: Long,
                           timeTicks: Long,
                           aggregators: Option[Map[String, Aggregator]] = None,
                           timestampSpec: Option[TimestampSpec] = None,
                           druidVersion: String = null) extends DruidDataSourceCapability {

  import DruidDataSource._

  lazy val timeDimension: Option[DruidColumn] = columns.values.find {
    case c if c.name == INNER_TIME_COLUMN_NAME => true
    // c.name is always __time if it is the time dimension
    //case c if timestampSpec.isDefined && c.name == timestampSpec.get.column => true
    case _ => false
  }

  lazy val dimensions: IndexedSeq[DruidDimension] = columns.values.filter {
    case d: DruidDimension => true
    case _ => false
  }.map(_.asInstanceOf[DruidDimension]).toIndexedSeq

  lazy val metrics: Map[String, DruidMetric] = columns.values.filter {
    case m: DruidMetric => true
    case _ => false
  }.map(m => m.name -> m.asInstanceOf[DruidMetric]).toMap

  override def supportsBoundFilter: Boolean = DruidDataSourceCapability.supportsBoundFilter(druidVersion)

  def numDimensions = dimensions.size

  def indexOfDimensions(d: String): Int = {
    dimensions.indexWhere(_.name == d)
  }

  def metric(name: String): Option[DruidMetric] = metrics.get(name)
}

object DruidDataSource {

  val INNER_TIME_COLUMN_NAME = "__time"
  val TIMESTAMP_KEY_NAME = "timestamp"

  def apply(dataSource: String, mr: MetadataResponse,
            ins: List[Interval]): DruidDataSource = {

    val numRows = mr.getNumRows
    val timeTicks = mr.timeTicks(ins)

    val columns = mr.columns.map {
      case (name, columnDetails) => (name -> DruidColumn(name, columnDetails, numRows, timeTicks))
    }
    new DruidDataSource(dataSource, ins, columns, mr.size, numRows, timeTicks,
      mr.aggregators, mr.timestampSpec)
  }
}
