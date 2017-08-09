import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark._
import org.apache.spark.graphx._
object GoogleBFS
{
    def main(args: Array[String])
    {
        val tasks = args(0).toInt
        val sc = new SparkContext(new SparkConf().setAppName("GoogleBFS"))

        // val graph = GraphLoader.edgeListFile(sc,"hdfs://moonshot-ha-nameservice/user/dae30/input/googlegraph/webgoogle.txt", numEdgePartitions = tasks).partitionBy(PartitionStrategy.RandomVertexCut)
        // val graph = GraphLoader.edgeListFile(sc,"hdfs://moonshot-ha-nameservice/user/dae30/input/googlegraph/tenth.txt", numEdgePartitions = tasks).partitionBy(PartitionStrategy.RandomVertexCut)
        val graph = GraphLoader.edgeListFile(sc,"hdfs://moonshot-ha-nameservice/user/dae30/input/googlegraph/hundredth.txt", numEdgePartitions = tasks).partitionBy(PartitionStrategy.RandomVertexCut)

        val root: VertexId = 1234

        val initialGraph = graph.mapVertices((id, _) => if (id == root) 0.0 else Double.PositiveInfinity)

        val findDistance = initialGraph.pregel(Double.PositiveInfinity)( (id, attr, msg) => math.min(attr, msg), triplet => { if (triplet.srcAttr != Double.PositiveInfinity && triplet.dstAttr == Double.PositiveInfinity ) { Iterator((triplet.dstId, triplet.srcAttr+1)) } else { Iterator.empty } }, (a,b) => math.min(a,b) )

        val subgraph = findDistance.subgraph(vpred = (vid, attr) => attr < Double.PositiveInfinity)

        val canReach = subgraph.numVertices

        // val inmem = canReach.persist()
        // inmem.saveAsTextFile("hdfs://moonshot-ha-nameservice/user/dae30/spark")
    }
}
