import org.apache.spark._ 
import org.apache.spark.graphx._ 
import org.apache.spark.rdd.RDD

class VertexProperty()
case class UserProperty(val name: String)
extends VertexProperty
case class ProductProperty(val name: String, val price: Double)
extends VertexProperty
var graph: Graph[VertexProperty, String] = null
// Assume the SparkContext has already been constructed
val sc: SparkContext
// Create an RDD for the vertices
val users: RDD[(VertexId, (String, String))] =
sc.parallelize(Array((3L, ("rxin", "student")), 
(7L, ("jgonzal", "postdoc")), 
(5L, ("franklin", "prof")), 
(2L, ("istoica", "prof"))))
val relationships: RDD[Edge[String]] = 
    sc.parallelize(Array(Edge(3L, 7L, "collab"), 
    Edge(5L, 3L,"advisor"), Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
// Define a default user in case there are relationship
 with missing user val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser)

val graph: Graph[(String, String), String] // Constructed from above
// Count all users which are postdocs graph.vertices.filter
{ case (id, (name, pos)) => pos == "postdoc" }.count 
// Count all the edges where src > dst 
graph.edges.filter(e => e.srcId > e.dstId).count
 

graph.vertices.filter
{ case (id, (name, pos)) => pos == "postdoc" }.count
 