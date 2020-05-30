package com.github.mmolimar.kukulcan

import java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.tools.{ToolsUtils => JToolsUtils}

import scala.collection.JavaConverters._

private[kukulcan] object KStreams extends Api[Topology => KStreams]("streams") {

  override protected def createInstance(props: Properties): Topology => KStreams = {
    topology: Topology => KStreams(topology, props)
  }

}

private[kukulcan] case class KStreams(topology: Topology, props: Properties) extends KafkaStreams(topology, props) {

  import com.github.mdr.ascii.graph.Graph
  import com.github.mdr.ascii.layout._
  import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
  import graphs._
  import org.apache.kafka.common.{Metric, MetricName}
  import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder._
  import org.apache.kafka.streams.{StreamsConfig, TopologyDescription}

  def reload(): Unit = KStreams.reload()

  def withApplicationId(applicationId: String): KStreams = {
    val newProps = new Properties()
    newProps.putAll(props)
    newProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    copy(props = newProps)
  }

  def withProperties(props: Properties): KStreams = copy(props = props)

  def getMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Map[MetricName, Metric] = {
    metrics.asScala
      .filter(metric => metric._1.group.matches(groupRegex) && metric._1.name.matches(nameRegex))
      .toMap
  }

  def listMetrics(groupRegex: String = ".*", nameRegex: String = ".*"): Unit = {
    JToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex).asJava)
  }

  private def successorEdges(nodes: java.util.Set[TopologyDescription.Node]): List[(String, String)] = {
    def nodeEdges(node: TopologyDescription.Node): List[(String, String)] = {
      node.successors.asScala.flatMap(successor => List(node.name -> successor.name) ++ nodeEdges(successor)).toList
    }

    nodes.asScala.flatMap { node =>
      val extra = node match {
        case source: Source => List(source.topicSet.asScala.mkString(",") -> source.name)
        case sink: Sink => List(sink.name -> sink.topic)
        case processor: Processor => processor.stores.asScala.map(s => processor.name -> s).toList
        case _ => List.empty
      }
      nodeEdges(node) ++ extra ++ successorEdges(node.successors)
    }.toList
  }

  private def topologyGraph(subtopologies: List[TopologyDescription.Subtopology]): KGraph[KGraphTopology[KGraph[String]]] = {
    val subtopologiesGraph = subtopologies.map { subtopology =>
      val edges = successorEdges(subtopology.nodes)
      val vertices = edges.flatMap(e => List(e._1, e._2)).toSet
      new KGraphSubtopology(id = subtopology.id, vertices = vertices, edges = edges)
    }
    val nodesGraph = subtopologies.flatMap(_.nodes.asScala.toList.flatMap {
      case source: Source =>
        source.topicSet.asScala.map(topic =>
          new KGraphNodeSource(source = topic, target = source.name)
        )
      case sink: Sink =>
        List(new KGraphNodeTarget(source = sink.name, target = sink.topic))
      case processor: Processor =>
        processor.stores.asScala.map(store =>
          new KGraphNodeProcessor(source = processor.name, target = store)
        )
      case _ => List.empty
    })

    val graph = subtopologiesGraph ++ nodesGraph
    val graphEdges = graph.flatMap {
      case node: KGraphNodeSource =>
        subtopologiesGraph.filter(g => g.vertices.contains(node.target)).map(target => (node, target))
      case node: KGraphNodeTarget =>
        subtopologiesGraph.filter(g => g.vertices.contains(node.source)).map(source => (source, node))
      case node: KGraphNodeProcessor =>
        subtopologiesGraph.filter(g => g.vertices.contains(node.source)).map(source => (source, node))
      case _ => List.empty
    }

    val aggregator = new KGraphTopology(vertices = graph.toSet, edges = graphEdges)
    new KGraph(vertices = Set(aggregator), edges = List.empty)
  }

  def printTopology(subtopologies: Boolean = true, globaStores: Boolean = true): Unit = {
    val description = topology.describe()
    if (subtopologies) {
      println(topologyGraph(description.subtopologies.asScala.toList))
    }
    if (globaStores) {

    }
  }

  private object graphs {

    class KGraph[V](
                     override val vertices: Set[V],
                     override val edges: List[(V, V)]
                   ) extends Graph(vertices = vertices, edges = edges) {

      def layout: LayoutPrefsImpl = LayoutPrefsImpl(doubleVertices = true)

      override def toString: String = {
        GraphLayout.renderGraph(this, layoutPrefs = layout.copy(doubleVertices = true, rounded = false))
      }

      override def equals(obj: Any): Boolean = {
        if (obj != null && this.getClass == this.getClass) {
          this.hashCode == obj.hashCode
        } else {
          false
        }
      }
    }

    class KGraphTopology[V](
                             override val vertices: Set[V],
                             override val edges: List[(V, V)]
                           ) extends KGraph(vertices = vertices, edges = edges) {

      override def toString: String = {
        s"Topologies:\n\n" +
          GraphLayout.renderGraph(this, layoutPrefs = layout.copy(doubleVertices = true, rounded = false))
      }
    }

    class KGraphSubtopology[V](
                                val id: Int,
                                override val vertices: Set[V],
                                override val edges: List[(V, V)]
                              ) extends KGraphTopology(vertices = vertices, edges = edges) {

      override def toString: String = {
        s"Sub-topology: $id\n\n" +
          GraphLayout.renderGraph(this, layoutPrefs = layout.copy(doubleVertices = false, rounded = false))
      }

      override lazy val hashCode = vertices.## + edges.## + id.hashCode

    }

    abstract class KGraphNode(val name: String) extends KGraph[String](vertices = Set(name), edges = List.empty) {

      def title: String = name

      override def toString: String = {
        GraphLayout.renderGraph(this, layoutPrefs = layout.copy(unicode = false, doubleVertices = false, rounded = false))
      }

      override lazy val hashCode = vertices.## + edges.## + title.hashCode

    }

    class KGraphNodeSource(val source: String, val target: String) extends KGraphNode(name = source)

    class KGraphNodeTarget(val source: String, val target: String) extends KGraphNode(name = target)

    class KGraphNodeProcessor(val source: String, val target: String) extends KGraphNode(name = target)

  }

}
