package com.github.mmolimar.kukulcan

import _root_.java.util.Properties

import org.apache.kafka.streams.{KafkaStreams, Topology}
import org.apache.kafka.tools.{ToolsUtils => JToolsUtils}

import scala.collection.JavaConverters._

/**
 * Factory for [[com.github.mmolimar.kukulcan.KStreams]] instances.
 *
 */
object KStreams {

  def apply(topology: Topology, props: Properties): KStreams = new KStreams(topology, props)

}

/**
 * An enriched implementation of the {@code org.apache.kafka.streams.KafkaStreams} class to
 * manages streams in Kafka.
 *
 * @param topology Topology specifying the computational logic.
 * @param props    Properties with the configuration.
 */
class KStreams(val topology: Topology, val props: Properties) extends KafkaStreams(topology, props) {

  import _root_.java.util.{Set => JSet}

  import com.github.mdr.ascii.graph.Graph
  import com.github.mdr.ascii.layout._
  import com.github.mdr.ascii.layout.prefs.LayoutPrefsImpl
  import kgraphs.NodeType._
  import kgraphs._
  import org.apache.kafka.common.{Metric, MetricName}
  import org.apache.kafka.streams.processor.internals.InternalTopologyBuilder._
  import org.apache.kafka.streams.{StreamsConfig, TopologyDescription}

  import scala.collection.mutable

  /**
   * Create new instance with a new application id
   *
   * @param applicationId The new application id.
   * @return The new instance created.
   */
  def withApplicationId(applicationId: String): KStreams = {
    val newProps = new Properties()
    props.asScala.foreach(p => newProps.put(p._1, p._2))
    newProps.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId)
    new KStreams(topology = this.topology, props = newProps)
  }

  /**
   * Create new instance with the properties specified.
   *
   * @param props The properties to create the instance.
   * @return The new instance created.
   */
  def withProperties(props: Properties): KStreams = new KStreams(topology = this.topology, props = props)

  /**
   * Get all metrics registered.
   *
   * @return a { @code Map} with the all metrics registered.
   */
  def getMetrics: Map[MetricName, Metric] = {
    getMetrics(".*", ".*")
  }

  /**
   * Get all metrics registered filtered by the group and name regular expressions.
   *
   * @param groupRegex Regex to filter metrics by group name.
   * @param nameRegex  Regex to filter metrics by its name.
   * @return a { @code Map} with the all metrics registered filtered by the group and name regular expressions.
   */
  def getMetrics(groupRegex: String, nameRegex: String): Map[MetricName, Metric] = {
    metrics.asScala
      .filter(metric => metric._1.group.matches(groupRegex) && metric._1.name.matches(nameRegex))
      .toMap
  }

  /**
   * Print all metrics.
   */
  def listMetrics(): Unit = {
    listMetrics(".*", ".*")
  }

  /**
   * Print all metrics filtered by the group and name regular expressions.
   *
   * @param groupRegex Regex to filter metrics by group name.
   * @param nameRegex  Regex to filter metrics by its name.
   */
  def listMetrics(groupRegex: String, nameRegex: String): Unit = {
    JToolsUtils.printMetrics(getMetrics(groupRegex, nameRegex).asJava)
  }

  /**
   * Print the topology in an ASCII graph.
   *
   * @param subtopologies If to include the subtopologies.
   * @param globalStores  If to include the global stores.
   */
  def printTopology(subtopologies: Boolean = true, globalStores: Boolean = true): Unit = {
    val description = topology.describe()
    val graphs = mutable.Set[KGraphTopology[KGraph[String]]]()

    if (subtopologies) {
      val graph = topologyGraph(description.subtopologies.asScala.toList)
      if (graph.vertices.nonEmpty) graphs.add(graph)
    }
    if (globalStores) {
      val graph = topologyGlobalStores(description.globalStores.asScala.toList)
      if (graph.vertices.nonEmpty) graphs.add(graph)
    }
    println(new KGraph(vertices = graphs.toSet, edges = List.empty))
  }

  private def successorEdges(nodes: JSet[TopologyDescription.Node]): List[(String, String)] = {
    def nodeEdges(node: TopologyDescription.Node): List[(String, String)] = {
      node.successors.asScala.flatMap(successor => List(node.name -> successor.name) ++ nodeEdges(successor)).toList
    }

    nodes.asScala.flatMap { node =>
      val extra = node match {
        case source: Source =>
          val topics = Option(source.topicSet).map(_.asScala.map(topic => topic -> source.name).toList).getOrElse(List.empty)
          val pattern = Option(source.topicPattern).map(tp => tp.pattern -> source.name).toList
          topics ++ pattern
        case sink: Sink =>
          Option(sink.topic).map(t => sink.name -> t).toList ++
            Option(sink.topicNameExtractor)
              .map(t => sink.name -> s"EXTRACTOR[${t.hashCode}]").toList
        case processor: Processor =>
          processor.stores.asScala.map(s => processor.name -> s).toList
        case _ => List.empty
      }
      nodeEdges(node) ++ extra ++ successorEdges(node.successors)
    }.toList
  }

  private def toKGraph(node: TopologyDescription.Node): List[KGraphNode] = node match {
    case source: Source =>
      val topics = Option(source.topicSet).map(_.asScala.map(topic =>
        new KGraphNodeSource(source = topic, target = source.name, TOPIC)
      )).getOrElse(List.empty)
      val pattern = Option(source.topicPattern).map(tp =>
        new KGraphNodeSource(source = tp.pattern, target = source.name, TOPIC_PATTERN)
      ).toList
      (topics ++ pattern).toList
    case sink: Sink =>
      Option(sink.topic).map(t => new KGraphNodeSink(source = sink.name, target = t, TOPIC)).toList ++
        Option(sink.topicNameExtractor)
          .map(t => new KGraphNodeSink(source = sink.name, target = s"EXTRACTOR[${t.hashCode}]", TOPIC_EXTRACTOR)).toList
    case processor: Processor =>
      processor.stores.asScala.map(store =>
        new KGraphNodeProcessor(source = processor.name, target = store, STORE)
      ).toList
    case _ => List.empty
  }

  private def edgesFromGraph(graph: List[KGraph[String]], subtopologies: List[KGraphSubtopology[String]]): List[(KGraph[String], KGraph[String])] = {
    graph.flatMap {
      case node: KGraphNodeSource =>
        subtopologies.filter(g => g.vertices.contains(node.target)).map(target => (node, target))
      case node: KGraphNodeSink =>
        subtopologies.filter(g => g.vertices.contains(node.source)).map(source => (source, node))
      case node: KGraphNodeProcessor =>
        subtopologies.filter(g => g.vertices.contains(node.source)).map(source => (source, node))
      case subtopology: KGraphSubtopology[String] =>
        graph.filter(g => g.isInstanceOf[KGraphNode]).flatMap {
          case node: KGraphNodeSource if subtopology.vertices.contains(node.source) &&
            !subtopology.vertices.contains(node.target) =>
            List((subtopology, node))
          case node: KGraphNodeSink if !subtopology.vertices.contains(node.source) &&
            subtopology.vertices.contains(node.target) =>
            List((node, subtopology))
          case node: KGraphNodeProcessor if !subtopology.vertices.contains(node.source) &&
            subtopology.vertices.contains(node.target) =>
            List((node, subtopology))
          case _ =>
            List.empty
        }
      case _ => List.empty
    }.distinct
  }

  private def topologyGraph(subtopologies: List[TopologyDescription.Subtopology]): KGraphTopology[KGraph[String]] = {
    val subtopologiesGraph = subtopologies.map { subtopology =>
      val edges = successorEdges(subtopology.nodes)
      val vertices = edges.flatMap(e => List(e._1, e._2)).toSet
      new KGraphSubtopology(id = subtopology.id, vertices = vertices, edges = edges)
    }
    val nodesGraph = subtopologies.flatMap(_.nodes.asScala.toList.flatMap(toKGraph)).toSet
    val graph = subtopologiesGraph ++ nodesGraph
    val graphEdges = edgesFromGraph(graph, subtopologiesGraph)

    new KGraphTopology(vertices = graph.toSet, edges = graphEdges)
  }

  private def topologyGlobalStores(globalStores: List[TopologyDescription.GlobalStore]): KGraphTopology[KGraph[String]] = {
    val globalStoresGraph = globalStores.map { globalStore =>
      val edges = {
        val processorEdges = {
          globalStore.processor.successors.asScala
            .map(s => globalStore.processor.name -> s.name).toList ++ successorEdges(globalStore.processor.successors)
        }
        val sourceEdges = {
          Option(globalStore.source.topicSet)
            .map(_.asScala.map(topic => topic -> globalStore.source.name).toList).getOrElse(List.empty) ++
            Option(globalStore.source.topicPattern).map(tp => tp.pattern -> globalStore.source.name).toList ++
            globalStore.source.successors.asScala.map(s => globalStore.source.name -> s.name).toList ++
            successorEdges(globalStore.source.successors)
        }
        processorEdges ++ sourceEdges
      }
      val vertices = edges.flatMap(e => List(e._1, e._2)).toSet
      new KGraphSubtopologyGlobalStore(id = globalStore.id, vertices = vertices, edges = edges)
    }
    val nodesGraph = globalStores.flatMap { gs =>
      gs.processor.successors.asScala.map(toKGraph) ++ gs.source.successors.asScala.map(toKGraph) ++
        List(
          Option(gs.source.topicSet).map(_.asScala
            .map(topic => new KGraphNodeSource(source = topic, target = gs.source.name, TOPIC)).toList)
            .getOrElse(List.empty),
          Option(gs.source.topicPattern)
            .map(tp => new KGraphNodeSource(source = tp.pattern, target = gs.processor.name, TOPIC_PATTERN)).toList
        )
    }.flatten

    val graph = globalStoresGraph ++ nodesGraph
    val graphEdges = edgesFromGraph(graph, globalStoresGraph)

    new KGraphTopology(vertices = graph.toSet, edges = graphEdges)
  }

  private object kgraphs {

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

      override lazy val hashCode: Int = vertices.## + edges.## + id.hashCode

    }

    class KGraphSubtopologyGlobalStore[V](
                                           override val id: Int,
                                           override val vertices: Set[V],
                                           override val edges: List[(V, V)]
                                         ) extends KGraphSubtopology(id = id, vertices = vertices, edges = edges) {

      override def toString: String = {
        s"Sub-topology: $id\nfor global store (will not generate tasks)\n\n" +
          GraphLayout.renderGraph(this, layoutPrefs = layout.copy(doubleVertices = false, rounded = false))
      }

    }

    sealed trait NodeType

    object NodeType {

      case object TOPIC extends NodeType

      case object TOPIC_PATTERN extends NodeType

      case object TOPIC_EXTRACTOR extends NodeType

      case object STORE extends NodeType

    }

    abstract class KGraphNode(val name: String) extends KGraph[String](vertices = Set(name), edges = List.empty) {

      def title: String = name

      def nodeType: NodeType

      override def toString: String = {
        GraphLayout.renderGraph(this, layoutPrefs = layout.copy(unicode = false, doubleVertices = false, rounded = false)) +
          s"\n$nodeType"
      }

      override lazy val hashCode: Int = vertices.## + edges.## + title.hashCode

    }

    class KGraphNodeSource(val source: String, val target: String, val nodeType: NodeType) extends KGraphNode(name = source)

    class KGraphNodeSink(val source: String, val target: String, val nodeType: NodeType) extends KGraphNode(name = target)

    class KGraphNodeProcessor(val source: String, val target: String, val nodeType: NodeType) extends KGraphNode(name = target)

  }

}
