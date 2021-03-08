package com.github.mmolimar.kukulcan.java;

import scala.Predef;
import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class to convert class from Java to Scala and viceversa.
 */
public class KUtils {

    /**
     * Convert a Java list to a Scala immutable Seq.
     *
     * @param list The Java {@code List[K]} to convert.
     * @return the immutable Scala {@code Seq[K]} converted.
     */
    public static <K> scala.collection.immutable.Seq<K> toScalaSeq(List<K> list) {
        return JavaConverters.asScalaBufferConverter(list).asScala().toList();
    }

    /**
     * Convert a Java map to a Scala immutable Map.
     *
     * @param map The Java {@code Map[K, V]} to convert.
     * @return the immutable Scala {@code Map[K, V]} converted.
     */
    public static <K, V> scala.collection.immutable.Map<K, V> toScalaMap(Map<K, V> map) {
        return JavaConverters.mapAsScalaMap(map).toMap(Predef.$conforms());
    }

    /**
     * Convert a Scala seq to a Java list.
     *
     * @param seq The Scala {@code Seq[K]} to convert.
     * @return the Java {@code List[K]} converted.
     */
    public static <K> List<K> toJavaList(scala.collection.Seq<K> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }

    /**
     * Convert a Scala map to a Java map.
     *
     * @param map The Scala {@code Map[K, V]} to convert.
     * @return the Java {@code Map[K, V]} converted.
     */
    public static <K, V> Map<K, V> toJavaMap(scala.collection.Map<K, V> map) {
        return JavaConverters.mapAsJavaMap(map);
    }

    /**
     * Convert a Scala option to a Java optional.
     *
     * @param opt The Scala {@code Option[K]} to convert.
     * @return the Java {@code Optional[K]} converted.
     */
    public static <K> Optional<K> toJavaOption(scala.Option<K> opt) {
        return Optional.ofNullable(opt.getOrElse(() -> null));
    }

    /**
     * Create a Scala option for an object.
     *
     * @param obj The object to create its Scala option.
     * @return the Scala {@code Option[K]}.
     */
    public static <K> scala.Option<K> scalaOption(K obj) {
        return scala.Option.apply(obj);
    }

}
