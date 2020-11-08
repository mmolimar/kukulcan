package com.github.mmolimar.kukulcan.java;

import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class to convert class from Java to Scala and viceversa.
 */
public class KUtils {

    public static <K> scala.collection.Seq<K> toScalaSeq(List<K> list) {
        return JavaConverters.asScalaBuffer(list).toSeq();
    }

    public static <K, V> scala.collection.Map<K, V> toScalaMap(Map<K, V> map) {
        return JavaConverters.mapAsScalaMap(map);
    }

    public static <T> scala.Option<T> toScalaOption(T obj) {
        return scala.Option.apply(obj);
    }

    public static <K> List<K> toJavaList(scala.collection.Seq<K> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }

    public static <K, V> Map<K, V> toJavaMap(scala.collection.Map<K, V> map) {
        return JavaConverters.mapAsJavaMap(map);
    }

    public static <K> Optional<K> toJavaOptional(scala.Option<K> opt) {
        return Optional.ofNullable(opt.getOrElse(() -> null));
    }

}
