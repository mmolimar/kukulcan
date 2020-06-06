package com.github.mmolimar.kukulcan.java;

import scala.collection.JavaConverters;

import java.util.List;
import java.util.Map;
import java.util.Optional;

class KUtils {

    static <K> scala.collection.Seq<K> toScalaSeq(List<K> list) {
        return JavaConverters.asScalaIteratorConverter(list.iterator()).asScala().toSeq();
    }

    static <K, V> scala.collection.Map<K, V> toScalaMap(Map<K, V> map) {
        return JavaConverters.mapAsScalaMap(map);
    }

    static <K, V> Map<K, V> toJavaMap(scala.collection.Map<K, V> map) {
        return JavaConverters.mapAsJavaMap(map);
    }

    static <K> List<K> toJavaList(scala.collection.Seq<K> seq) {
        return JavaConverters.seqAsJavaList(seq);
    }

    static <K> Optional<K> toJavaOptional(scala.Option<K> opt) {
        return Optional.ofNullable(opt.getOrElse(() -> null));
    }

    static <T> scala.Option<T> toScalaOption(T obj) {
        return scala.Option.apply(obj);
    }

}
