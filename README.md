# kukulcan [![Build Status](https://circleci.com/gh/mmolimar/kukulcan.svg?style=shield)](https://circleci.com/gh/mmolimar/kukulcan)

*K'uk'ulkan* ("Feathered Serpent") is the name of a deity which was worshipped by the Yucatec maya people. You can
read a lot more in books or on the Internet about it and will see that, in someways, is related to the wind and water.

Beyond the origin of this name I reused to name this project, Kukulcan provides an API and different
sort of [REPLs](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) to interact with your streams 
or administer your [Apache Kafka](https://kafka.apache.org) deployment.

It supports POSIX and Windows OS and Scala, Java and Python programming languages.

![](/docs/img/kukulcan.png)

## Motivation

As Kafka users (developers, devops, etc.), we have to interact with it in different ways. Maybe using the
command-line tools it provides, an external tool to look for a specific info, an IDE to develop a snippet
in order to test something... but what if you could do all those things in the same interface with the
flexibility of a REPL to do exactly what you need?

This is the aim of this project: enabling a simple interface to interact with your Kafka cluster taking advantage
of its rich API plus some additional utils included and make your interaction better.

## Getting started

### Requirements

Before starting, you'll need to have installed JDK 11 and [SBT](https://www.scala-sbt.org/). Additionally,
if you want to use the PyKukulcan REPL, you'll also need Python (supported 2.7, 3.4, 3.5, 3.6, 3.7 and 3.8 versions)
and [pip](https://pypi.org/project/pip) installed.

### Building from source

Just clone the ``kukulcan`` repo and *"kukulcan-it"*:

``git clone https://github.com/mmolimar/kukulcan.git && cd kukulcan``

``sbt kukulcan``

### Configuration

In the ``config`` directory, you'll find some files with the configurations for each of the APIs Kukulcan provides.
All the possible configs for each API are in these files with their description so, if you need to set some
specific configs for your environment, you should set them there before starting.

Also, there is an important environment variable named **``KUKULCAN_HOME``**. If not set, its default value will be
the root folder of the cloned project. When starting a Kukulcan REPL, it will look for this environment variable
and read the properties files contained in its ``config`` subdirectory.

## Modules

The project contains three subprojects or modules described below but, if you generate the Scala docs executing
``sbt doc`` in the command line, you'll be able to see a more detailed description.

### kukulcan-api

Contains all the Scala and Java classes to interact with Kafka and extending the Kafka API to enable new
functionalities. This API contains:

* **KAdmin**: grouped utils for administrative operations for topics, configs, ACLs and metrics.
* **KConnect**: methods to execute requests against Kafka Connect.
* **KConsumer** and **KProducer**: Kafka consumer/producer with some extra features.
* **KStreams**: extends Kafka Streams to see how your topology is (printing it in a graph).
* **KSchemaRegistry**: to interact with Confluent Schema Registry.
* **KKsql**: client for querying KSQL server and integrated with the KSQL-CLI.

### kukulcan-repl

Enables two sort of entry points for the REPLs: one based on the [Scala REPL](https://docs.scala-lang.org/overviews/repl/overview.html)
and the other based on [JShell](https://docs.oracle.com/javase/9/jshell).

Additionally, it includes the logic to read and ``reload`` your configurations stored the ``config`` directory
transparently.

### pykukulcan

This module includes the needed bindings to use Kukulcan with Python via [py4j](https://www.py4j.org).
By now, the only bindings available are for the REPL (in Java).

## Running the REPL

Kukulcan takes advantage of multiple functionalities we already have in Java, Scala and other tools to build a
custom REPL. Actually, the project enables four type of REPLs with Ammonite, Scala, JShell and Python.

After building the source, you'll be able to start any of the following REPL options. Here are a few examples
you can do with Kukulcan:

- Graphical representation of a topology in Kafka Streams:

  ![](/docs/img/kstreams.png)

- Kafka Connect interaction:

  ![](/docs/img/kconnect.png)

> **NOTE**: The REPLs have already the Kukulcan imports: ``com.github.mmolimar.kukulcan`` in case of the
Scala and Ammonite REPLs and ``com.github.mmolimar.kukulcan.Kukulcan`` in case of the JShell REPL. So you just
have to start typing ``kukulcan.<option>.`` or ``Kukulcan.<option>.`` respectively.


### Kukulcan Ammonite REPL

[Ammonite](https://ammonite.io) is an improved Scala REPL with a lot of interesting features you can find very
useful to load scripts or even code in an easier way. Obviously, you must install it previously.

It already includes all dependencies, and the Kukulcan Scala API to interact with Kafka.

> **NOTE**: If you're going to use Kukulcan with Ammonite, you'll have to publish the project in local, executing
  ``sbt publishLocal``.

To execute this REPL you just have to type:

``./bin/kukulcan-amm``

I do recommend using this REPL even though the other ones are fine as well. The only drawback here is that
Ammonite-REPL does not support Windows systems.

### Kukulcan Scala REPL

A Scala REPL including all dependencies and the Kukulcan Scala API to interact with Kafka.

To execute this REPL you just have to type:

``./bin/kukulcan``

For Windows OS:

``bin\kukulcan.cmd``

### Kukulcan JShell REPL

An enriched Java JShell to interact with Kafka including all dependencies and imports from the Kukulcan Java API.

To execute this REPL you just have to type:

``./bin/jkukulcan``

For Windows OS:

``bin\jkukulcan.cmd``

### PyKukulcan REPL

A Python shell including the needed bindings with Kukulcan Scala API via [Py4J](https://www.py4j.or).

> **NOTE**: If you're going to use the PyKukulcan REPL, you must have Python and pip installed. Then, install
``pykukulcan`` in this way: ``pip install python/ -r python/requirements.txt``

To execute this REPL you just have to type:

``./bin/pykukulcan``

For Windows OS:

``bin\pykukulcan.cmd``

## TODO's

- [ ] Tools in the Admin API.
- [ ] Integration with REST Proxy.
- [ ] API for Python.

## Contribute

- Source Code: https://github.com/mmolimar/kukulcan
- Issue Tracker: https://github.com/mmolimar/kukulcan/issues

## License

Released under the Apache License, version 2.0.
