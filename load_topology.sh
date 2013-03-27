#!/bin/bash

mvn compile && mvn package

/home/maphysics/Downloads/storm-0.9.0-wip16/bin/storm jar target/storm-starter-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.starter.ReverseStringTopology test_topology -c nimbus.host=localhost

