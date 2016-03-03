# Jedis-Sentinel-Slave

This extension to [Jedis](https://github.com/xetorthio/jedis) adds a special Jedis pool, whose members 
(masters & slave) are controlled by a Redis Sentinel instance. The `JedisMasterSlavePool` provides you
a `getSlaveResource()` method to open a connection to a Redis slave.

## Roadmap 

The `JedisMasterSlavePool` needs some polishing. Goal is to provide a list of all current slaves
and a connection to the nearest slave, either identified by hostname or ping latency.

## The extension is available on maven central

    <dependency>
       <groupId>com.s24..util</groupId>
       <artifactId>jedis-sentinel-slave</artifactId>
       <version>0.0.1</version>
    </dependency>

## License

This project is licensed under the [Apache License, Version 2](http://www.apache.org/licenses/LICENSE-2.0.html).

