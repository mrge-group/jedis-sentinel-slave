package redis.clients.jedis;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.util.Pool;

/**
 * A abstraction over a sentinel configured master/slave pool. The master is
 * retrieved from sentinel on every write operation. The slave is retrieved from
 * sentinel as well. We are going to prefer slaves on the same physical host.
 * 
 * @author Shopping24 GmbH, Torsten Bøgh Köster (@tboeghk)
 */
public class JedisMasterSlavePool extends JedisSentinelPool {

   private final Logger logger = LoggerFactory.getLogger(getClass());

   private final Pool<Jedis> slavePool;
   private final Set<String> sentinels;
   private final String masterName;
   private HostAndPort currentSlave;

   /**
    * Create a new master slave pool
    */
   public JedisMasterSlavePool(GenericObjectPoolConfig poolConfig, String masterName, Set<String> sentinels,
         final int database) {
      super(masterName, sentinels, poolConfig, Protocol.DEFAULT_TIMEOUT * 5, null, database);

      this.masterName = masterName;
      this.sentinels = sentinels;
      this.slavePool = initClosestSlave(poolConfig.clone(), database);
   }

   /**
    * 
    */
   protected Pool<Jedis> initClosestSlave(GenericObjectPoolConfig poolConfig, int database) {
      Pool<Jedis> pool = null;

      try {
         // get ip addresses
         String myip = InetAddress.getLocalHost().getHostAddress();

         // we have some slaves. If we do not retrieve one on
         // localhost, store candidates here
         Set<URI> candidates = new HashSet<>();

         // iterate sentinels
         for (String sentinel : sentinels) {
            try (Jedis j = new Jedis("redis://" + sentinel)) {

               // retrieve slaves
               List<Map<String, String>> slaves = j.sentinelSlaves(masterName);
               if (!slaves.isEmpty()) {

                  for (Map<String, String> s : slaves) {
                     try {
                        URI candidate = new URI(
                              String.format(Locale.US, "redis://%s:%s/%s", s.get("ip"), s.get("port"), database));

                        if (myip.equals(s.get("ip"))) {
                           createSlavePool(poolConfig, candidate);
                        } else {
                           candidates.add(candidate);
                        }
                     } catch (Exception e) {
                        logger.warn(e.getMessage());
                     }
                  }

                  // we found anything and will not continue on to other
                  // sentinels as
                  // information will be identical
                  if (pool == null) {
                     pool = createSlavePool(poolConfig, candidates.iterator().next());
                  }
               }
            }
         }
      } catch (UnknownHostException e) {
         logger.error(e.getMessage());
      }

      // fallback is the master connection
      if (pool != null) {
         return pool;
      }
      return this;
   }

   /**
    * Takes care of building the real jedis pool.
    */
   protected JedisPool createSlavePool(GenericObjectPoolConfig poolConfig, URI candidate) {
      checkNotNull(poolConfig, "Pre-condition violated: poolConfig must not be null.");
      checkNotNull(candidate, "Pre-condition violated: candidate must not be null.");

      poolConfig.setJmxNamePrefix(candidate.toString());
      logger.info("Connecting to slave {} ...", candidate);

      // save host and port for later
      this.currentSlave = new HostAndPort(candidate.getHost(), candidate.getPort());

      // create pool
      return new JedisPool(poolConfig, candidate);
   }

   /**
    * Returns the currently confiugred slave
    */
   public HostAndPort getCurrentHostSlave() {
      return currentSlave;
   }

   /**
    * Returns a read only resource to a slave. If no slave is configured, the
    * master connection is returned.
    */
   public Jedis getSlaveResource() {
      return slavePool.getResource();
   }

   /**
    * Shutdown sentinel/master connections and the ro connections.
    */
   @Override
   public void destroy() {
      super.destroy();

      // destroy client pool as well
      if (slavePool != this) {
         slavePool.destroy();
      }
   }

}