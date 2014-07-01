

import java.util.HashSet;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MultiTableBatchWriter;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * A set of configuration utils to read a Hadoop {@link Configuration} object and create Cloudbase/Accumulo objects.
 */
public class ConfigUtils {
    private static final Logger logger = Logger.getLogger(ConfigUtils.class);

    public static final String CLOUDBASE_AUTHS = "sc.cloudbase.authorizations";
    public static final String CLOUDBASE_INSTANCE = "sc.cloudbase.instancename";
    public static final String CLOUDBASE_ZOOKEEPERS = "sc.cloudbase.zookeepers";
    public static final String CLOUDBASE_USER = "sc.cloudbase.username";
    public static final String CLOUDBASE_PASSWORD = "sc.cloudbase.password";

    public static final String CLOUDBASE_WRITER_MAX_WRITE_THREADS = "sc.cloudbase.writer.maxwritethreads";
    public static final String CLOUDBASE_WRITER_MAX_LATENCY = "sc.cloudbase.writer.maxlatency";
    public static final String CLOUDBASE_WRITER_MAX_MEMORY = "sc.cloudbase.writer.maxmemory";

    public static final String FREE_TEXT_QUERY_TERM_LIMIT = "sc.freetext.querytermlimit";

    public static final String FREE_TEXT_DOC_TABLENAME = "sc.freetext.doctable";
    public static final String FREE_TEXT_TERM_TABLENAME = "sc.freetext.termtable";
    public static final String GEO_TABLENAME = "sc.geo.table";
    public static final String GEO_NUM_PARTITIONS = "sc.geo.numPartitions";

    public static final String USE_MOCK_INSTANCE = ".useMockInstance";

    public static final String NUM_PARTITIONS = "sc.cloudbase.numPartitions";

    private static final int WRITER_MAX_WRITE_THREADS = 1;
    private static final long WRITER_MAX_LATNECY = Long.MAX_VALUE;
    private static final long WRITER_MAX_MEMORY = 10000L;

    public static final String DISPLAY_QUERY_PLAN = "query.printqueryplan";

    public static final String FREETEXT_PREDICATES_LIST = "sc.freetext.predicates";
    public static final String FREETEXT_DOC_NUM_PARTITIONS = "sc.freetext.numPartitions.text";
    public static final String FREETEXT_TERM_NUM_PARTITIONS = "sc.freetext.numPartitions.term";

    public static final String TOKENIZER_CLASS = "sc.freetext.tokenizer.class";

    public static final String GEO_PREDICATES_LIST = "sc.geo.predicates";

    public static boolean isDisplayQueryPlan(Configuration conf){
        return conf.getBoolean(DISPLAY_QUERY_PLAN, false);
    }
    
    /**
     * get a value from the configuration file and throw an exception if the value does not exist.
     * 
     * @param conf
     * @param key
     * @return
     */
    private static String getStringCheckSet(Configuration conf, String key) {
        String value = conf.get(key);
        Validate.notNull(value, key + " not set");
        return value;
    }

    /**
     * @param conf
     * @param tablename
     * @return if the table was created
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableExistsException
     */
    public static boolean createTableIfNotExists(Configuration conf, String tablename) throws AccumuloException, AccumuloSecurityException,
            TableExistsException {
        TableOperations tops = getConnector(conf).tableOperations();
        if (!tops.exists(tablename)) {
            logger.info("Creating table: " + tablename);
            tops.create(tablename);
            return true;
        }
        return false;
    }

    public static String getFreeTextDocTablename(Configuration conf) {
        return getStringCheckSet(conf, FREE_TEXT_DOC_TABLENAME);
    }

    public static String getFreeTextTermTablename(Configuration conf) {
        return getStringCheckSet(conf, FREE_TEXT_TERM_TABLENAME);
    }

    public static int getFreeTextTermLimit(Configuration conf) {
        return conf.getInt(FREE_TEXT_QUERY_TERM_LIMIT, 100);
    }

    public static String getGeoTablename(Configuration conf) {
        return getStringCheckSet(conf, GEO_TABLENAME);
    }

    public static Set<URI> getFreeTextPredicates(Configuration conf) {
        return getPredicates(conf, FREETEXT_PREDICATES_LIST);
    }

    public static Set<URI> getGeoPredicates(Configuration conf) {
        return getPredicates(conf, GEO_PREDICATES_LIST);
    }

    private static Set<URI> getPredicates(Configuration conf, String confName) {
        String[] validPredicateStrings = conf.getStrings(confName, new String[] {});
        Set<URI> predicates = new HashSet<URI>();
        for (String prediateString : validPredicateStrings) {
            predicates.add(new URIImpl(prediateString));
        }
        return predicates;
    }

    public static BatchWriter createDefaultBatchWriter(String tablename, Configuration conf) throws TableNotFoundException,
            AccumuloException, AccumuloSecurityException {
        Long DEFAULT_MAX_MEMORY = getWriterMaxMemory(conf);
        Long DEFAULT_MAX_LATENCY = getWriterMaxLatency(conf);
        Integer DEFAULT_MAX_WRITE_THREADS = getWriterMaxWriteThreads(conf);
        Connector connector = ConfigUtils.getConnector(conf);
        return connector.createBatchWriter(tablename, DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
    }

    public static MultiTableBatchWriter createMultitableBatchWriter(Configuration conf) throws AccumuloException, AccumuloSecurityException {
        Long DEFAULT_MAX_MEMORY = getWriterMaxMemory(conf);
        Long DEFAULT_MAX_LATENCY = getWriterMaxLatency(conf);
        Integer DEFAULT_MAX_WRITE_THREADS = getWriterMaxWriteThreads(conf);
        Connector connector = ConfigUtils.getConnector(conf);
        return connector.createMultiTableBatchWriter(DEFAULT_MAX_MEMORY, DEFAULT_MAX_LATENCY, DEFAULT_MAX_WRITE_THREADS);
    }

    public static Scanner createScanner(String tablename, Configuration conf) throws AccumuloException, AccumuloSecurityException,
            TableNotFoundException {
        Connector connector = ConfigUtils.getConnector(conf);
        Authorizations auths = ConfigUtils.getAuthorizations(conf);
        return connector.createScanner(tablename, auths);

    }

    public static int getWriterMaxWriteThreads(Configuration conf) {
        return conf.getInt(CLOUDBASE_WRITER_MAX_WRITE_THREADS, WRITER_MAX_WRITE_THREADS);
    }

    public static long getWriterMaxLatency(Configuration conf) {
        return conf.getLong(CLOUDBASE_WRITER_MAX_LATENCY, WRITER_MAX_LATNECY);
    }

    public static long getWriterMaxMemory(Configuration conf) {
        return conf.getLong(CLOUDBASE_WRITER_MAX_MEMORY, WRITER_MAX_MEMORY);
    }

    public static String getUsername(JobContext job) {
        return getUsername(job.getConfiguration());
    }

    public static String getUsername(Configuration conf) {
        return conf.get(CLOUDBASE_USER);
    }

    public static Authorizations getAuthorizations(JobContext job) {
        return getAuthorizations(job.getConfiguration());
    }

    public static Authorizations getAuthorizations(Configuration conf) {
        String authString = conf.get(CLOUDBASE_AUTHS, "");
        if (authString.isEmpty()) {
            return new Authorizations();
        }
        return new Authorizations(authString.split(","));
    }

    public static Instance getInstance(JobContext job) {
        return getInstance(job.getConfiguration());
    }

    public static Instance getInstance(Configuration conf) {
        if (useMockInstance(conf)) {
            return new MockInstance(conf.get(CLOUDBASE_INSTANCE));
        }
        return new ZooKeeperInstance(conf.get(CLOUDBASE_INSTANCE), conf.get(CLOUDBASE_ZOOKEEPERS));
    }

    public static String getPassword(JobContext job) {
        return getPassword(job.getConfiguration());
    }

    public static String getPassword(Configuration conf) {
        return conf.get(CLOUDBASE_PASSWORD, "");
    }

    public static Connector getConnector(JobContext job) throws AccumuloException, AccumuloSecurityException {
        return getConnector(job.getConfiguration());
    }

    public static Connector getConnector(Configuration conf) throws AccumuloException, AccumuloSecurityException {
        Instance instance = ConfigUtils.getInstance(conf);

        return instance.getConnector(getUsername(conf), getPassword(conf));
    }

    public static boolean useMockInstance(Configuration conf) {
        return conf.getBoolean(USE_MOCK_INSTANCE, false);
    }

    private static int getNumPartitions(Configuration conf) {
        return conf.getInt(NUM_PARTITIONS, 25);
    }

    public static int getFreeTextDocNumPartitions(Configuration conf) {
        return conf.getInt(FREETEXT_DOC_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static int getFreeTextTermNumPartitions(Configuration conf) {
        return conf.getInt(FREETEXT_TERM_NUM_PARTITIONS, getNumPartitions(conf));
    }

    public static int getGeoNumPartitions(Configuration conf) {
        return conf.getInt(GEO_NUM_PARTITIONS, getNumPartitions(conf));
    }

    
}
