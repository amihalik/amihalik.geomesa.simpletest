
import geomesa.core.data.AccumuloDataStore;
import geomesa.core.index.Constants;
import info.aduna.iteration.CloseableIteration;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.DataUtilities;
import org.geotools.data.FeatureSource;
import org.geotools.data.FeatureStore;
import org.geotools.data.Query;
import org.geotools.factory.Hints;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.FeatureIterator;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

/**
 * A {@link GeoIndexer} wrapper around a GeoMesa {@link AccumuloDataStore}. This class configures and connects to the Datastore, creates the
 * RDF Feature Type, and interacts with the Datastore.
 * <p>
 * Specifically, this class creates a RDF Feature type and stores each RDF Statement as a RDF Feature in the datastore. Each feature
 * contains the standard set of GeoMesa attributes (Geometry, Start Date, and End Date). The GeoMesaGeoIndexer populates the Geometry
 * attribute by parsing the Well-Known Text contained in the RDF Statement’s object literal value.
 * <p>
 * The RDF Feature contains four additional attributes for each component of the RDF Statement. These attributes are:
 * <p>
 * <table border="1">
 * <tr>
 * <th>Name</th>
 * <th>Symbol</th>
 * <th>Type</th>
 * </tr>
 * <tr>
 * <td>Subject Attribute</td>
 * <td>S</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Predicate Attribute</td>
 * <td>P</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Object Attribute</td>
 * <td>O</td>
 * <td>String</td>
 * </tr>
 * </tr>
 * <tr>
 * <td>Context Attribute</td>
 * <td>C</td>
 * <td>String</td>
 * </tr>
 * </table>
 */
public class GeoMesaGeoIndexer implements GeoIndexer {

    private static final Logger logger = Logger.getLogger(GeoMesaGeoIndexer.class);

    private static final String FEATURE_NAME = "RDF";

    private static final String SUBJECT_ATTRIBUTE = "S";
    private static final String PREDICATE_ATTRIBUTE = "P";
    private static final String OBJECT_ATTRIBUTE = "O";
    private static final String CONTEXT_ATTRIBUTE = "C";

    private Set<URI> validPredicates;

    private FeatureStore<SimpleFeatureType, SimpleFeature> featureStore;
    private FeatureSource<SimpleFeatureType, SimpleFeature> featureSource;
    private SimpleFeatureType featureType;

    public GeoMesaGeoIndexer(Configuration conf) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
            SchemaException {
        configure(conf);
    }

    private void configure(Configuration conf) throws AccumuloException, AccumuloSecurityException, TableNotFoundException, IOException,
            SchemaException {
        validPredicates = ConfigUtils.getGeoPredicates(conf);

        DataStore dataStore = createDataStore(conf);
        featureType = getStatementFeatureType(dataStore);

        featureSource = dataStore.getFeatureSource(featureType.getName());
        if (!(featureSource instanceof FeatureStore))
            throw new IllegalStateException("Could not retrieve feature store");
        featureStore = (FeatureStore<SimpleFeatureType, SimpleFeature>) featureSource;
    }

    private static DataStore createDataStore(Configuration conf) throws IOException {
        // get the configuration parameters
        Instance instance = ConfigUtils.getInstance(conf);
        boolean useMock = instance instanceof MockInstance;
        String instanceId = instance.getInstanceName();
        String zookeepers = instance.getZooKeepers();
        String user = ConfigUtils.getUsername(conf);
        String password = ConfigUtils.getPassword(conf);
        String auths = ConfigUtils.getAuthorizations(conf).toString();
        String tableName = ConfigUtils.getGeoTablename(conf);
        int numParitions = ConfigUtils.getGeoNumPartitions(conf);

        String featureSchemaFormat = "%~#s%" + numParitions + "#r%" + FEATURE_NAME + "#cstr%0,3#gh%yyyyMMdd#d::%~#s%3,2#gh::%~#s%#id";
        // build the map of parameters
        Map<String, Serializable> params = new HashMap<String, Serializable>();
        params.put("instanceId", instanceId);
        params.put("zookeepers", zookeepers);
        params.put("user", user);
        params.put("password", password);
        params.put("auths", auths);
        params.put("tableName", tableName);
        params.put("indexSchemaFormat", featureSchemaFormat);
        params.put("useMock", Boolean.toString(useMock));

        // fetch the data store from the finder
        return DataStoreFinder.getDataStore(params);
    }

    private static SimpleFeatureType getStatementFeatureType(DataStore dataStore) throws IOException, SchemaException {
        SimpleFeatureType featureType;

        String[] datastoreFeatures = dataStore.getTypeNames();
        if (Arrays.asList(datastoreFeatures).contains(FEATURE_NAME)) {
            featureType = dataStore.getSchema(FEATURE_NAME);
        } else {
            String featureSchema = SUBJECT_ATTRIBUTE + ":String," //
                    + PREDICATE_ATTRIBUTE + ":String," //
                    + OBJECT_ATTRIBUTE + ":String," //
                    + CONTEXT_ATTRIBUTE + ":String," //
                    + Constants.SF_PROPERTY_GEOMETRY + ":Geometry:srid=4326";
            featureType = DataUtilities.createType(FEATURE_NAME, featureSchema);
            dataStore.createSchema(featureType);
        }
        return featureType;
    }

    @Override
    public void storeStatements(Collection<Statement> statements) throws IOException {
        // create a feature collection
        DefaultFeatureCollection featureCollection = new DefaultFeatureCollection();

        for (Statement statement : statements) {
            // if the predicate list is empty, accept all predicates.
            // Otherwise, make sure the predicate is on the "valid" list
            boolean isValidPredicate = validPredicates.isEmpty() || validPredicates.contains(statement.getPredicate());

            if (isValidPredicate && (statement.getObject() instanceof Literal)) {
                try {
                    SimpleFeature feature = createFeature(featureType, statement);
                    featureCollection.add(feature);
                } catch (ParseException e) {
                    logger.warn("Error getting geo from statement: " + statement.toString(), e);
                }
            }
        }

        // write this feature collection to the store
        if (!featureCollection.isEmpty()) {
            featureStore.addFeatures(featureCollection);
        }
    }

    @Override
    public void storeStatement(Statement statement) throws IOException {
        storeStatements(Collections.singleton(statement));
    }

    private static SimpleFeature createFeature(SimpleFeatureType featureType, Statement statement) throws ParseException {
        String subject = StatementSerializer.writeSubject(statement);
        String predicate = StatementSerializer.writePredicate(statement);
        String object = StatementSerializer.writeObject(statement);
        String context = StatementSerializer.writeContext(statement);

        // create the feature
        Object[] noValues = {};

        // create the hash
        String statementId = Md5Hash.md5Base64(StatementSerializer.writeStatement(statement));
        SimpleFeature newFeature = SimpleFeatureBuilder.build(featureType, noValues, statementId);

        // write the statement data to the fields
        newFeature.setDefaultGeometry((new WKTReader()).read(getWellKnownText(statement)));
        newFeature.setAttribute(SUBJECT_ATTRIBUTE, subject);
        newFeature.setAttribute(PREDICATE_ATTRIBUTE, predicate);
        newFeature.setAttribute(OBJECT_ATTRIBUTE, object);
        newFeature.setAttribute(CONTEXT_ATTRIBUTE, context);

        // preserve the ID that we created for this feature
        // (set the hint to FALSE to have GeoTools generate IDs)
        newFeature.getUserData().put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE);

        return newFeature;
    }

    private static String getWellKnownText(Statement statement) throws ParseException {
        org.openrdf.model.Value v = statement.getObject();
        if (!(v instanceof Literal)) {
            throw new ParseException("Statement does not contain Literal: " + statement.toString());
        }

        Literal lit = (Literal) v;
        if (!GeoConstants.XMLSCHEMA_OGC_WKT.equals(lit.getDatatype())) {
            logger.warn("Literal is not of type " + GeoConstants.XMLSCHEMA_OGC_WKT + ": " + statement.toString());
        }

        return lit.getLabel().toString();
    }

    private CloseableIteration<Statement, QueryEvaluationException> performQuery(String type, Geometry geometry) {
        List<String> filterParms = new ArrayList<String>();

        filterParms.add(type + "(" + Constants.SF_PROPERTY_GEOMETRY + ", " + geometry + " )");

        String filterString = StringUtils.join(filterParms, " AND ");
        logger.info("Performing geomesa query : " + filterString);

        return getIteratorWrapper(filterString);
    }

    private CloseableIteration<Statement, QueryEvaluationException> getIteratorWrapper(final String filterString) {

        return new CloseableIteration<Statement, QueryEvaluationException>() {

            private FeatureIterator<SimpleFeature> featureIterator = null;

            FeatureIterator<SimpleFeature> getIterator() throws QueryEvaluationException {
                if (featureIterator == null) {
                    Filter cqlFilter;
                    try {
                        cqlFilter = ECQL.toFilter(filterString);
                    } catch (CQLException e) {
                        logger.error("Error parsing query: " + filterString, e);
                        throw new QueryEvaluationException(e);
                    }

                    Query query = new Query(featureType.getTypeName(), cqlFilter);
                    try {
                        featureIterator = featureSource.getFeatures(query).features();
                    } catch (IOException e) {
                        logger.error("Error performing query: " + filterString, e);
                        throw new QueryEvaluationException(e);
                    }

                }
                return featureIterator;
            }

            @Override
            public boolean hasNext() throws QueryEvaluationException {
                return getIterator().hasNext();
            }

            @Override
            public Statement next() throws QueryEvaluationException {
                SimpleFeature feature = (SimpleFeature) getIterator().next();
                String subjectString = feature.getAttribute(SUBJECT_ATTRIBUTE).toString();
                String predicateString = feature.getAttribute(PREDICATE_ATTRIBUTE).toString();
                String objectString = feature.getAttribute(OBJECT_ATTRIBUTE).toString();
                String contextString = feature.getAttribute(CONTEXT_ATTRIBUTE).toString();
                Statement statement = StatementSerializer.readStatement(subjectString, predicateString, objectString, contextString);
                return statement;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Remove not implemented");
            }

            @Override
            public void close() throws QueryEvaluationException {
                getIterator().close();
            }
        };
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryEquals(Geometry query) {
        return performQuery("EQUALS", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(Geometry query) {
        return performQuery("DISJOINT", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryIntersects(Geometry query) {
        return performQuery("INTERSECTS", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryTouches(Geometry query) {
        return performQuery("TOUCHES", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryCrosses(Geometry query) {
        return performQuery("CROSSES", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryWithin(Geometry query) {
        return performQuery("WITHIN", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryContains(Geometry query) {
        return performQuery("CONTAINS", query);
    }

    @Override
    public CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(Geometry query) {
        return performQuery("OVERLAPS", query);
    }

    @Override
    public Set<URI> getIndexablePredicates() {
        return validPredicates;
    }

    @Override
    public void flush() throws IOException {
        // TODO cache and flush features instead of writing them one at a time
    }

    @Override
    public void close() throws IOException {
        flush();
    }

}
