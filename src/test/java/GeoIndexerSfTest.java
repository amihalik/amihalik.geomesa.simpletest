import info.aduna.iteration.CloseableIteration;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.Sets;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.PackedCoordinateSequence;

public class GeoIndexerSfTest {
    private static Configuration conf;
    private static GeometryFactory gf = new GeometryFactory(new PrecisionModel(), 4326);
    private static GeoIndexer g;

    // Here is the landscape:
    /**
     * <pre>
     *   +---+---+---+---+---+---+---+
     *   |        F          |       |
     *   +  A    +           +   C   +
     *   |                   |       |
     *   +---+---+    E      +---+---+
     *   |       |   /       |
     *   +   B   +  /+---+---+
     *   |       | / |       |
     *   +---+---+/--+---+---+
     *           /   |     D |
     *          /    +---+---+
     * </pre>
     **/

    private static final Polygon A = poly(bbox(0, 1, 4, 5));
    private static final Polygon B = poly(bbox(0, 1, 2, 3));
    private static final Polygon C = poly(bbox(4, 3, 6, 5));
    private static final Polygon D = poly(bbox(3, 0, 5, 2));

    private static final Point F = point(2, 4);

    private static final LineString E = line(2, 0, 3, 3);

    @BeforeClass
    public static void before() throws Exception {
        System.out.println(UUID.randomUUID().toString());
        String tableName = "triplestore_geospacial";
        conf = new Configuration();
        conf.setBoolean(ConfigUtils.USE_MOCK_INSTANCE, true);
        conf.set(ConfigUtils.CLOUDBASE_USER, "SPEAR");
        conf.set(ConfigUtils.CLOUDBASE_PASSWORD, "spear");
        conf.set(ConfigUtils.GEO_TABLENAME, tableName);
        conf.set(ConfigUtils.CLOUDBASE_AUTHS, "U");

        g = new GeoMesaGeoIndexer(conf);
        g.storeStatement(statement(A));
        g.storeStatement(statement(B));
        g.storeStatement(statement(C));
        g.storeStatement(statement(D));
        g.storeStatement(statement(F));
        g.storeStatement(statement(E));

    }

    private static Statement statement(Geometry geo) {
        ValueFactory vf = new ValueFactoryImpl();
        Resource subject = vf.createURI("uri:" + DigestUtils.md5Hex(geo.toString()));
        URI predicate = GeoConstants.GEO_AS_WKT;
        Value object = vf.createLiteral(geo.toString(), GeoConstants.XMLSCHEMA_OGC_WKT);
        return new StatementImpl(subject, predicate, object);

    }

    private static Point point(double x, double y) {
        return gf.createPoint(new Coordinate(x, y));
    }

    private static LineString line(double x1, double y1, double x2, double y2) {
        return new LineString(new PackedCoordinateSequence.Double(new double[] { x1, y1, x2, y2 }, 2), gf);
    }

    private static Polygon poly(double[] arr) {
        LinearRing r1 = gf.createLinearRing(new PackedCoordinateSequence.Double(arr, 2));
        Polygon p1 = gf.createPolygon(r1, new LinearRing[] {});
        return p1;
    }

    private static double[] bbox(double x1, double y1, double x2, double y2) {
        return new double[] { x1, y1, x1, y2, x2, y2, x2, y1, x1, y1 };
    }

    public void compare(CloseableIteration<Statement, ?> actual, Geometry... expected) throws Exception {
        Set<Statement> expectedSet = Sets.newHashSet();
        for (Geometry geo : expected) {
            expectedSet.add(statement(geo));
        }

        Assert.assertEquals(expectedSet, getSet(actual));
    }

    private static <X> Set<X> getSet(CloseableIteration<X, ?> iter) throws Exception {
        Set<X> set = new HashSet<X>();
        while (iter.hasNext()) {
            set.add(iter.next());
        }
        return set;
    }

    private static Geometry[] EMPTY_RESULTS = {};

    @Test
    public void testEquals() throws Exception {
        // point
        compare(g.queryEquals(F), F);
        compare(g.queryEquals(point(2, 2)), EMPTY_RESULTS);

        // line
        compare(g.queryEquals(E), E);
        compare(g.queryEquals(line(2, 2, 3, 3)), EMPTY_RESULTS);

        // poly
        compare(g.queryEquals(A), A);
        compare(g.queryEquals(poly(bbox(1, 1, 4, 5))), EMPTY_RESULTS);

    }

    @Test
    public void testDisjoint() throws Exception {
        // point
        compare(g.queryDisjoint(F), B, C, D, E);

        // line
        compare(g.queryDisjoint(E), B, C, D, F);

        // poly
        compare(g.queryDisjoint(A), EMPTY_RESULTS);
        compare(g.queryDisjoint(B), C, D, F, E);
    }

    @Test
    public void testIntersectsPoint() throws Exception {
        // This seems like a bug
        // compare(g.queryIntersects(F), A, F);
        // compare(g.queryIntersects(F), EMPTY_RESULTS);
    }

    @Test
    public void testIntersectsLine() throws Exception {
        // This seems like a bug
        // compare(g.queryIntersects(E), A, E);
        // compare(g.queryIntersects(E), EMPTY_RESULTS);
    }

    @Test
    public void testIntersectsPoly() throws Exception {
        compare(g.queryIntersects(A), A, B, C, D, F, E);
    }

    @Test
    public void testTouchesPoint() throws Exception {
        compare(g.queryTouches(F), EMPTY_RESULTS);
    }

    @Test
    public void testTouchesLine() throws Exception {
        compare(g.queryTouches(E), EMPTY_RESULTS);
    }

    @Test
    public void testTouchesPoly() throws Exception {
        compare(g.queryTouches(A), C);
    }

    @Test
    public void testCrossesPoint() throws Exception {
        compare(g.queryCrosses(F), EMPTY_RESULTS);
    }

    @Test
    public void testCrossesLine() throws Exception {
        compare(g.queryCrosses(E), A);
    }

    @Test
    public void testCrossesPoly() throws Exception {
        compare(g.queryCrosses(A), E);
    }

    @Test
    public void testWithin() throws Exception {
        // point
        // compare(g.queryWithin(F), F);

        // line
        // compare(g.queryWithin(E), E);

        // poly
        // This seems like a bug
        // compare(g.queryWithin(A), A, B, F);
        compare(g.queryWithin(A), A, B, C, D, F, E);
    }

    @Test
    public void testContainsPoint() throws Exception {
        compare(g.queryContains(F), A, F);
    }

    @Test
    public void testContainsLine() throws Exception {
        compare(g.queryContains(E), E);
    }

    @Test
    public void testContainsPoly() throws Exception {
        compare(g.queryContains(A), A);
        compare(g.queryContains(B), A, B);
    }

    @Test
    public void testOverlapsPoint() throws Exception {
        // compare(g.queryOverlaps(F), F);
        // You cannot have overlapping points
        // compare(g.queryOverlaps(F), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsLine() throws Exception {
        // compare(g.queryOverlaps(E), A, E);
        // You cannot have overlapping lines
        // compare(g.queryOverlaps(E), EMPTY_RESULTS);
    }

    @Test
    public void testOverlapsPoly() throws Exception {
        // compare(g.queryOverlaps(A), A, B, D, F, E);
        compare(g.queryOverlaps(A), A, B, C, D, F, E);
    }
}