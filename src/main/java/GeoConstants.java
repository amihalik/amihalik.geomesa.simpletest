

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

/**
 * A set of URIs used in GeoSPARQL
 */
public class GeoConstants {
	public static final String NS_GEO = "http://www.opengis.net/ont/geosparql#";
	public static final String NS_GEOF = "http://www.opengis.net/def/function/geosparql/";

	public static final URI XMLSCHEMA_OGC_WKT = new URIImpl(NS_GEO + "wktLiteral");
	public static final URI GEO_AS_WKT = new URIImpl(NS_GEO + "asWKT");

	public static final URI GEO_SF_EQUALS = new URIImpl(NS_GEOF + "sfEquals");
	public static final URI GEO_SF_DISJOINT = new URIImpl(NS_GEOF + "sfDisjoint");
	public static final URI GEO_SF_INTERSECTS = new URIImpl(NS_GEOF + "sfIntersects");
	public static final URI GEO_SF_TOUCHES = new URIImpl(NS_GEOF + "sfTouches");
	public static final URI GEO_SF_CROSSES = new URIImpl(NS_GEOF + "sfCrosses");
	public static final URI GEO_SF_WITHIN = new URIImpl(NS_GEOF + "sfWithin");
	public static final URI GEO_SF_CONTAINS = new URIImpl(NS_GEOF + "sfContains");
	public static final URI GEO_SF_OVERLAPS = new URIImpl(NS_GEOF + "sfOverlaps");
}
