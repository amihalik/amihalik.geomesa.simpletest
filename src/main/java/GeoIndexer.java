

import info.aduna.iteration.CloseableIteration;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.QueryEvaluationException;

import com.vividsolutions.jts.geom.Geometry;

/**
 * A repository to store, index, and retrieve {@link Statement}s based on geospatial features.
 */
public interface GeoIndexer extends Closeable, Flushable {

	/**
	 * Index and Store the {@link Statement} object.
	 * 
	 * @param statement
	 *            the statement object
	 * @throws IOException
	 */
	public abstract void storeStatement(Statement statement) throws IOException;

	/**
     * Index and Store the collection of {@link Statement} objects.
     * 
     * @param statement
     *            the statement object
     * @throws IOException
     */
    public abstract void storeStatements(Collection<Statement> statements) throws IOException;

	/**
	 * Returns statements that contain a geometry that is equal to the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>
	 * "Two geometries are topologically equal if their interiors intersect and no part of the interior or boundary of one geometry intersects the exterior of the other"
	 * <li>"A is equal to B if A is within B and A contains B"
	 * </ul>
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryEquals(Geometry query);

	/**
	 * Returns statements that contain a geometry that is disjoint to the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>"A and B are disjoint if they have no point in common. They form a set of disconnected geometries."
	 * <li>"A and B are disjoint if A does not intersect B"
	 * </ul>
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryDisjoint(Geometry query);

	/**
	 * Returns statements that contain a geometry that Intersects the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>"a intersects b: geometries a and b have at least one point in common."
	 * <li>"not Disjoint"
	 * </ul>
	 * 
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryIntersects(Geometry query);

	/**
	 * Returns statements that contain a geometry that Touches the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>"a touches b, they have at least one boundary point in common, but no interior points."
	 * </ul>
	 * 
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryTouches(Geometry query);

	/**
	 * Returns statements that contain a geometry that crosses the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>
	 * "a crosses b, they have some but not all interior points in common (and the dimension of the intersection is less than that of at least one of them)."
	 * </ul>
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryCrosses(Geometry query);

	/**
	 * Returns statements that contain a geometry that is Within the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>"a is within b, a lies in the interior of b"
	 * <li>Same as: "Contains(b,a)"
	 * </ul>
	 * 
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryWithin(Geometry query);

	/**
	 * Returns statements that contain a geometry that Contains the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>b is within a. Geometry b lies in the interior of a. Another definition:
	 * "a 'contains' b iff no points of b lie in the exterior of a, and at least one point of the interior of b lies in the interior of a"
	 * <li>Same: Within(b,a)
	 * </ul>
	 * 
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryContains(Geometry query);

	/**
	 * Returns statements that contain a geometry that Overlaps the queried {@link Geometry} and meet the {@link StatementContraints}.
	 * 
	 * <p>
	 * From Wikipedia (http://en.wikipedia.org/wiki/DE-9IM):
	 * <ul>
	 * <li>a crosses b, they have some but not all interior points in common (and the dimension of the intersection is less than that of at
	 * least one of them).
	 * </ul>
	 * 
	 * 
	 * @param query
	 *            the queried geometry
	 * @param contraints
	 *            the {@link StatementContraints}
	 * @return
	 */
	public abstract CloseableIteration<Statement, QueryEvaluationException> queryOverlaps(Geometry query);

	/**
	 * @return the set of predicates indexed by the indexer.
	 */
	public abstract Set<URI> getIndexablePredicates();

	@Override
	public abstract void flush() throws IOException;

	@Override
	public abstract void close() throws IOException;
}
