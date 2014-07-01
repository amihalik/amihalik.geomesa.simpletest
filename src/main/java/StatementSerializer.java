
import java.io.IOException;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ContextStatementImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * A set of Utilities to serialize {@link Statement}s to/from {@link String}s.
 */
public class StatementSerializer {
    private static String SEP = "\u0000";

    private static ValueFactory VALUE_FACTORY = new ValueFactoryImpl();

    /**
     * Read a {@link Statement} from a {@link String}
     * 
     * @param in
     *            the {@link String} to parse
     * @return a {@link Statement}
     */
    public static Statement readStatement(String in) throws IOException {
        String[] parts = in.split(SEP);

        if (parts.length != 4) {
            throw new IOException("Not a valid statement: " + in);
        }

        String contextString = parts[0];
        String subjectString = parts[1];
        String predicateString = parts[2];
        String objectString = parts[3];
        return readStatement(subjectString, predicateString, objectString, contextString);
    }

    public static Statement readStatement(String subjectString, String predicateString, String objectString) {
        return readStatement(subjectString, predicateString, objectString, "");
    }

    public static Statement readStatement(String subjectString, String predicateString, String objectString, String contextString) {
        Resource subject = createResource(subjectString);
        URI predicate = VALUE_FACTORY.createURI(predicateString);

        boolean isObjectLiteral = objectString.startsWith("\"");

        Value object = null;
        if (isObjectLiteral) {
            object = parseLiteral(objectString);
        } else {
            object = createResource(objectString);
        }

        if (contextString == null || contextString.isEmpty()) {
            return new StatementImpl(subject, predicate, object);
        } else {
            Resource context = VALUE_FACTORY.createURI(contextString);
            return new ContextStatementImpl(subject, predicate, object, context);
        }
    }

    private static Resource createResource(String str) {
        if (str.startsWith("_")) {
            return VALUE_FACTORY.createBNode(str.substring(2));
        }
        return VALUE_FACTORY.createURI(str);

    }

    private static Literal parseLiteral(String fullLiteralString) {
        Validate.notNull(fullLiteralString);
        Validate.isTrue(fullLiteralString.length() > 1);

        if (fullLiteralString.endsWith("\"")) {
            String fullLiteralWithoutQuotes = fullLiteralString.substring(1, fullLiteralString.length() - 1);
            return VALUE_FACTORY.createLiteral(fullLiteralWithoutQuotes, (String) null);
        } else {

            // find the closing quote
            int labelEnd = fullLiteralString.lastIndexOf("\"");

            String label = fullLiteralString.substring(1, labelEnd);

            String data = fullLiteralString.substring(labelEnd + 1);

            if (data.startsWith("@")) {
                // the data is "language"
                String lang = data.substring(1);
                return VALUE_FACTORY.createLiteral(label, lang);
            } else if (data.startsWith("^^<")) {
                // the data is a "datatype"
                String datatype = data.substring(3, data.length() - 1);
                URI datatypeUri = VALUE_FACTORY.createURI(datatype);
                return VALUE_FACTORY.createLiteral(label, datatypeUri);
            }
        }
        return null;

    }

    public static String writeSubject(Statement statement) {
        return statement.getSubject().toString();
    }

    public static String writeObject(Statement statement) {
        return statement.getObject().toString();
    }

    public static String writePredicate(Statement statement) {
        return statement.getPredicate().toString();
    }

    public static String writeContext(Statement statement) {
        if (statement.getContext() == null) {
            return "";
        }
        return statement.getContext().toString();
    }

    /**
     * Write a {@link Statement} to a {@link String}
     * 
     * @param statement
     *            the {@link Statement} to write
     * @return a {@link String} representation of the statement
     */
    public static String writeStatement(Statement statement) {
        Resource subject = statement.getSubject();
        Resource context = statement.getContext();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();

        Validate.notNull(subject);
        Validate.notNull(predicate);
        Validate.notNull(object);

        String s = "";
        if (context == null) {
            s = SEP + subject.toString() + SEP + predicate.toString() + SEP + object.toString();
        } else {
            s = context.toString() + SEP + subject.toString() + SEP + predicate.toString() + SEP + object.toString();
        }
        return s;
    }



}
