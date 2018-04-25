package allegrograph;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResultHandlerException;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;

import com.franz.agraph.repository.AGCatalog;
import com.franz.agraph.repository.AGRepository;
import com.franz.agraph.repository.AGRepositoryConnection;
import com.franz.agraph.repository.AGServer;

/**
 * An example that shows how RDF4J may be used with Allegrograph to insert and query data.
 */
public class Example {

	public static void main(final String[] args) throws Exception {
	    try(final AGRepositoryConnection rdfConn = connect(args[0], args[1], args[2], "Example")) {
	        // Write a few Statements to the repository.
	        final ValueFactory vf = rdfConn.getValueFactory();

	        final IRI alice = vf.createIRI("urn:alice");
	        final IRI bob = vf.createIRI("urn:bob");
	        final IRI charlie = vf.createIRI("urn:charlie");
	        final IRI talksTo = vf.createIRI("urn:talksTo");

	        // Try adding everything twice to ensure dupes are not being stored
	        System.out.println("Triple count before inserts: " + rdfConn.size());
	        rdfConn.begin();
	        rdfConn.add(alice, talksTo, bob);
	        rdfConn.add(alice, talksTo, bob);
	        rdfConn.add(bob, talksTo, alice);
	        rdfConn.add(bob, talksTo, alice);
	        rdfConn.add(bob, talksTo, charlie);
	        rdfConn.add(bob, talksTo, charlie);
	        rdfConn.add(charlie, talksTo, bob);
	        rdfConn.add(charlie, talksTo, bob);
	        rdfConn.commit();
	        System.out.println("Triple count after inserts: " + rdfConn.size());

	        // Run a query against that data.
	        System.out.println("People who bob talks to:");
	        final TupleQuery query = rdfConn.prepareTupleQuery(QueryLanguage.SPARQL, "SELECT ?person WHERE { <urn:bob> <urn:talksTo> ?person }");
	        query.evaluate(new TupleQueryResultHandler() {
	            @Override
	            public void handleSolution(final BindingSet bindingSet) throws TupleQueryResultHandlerException {
	                System.out.println(bindingSet);
	            }

	            @Override
	            public void handleBoolean(final boolean value) throws QueryResultHandlerException { }
	            @Override
	            public void handleLinks(final List<String> linkUrls) throws QueryResultHandlerException { }
	            @Override
	            public void startQueryResult(final List<String> bindingNames) throws TupleQueryResultHandlerException { }
	            @Override
	            public void endQueryResult() throws TupleQueryResultHandlerException { }
	        });
	    }
	}

	private static AGRepositoryConnection connect(final String serverUrl, final String username, final String password, final String repository) throws Exception {
	    requireNonNull(serverUrl);
	    requireNonNull(username);
	    requireNonNull(password);
	    requireNonNull(repository);

	    final AGServer server = new AGServer(serverUrl, username, password);
	    try {
	        System.out.println("Server version: " + server.getVersion());
	        System.out.println("Server build date: " + server.getBuildDate());
	        System.out.println("Server revision: " + server.getRevision());
	        System.out.println("Server catalogs: " + server.listCatalogs());
	    } catch(final Exception e) {
	        throw new Exception("Got error when attempting to connect to server at " + serverUrl, e);
	    }

	    final AGCatalog catalog = server.getCatalog("/");
	    if(catalog == null) {
	        throw new Exception("Catalog / does not exist.");
	    }

	    System.out.println("Available repositoryies in catalog " + catalog.getCatalogName() + ": " + catalog.listRepositories());
	    final AGRepository repo = catalog.createRepository(repository);
	    repo.initialize();

	    // Do not insert duplicate statements with graph granularity.
	    repo.setDuplicateSuppressionPolicy("spog");

	    // Close everything when the program terminates.
	    Runtime.getRuntime().addShutdownHook(new Thread() {
	        @Override
            public void run() {
	            try {
	                repo.close();
	                System.out.println("Closed the RDF4J repository.");
	            } catch(final Exception e) {
	                System.err.println("Could not close the RDF4J repository.");
	                e.printStackTrace();
	            }

	            try {
	                server.close();
	                System.out.println("Closed the AGServer.");
	            } catch(final Exception e) {
	                System.err.println("Could not close the AGServer.");
	                e.printStackTrace();
	            }
	        }
	    });

	    return repo.getConnection();
	}
}