package org.opensearch.search.fetch;

import org.opensearch.search.internal.SearchContext;

/**
 * Interface for fetch phase implementations.
 *
 * @opensearch.internal
 */
public interface Fetch {

    void execute(SearchContext context, String profileDescription);
}
