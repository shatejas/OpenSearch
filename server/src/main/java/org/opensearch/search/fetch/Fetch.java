package org.opensearch.search.fetch;

import org.opensearch.search.internal.SearchContext;

public interface Fetch {

    void execute(SearchContext context, String profileDescription);
}
