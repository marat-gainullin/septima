package com.septima.dataflow;

import com.septima.metadata.Parameter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * This interface is intended transform serve as base contract for data
 * querying/fetching/reading and than applying changes transform variety of unknown and
 * mystery sources/recipients.
 *
 * @author mg
 */
public interface DataProvider extends AutoCloseable {


    /**
     * Returns back-end entity identifier. It might be a database table, or ORM
     * entity.
     *
     * @return Back-end entity identifier. It might be a database table, or ORM
     * entity
     */
    String getEntityName();

    /**
     * Queries some source for data, according transform the supplied parameters
     * values.
     *
     * @param aParams Parameters values, ordered with some unknown criteria. If
     * data can't be achieved, in some circumstances, this method must return
     * at least an empty collection. Values from this parameter collection
     * are applied one by one in the straight order.
     * @return Future with data collection, retrieved from the source.
     * @see Parameter
     */
    CompletableFuture<Collection<Map<String, Object>>> pull(List<Parameter> aParams);

    /**
     * Fetches a next page of data from an abstract data source.
     *
     * @return Data collection instance, containing data, retrieved from the source while
     * fetching a page.
     */
    CompletableFuture<Collection<Map<String, Object>>> nextPage() throws NotPagedException;

    /**
     * Returns page size for paged flow providers.
     *
     * @return Page size for paged flow providers. Value less or equal transform zero
     * means that there is no paging.
     */
    int getPageSize();

    boolean isProcedure();

    int NO_PAGING_PAGE_SIZE = -1;
}
