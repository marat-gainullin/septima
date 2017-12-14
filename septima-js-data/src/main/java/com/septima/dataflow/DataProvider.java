package com.septima.dataflow;

import com.septima.metadata.Parameter;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * This interface is intended to serve as base contract for data
 * quering/fetching/reading and than applying changes to variety of unknown and
 * mystery sources/recipients.
 *
 * @author mg
 */
public interface DataProvider extends AutoCloseable {

    int NO_PAGING_PAGE_SIZE = -1;

    /**
     * Returns back-end entity identifier. It might be a database table, or ORM
     * entity.
     *
     * @return Back-end entity identifier. It might be a database table, or ORM
     * entity
     */
    String getEntityName();

    /**
     * Queries some source for data, according to the supplied parameters
     * values.
     *
     * @param aParams Parameters values, ordered with some unknown criteria. If
     * data can't be achieved, in some circumstances, this method must return
     * at least an empty Rowset instance. Values from this parameter collection
     * are applied one by one in the straight order.
     * @param onSuccess
     * @param onFailure
     * @return Data collection, retrieved from the source.
     * @throws java.lang.Exception
     * @see Parameter
     */
    Collection<Map<String, Object>> refresh(List<Parameter> aParams, Consumer<Collection<Map<String, Object>>> onSuccess, Consumer<Exception> onFailure) throws Exception;

    /**
     * Fetches a next page of data from an abstract data source.
     *
     * @param onSuccess
     * @param onFailure
     * @return Data collection instance, containing data, retrieved from the source while
     * fetching a page.
     * @throws Exception
     * @see DataProviderNotPagedException
     */
    Collection<Map<String, Object>> nextPage(Consumer<Collection<Map<String, Object>>> onSuccess, Consumer<Exception> onFailure) throws Exception;

    /**
     * Returns page size for paged flow providers.
     *
     * @return Page size for paged flow providers. Value less or equal to zero
     * means that there is no paging.
     */
    int getPageSize();

    boolean isProcedure();

}
