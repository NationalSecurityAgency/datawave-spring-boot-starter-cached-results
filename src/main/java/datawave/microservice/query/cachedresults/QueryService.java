package datawave.microservice.query.cachedresults;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.webservice.query.exception.QueryException;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.GenericResponse;
import datawave.webservice.result.VoidResponse;

public interface QueryService {
    // duplicate
    GenericResponse<String> duplicate(String queryId, DatawaveUserDetails currentUser) throws QueryException;
    
    // next
    BaseQueryResponse next(String queryId, DatawaveUserDetails currentUser) throws QueryException;
    
    // close?
    VoidResponse close(String queryId, DatawaveUserDetails currentUser) throws QueryException;
    
    // cancel?
    VoidResponse cancel(String queryId, DatawaveUserDetails currentUser) throws QueryException;
    
    // remove?
    VoidResponse remove(String queryId, DatawaveUserDetails currentUser) throws QueryException;
}
