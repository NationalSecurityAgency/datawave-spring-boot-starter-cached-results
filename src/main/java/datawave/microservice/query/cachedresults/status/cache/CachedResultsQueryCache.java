package datawave.microservice.query.cachedresults.status.cache;

import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.cachedresults.config.CachedResultsQueryProperties;
import datawave.microservice.query.cachedresults.status.CachedResultsQueryStatus;
import datawave.microservice.query.cachedresults.status.cache.util.CacheUpdater;
import datawave.microservice.query.cachedresults.status.cache.util.LockedCacheUpdateUtil;
import datawave.webservice.query.exception.QueryException;

public class CachedResultsQueryCache {
    private final CachedResultsQueryProperties cachedResultsQueryProperties;
    private final CachedResultsQueryStatusCache queryStatusCache;
    private final CachedResultsQueryIdByAliasCache queryIdByAliasCache;
    private final CachedResultsQueryIdByViewCache queryIdByViewCache;
    
    private LockedCacheUpdateUtil<CachedResultsQueryStatus> queryStatusLockedCacheUpdateUtil;
    
    public CachedResultsQueryCache(CachedResultsQueryProperties cachedResultsQueryProperties, CachedResultsQueryStatusCache queryStatusCache,
                    CachedResultsQueryIdByAliasCache queryIdByAliasCache, CachedResultsQueryIdByViewCache queryIdByViewCache) {
        this.cachedResultsQueryProperties = cachedResultsQueryProperties;
        this.queryStatusCache = queryStatusCache;
        this.queryIdByAliasCache = queryIdByAliasCache;
        this.queryIdByViewCache = queryIdByViewCache;
        this.queryStatusLockedCacheUpdateUtil = new LockedCacheUpdateUtil<>(queryStatusCache);
    }
    
    public CachedResultsQueryStatus createQuery(String definedQueryId, String alias, DatawaveUserDetails currentUser) {
        return queryStatusCache.create(definedQueryId, alias, currentUser);
    }
    
    public CachedResultsQueryStatus lookupQueryStatus(String key) {
        CachedResultsQueryStatus cachedResultsQueryStatus = getQueryStatus(key);
        
        if (cachedResultsQueryStatus == null) {
            cachedResultsQueryStatus = getQueryStatusByAlias(key);
            
            if (cachedResultsQueryStatus == null) {
                cachedResultsQueryStatus = getQueryStatusByView(key);
            }
        }
        
        return cachedResultsQueryStatus;
    }
    
    public CachedResultsQueryStatus getQueryStatus(String definedQueryId) {
        return queryStatusCache.get(definedQueryId);
    }
    
    public CachedResultsQueryStatus update(String definedQueryId, CachedResultsQueryStatus cachedResultsQueryStatus)
                    throws QueryException, InterruptedException {
        return queryStatusCache.update(definedQueryId, cachedResultsQueryStatus);
    }
    
    public CachedResultsQueryStatus lockedUpdate(String definedQueryId, CacheUpdater<CachedResultsQueryStatus> updater)
                    throws QueryException, InterruptedException {
        return queryStatusLockedCacheUpdateUtil.lockedUpdate(definedQueryId, updater, cachedResultsQueryProperties.getLockWaitTimeMillis(),
                        cachedResultsQueryProperties.getLockLeaseTimeMillis());
    }
    
    public CachedResultsQueryStatus lockedUpdate(String definedQueryId, CacheUpdater<CachedResultsQueryStatus> updater, long waitTimeMillis,
                    long leaseTimeMillis) throws QueryException, InterruptedException {
        return queryStatusLockedCacheUpdateUtil.lockedUpdate(definedQueryId, updater, waitTimeMillis, leaseTimeMillis);
    }
    
    public void lockQueryStatus(String definedQueryId) {
        queryStatusCache.lock(definedQueryId);
    }
    
    public void lockQueryStatus(String definedQueryId, long leaseTimeMillis) {
        queryStatusCache.lock(definedQueryId, leaseTimeMillis);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId) {
        return queryStatusCache.tryLock(definedQueryId);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId, long waitTimeMillis) {
        return queryStatusCache.tryLock(definedQueryId, waitTimeMillis);
    }
    
    public boolean tryLockQueryStatus(String definedQueryId, long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
        return queryStatusCache.tryLock(definedQueryId, waitTimeMillis, leaseTimeMillis);
    }
    
    public void unlockQueryStatus(String definedQueryId) {
        queryStatusCache.unlock(definedQueryId);
    }
    
    public void forceUnlockQueryStatus(String definedQueryId) {
        queryStatusCache.forceUnlock(definedQueryId);
    }
    
    public CachedResultsQueryStatus removeQueryStatus(String definedQueryId) {
        CachedResultsQueryStatus cachedResultsQueryStatus = queryStatusCache.get(definedQueryId);
        queryStatusCache.remove(definedQueryId);
        removeQueryIdByViewLookup(cachedResultsQueryStatus.getView());
        removeQueryIdByAliasLookup(cachedResultsQueryStatus.getAlias());
        return cachedResultsQueryStatus;
    }
    
    public String lookupQueryId(String key) {
        String definedQueryId = lookupQueryIdByAlias(key);
        if (definedQueryId == null) {
            definedQueryId = lookupQueryIdByView(key);
        }
        return definedQueryId;
    }
    
    private CachedResultsQueryStatus getQueryStatusByAlias(String alias) {
        String definedQueryId = lookupQueryIdByAlias(alias);
        return (definedQueryId != null) ? queryStatusCache.get(definedQueryId) : null;
    }
    
    private CachedResultsQueryStatus getQueryStatusByView(String view) {
        String definedQueryId = lookupQueryIdByView(view);
        return (definedQueryId != null) ? queryStatusCache.get(definedQueryId) : null;
    }
    
    public String putQueryIdByAliasLookup(String alias, String queryId) {
        if (alias != null) {
            return queryIdByAliasCache.update(alias, queryId);
        }
        return queryId;
    }
    
    public String lookupQueryIdByAlias(String alias) {
        return queryIdByAliasCache.get(alias);
    }
    
    public void removeQueryIdByAliasLookup(String alias) {
        queryIdByAliasCache.remove(alias);
    }
    
    public String putQueryIdByViewLookup(String view, String queryId) {
        if (view != null) {
            return queryIdByViewCache.update(view, queryId);
        }
        return queryId;
    }
    
    public String lookupQueryIdByView(String view) {
        return queryIdByViewCache.get(view);
    }
    
    public void removeQueryIdByViewLookup(String view) {
        queryIdByViewCache.remove(view);
    }
}
