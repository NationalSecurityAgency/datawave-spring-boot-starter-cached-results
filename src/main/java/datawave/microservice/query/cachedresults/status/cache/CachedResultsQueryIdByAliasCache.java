package datawave.microservice.query.cachedresults.status.cache;

import datawave.microservice.cached.LockableCacheInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import static datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryIdByAliasCache.CACHE_NAME;

@CacheConfig(cacheNames = CACHE_NAME)
public class CachedResultsQueryIdByAliasCache extends LockableCache<String> {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "cachedResultsQueryIdByAliasCache";
    
    public CachedResultsQueryIdByAliasCache(LockableCacheInspector cacheInspector) {
        super(cacheInspector, CACHE_NAME);
    }
    
    @Override
    public String get(String alias) {
        return cacheInspector.list(CACHE_NAME, String.class, alias);
    }
    
    @Override
    @CachePut(key = "#alias")
    public String update(String alias, String queryId) {
        return queryId;
    }
    
    @CacheEvict(key = "#alias")
    public void remove(String alias) {
        if (log.isDebugEnabled()) {
            log.debug("Evicting alias {}", alias);
        }
    }
}
