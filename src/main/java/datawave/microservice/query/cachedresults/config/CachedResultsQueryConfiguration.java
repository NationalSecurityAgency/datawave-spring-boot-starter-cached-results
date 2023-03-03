package datawave.microservice.query.cachedresults.config;

import com.hazelcast.spring.cache.HazelcastCacheManager;
import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import datawave.microservice.cached.CacheInspector;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.cached.LockableHazelcastCacheInspector;
import datawave.microservice.cached.UniversalLockableCacheInspector;
import datawave.core.query.cachedresults.CachedResultsQueryParameters;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryCache;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryIdByAliasCache;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryIdByViewCache;
import datawave.microservice.query.cachedresults.status.cache.CachedResultsQueryStatusCache;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcProperties;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.CacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.context.annotation.RequestScope;

import javax.sql.DataSource;
import java.util.function.Function;

@Configuration
@EnableConfigurationProperties(CachedResultsQueryProperties.class)
@ConditionalOnProperty(name = "datawave.query.cached-results.enabled", havingValue = "true", matchIfMissing = true)
public class CachedResultsQueryConfiguration {
    
    @Bean
    @ConfigurationProperties("spring.datasource.cached-results")
    public DataSourceProperties cachedResultsDataSourceProperties() {
        return new DataSourceProperties();
    }
    
    @Bean
    @ConfigurationProperties("spring.datasource.cached-results.hikari")
    public DataSource cachedResultsDataSource() {
        // @formatter:off
        return cachedResultsDataSourceProperties()
                .initializeDataSourceBuilder()
                .build();
        // @formatter:on
    }
    
    @Bean
    public JdbcTemplate cachedResultsJdbcTemplate() {
        return new JdbcTemplate(cachedResultsDataSource());
    }
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public CachedResultsQueryParameters cachedResultsQueryParameters() {
        CachedResultsQueryParameters cachedResultsQueryParameters = new CachedResultsQueryParameters();
        cachedResultsQueryParameters.clear();
        return cachedResultsQueryParameters;
    }
    
    @Bean
    @ConditionalOnMissingBean
    @RequestScope
    public SecurityMarking securityMarking() {
        SecurityMarking securityMarking = new ColumnVisibilitySecurityMarking();
        securityMarking.clear();
        return securityMarking;
    }
    
    @Bean
    public CachedResultsQueryIdByAliasCache cachedResultsQueryIdByAliasCache(Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new CachedResultsQueryIdByAliasCache(lockableCacheInspector);
    }
    
    @Bean
    public CachedResultsQueryIdByViewCache cachedResultsQueryIdByViewCache(Function<CacheManager,CacheInspector> cacheInspectorFactory,
                    CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new CachedResultsQueryIdByViewCache(lockableCacheInspector);
    }
    
    @Bean
    public CachedResultsQueryStatusCache cachedResultsStatusCache(Function<CacheManager,CacheInspector> cacheInspectorFactory, CacheManager cacheManager) {
        LockableCacheInspector lockableCacheInspector;
        if (cacheManager instanceof HazelcastCacheManager)
            lockableCacheInspector = new LockableHazelcastCacheInspector(cacheManager);
        else
            lockableCacheInspector = new UniversalLockableCacheInspector(cacheInspectorFactory.apply(cacheManager));
        return new CachedResultsQueryStatusCache(lockableCacheInspector);
    }
    
    @Bean
    public CachedResultsQueryCache cachedResultsQueryCache(CachedResultsQueryProperties cachedResultsQueryProperties,
                    CachedResultsQueryIdByAliasCache cachedResultsQueryIdByAliasCache, CachedResultsQueryIdByViewCache cachedResultsQueryIdByViewCache,
                    CachedResultsQueryStatusCache cachedResultsStatusCache) {
        return new CachedResultsQueryCache(cachedResultsQueryProperties, cachedResultsStatusCache, cachedResultsQueryIdByAliasCache,
                        cachedResultsQueryIdByViewCache);
    }
}
