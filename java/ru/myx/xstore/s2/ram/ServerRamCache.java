/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import java.util.Map;
import java.util.Set;

import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CacheL3;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.help.Convert;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.Locker;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Server;
import ru.myx.xstore.s2.Transaction;

/**
 * @author myx
 * 
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public final class ServerRamCache implements Server {
	private static final BaseLink						NULL_OBJECT_LNK		= new NulObjectLink();
	
	private static final String[]						NULL_OBJECT_STRS	= new String[] { "", "" };
	
	private static final Map.Entry<String, Object>[]	NULL_OBJECT_OBJS	= Convert.Array.toAny( new Map.Entry[0] );
	
	private final Server								parent;
	
	private final CacheL2<Object>						cacheTreeLink;
	
	private final CacheL2<Object>						cacheTreeSearch;
	
	private final CacheL2<Object>						cacheTreeSearchLocal;
	
	private final CacheL2<Object>						cacheTreeIDA;
	
	private final CacheL2<Object>						cacheTreeIDE;
	
	private final CacheL2<External>						cacheExtra;
	
	private final CacheL2<int[]>						cacheDictionary;
	
	private final CreatorLink							createLink;
	
	private final CreatorIdentitySearch					createIdentity;
	
	private final CreatorSearch							createSearch;
	
	private final CreatorSearchLocal					createSearchLocal;
	
	private final DictionaryCached						indexingDictionary;
	
	private final ExternalizerCached					externalizer;
	
	/**
	 * @param cache
	 * @param cachePrefix
	 * @param parent
	 */
	public ServerRamCache(final CacheL3<Object> cache, final String cachePrefix, final Server parent) {
		this.parent = parent;
		/**
		 * FIXME: too much caches, change to guid, cid and CacheL2
		 * Server.getCacheRendered()???
		 */
		this.cacheTreeLink = cache.getCacheL2( cachePrefix + "_trlink" );
		this.cacheTreeIDA = cache.getCacheL2( cachePrefix + "_trida" );
		this.cacheTreeIDE = cache.getCacheL2( cachePrefix + "_tride" );
		this.cacheTreeSearch = cache.getCacheL2( cachePrefix + "_trsrh" );
		this.cacheTreeSearchLocal = cache.getCacheL2( cachePrefix + "_trsrl" );
		this.cacheExtra = cache.getCacheL2( cachePrefix + "_extra" );
		this.cacheDictionary = cache.getCacheL2( cachePrefix + "_dict" );
		this.externalizer = new ExternalizerCached( parent.getExternalizer(), this.cacheExtra );
		this.indexingDictionary = new DictionaryCached( parent.getIndexingDictionary(), this.cacheDictionary );
		this.createLink = new CreatorLink( this, parent, ServerRamCache.NULL_OBJECT_LNK );
		this.createIdentity = new CreatorIdentitySearch( parent, ServerRamCache.NULL_OBJECT_STRS );
		this.createSearch = new CreatorSearch( ServerRamCache.NULL_OBJECT_OBJS );
		this.createSearchLocal = new CreatorSearchLocal( ServerRamCache.NULL_OBJECT_STRS );
	}
	
	@Override
	public final boolean addInterest(final Interest interest) {
		return this.parent.addInterest( interest );
	}
	
	@Override
	public final boolean areSynchronizationsSupported() {
		return this.parent.areSynchronizationsSupported();
	}
	
	final void clear() {
		this.cacheTreeLink.clear();
		this.cacheTreeIDA.clear();
		this.cacheTreeIDE.clear();
		this.cacheTreeSearch.clear();
		this.cacheExtra.clear();
		this.cacheDictionary.clear();
	}
	
	final void clear(final Set<String> clearLinks) {
		if (clearLinks != null) {
			for (final String guid : clearLinks) {
				this.cacheTreeLink.remove( guid );
				this.cacheTreeIDA.remove( guid );
				this.cacheTreeIDE.remove( guid );
				this.cacheTreeSearchLocal.remove( guid );
			}
		}
	}
	
	final void clear(final String lnkId) {
		if (lnkId != null) {
			this.cacheTreeLink.remove( lnkId );
			this.cacheTreeIDA.remove( lnkId );
			this.cacheTreeIDE.remove( lnkId );
			this.cacheTreeSearch.remove( lnkId );
			this.cacheTreeSearchLocal.remove( lnkId );
		}
	}
	
	@Override
	public final void clearAllRecycled() {
		this.parent.clearAllRecycled();
	}
	
	@Override
	public final Locker createLock(final String guid, final int version) {
		return this.parent.createLock( guid, version );
	}
	
	@Override
	public final Transaction createTransaction() {
		return new TransactionRamCache( this, this.parent.createTransaction() );
	}
	
	@Override
	public final ExternalHandler getExternalizer() {
		return this.externalizer;
	}
	
	@Override
	public final IndexingDictionary getIndexingDictionary() {
		return this.indexingDictionary;
	}
	
	@Override
	public final IndexingStemmer getIndexingStemmer() {
		return this.parent.getIndexingStemmer();
	}
	
	@Override
	public final Object getIssuer() {
		return this.parent.getIssuer();
	}
	
	@Override
	public final BaseLink getLink(final String guid) throws Exception {
		final Object result = this.cacheTreeLink.get( guid, "$LINK", null, guid, this.createLink );
		return result == ServerRamCache.NULL_OBJECT_LNK
				? null
				: (BaseLink) result;
	}
	
	@Override
	public final BaseLink getLinkForAlias(final String alias, final boolean all) {
		return this.parent.getLinkForAlias( alias, all );
	}
	
	@Override
	public final BaseRecycled[] getRecycled() {
		final BaseRecycled[] parental = this.parent.getRecycled();
		if (parental == null || parental.length == 0) {
			return null;
		}
		final BaseRecycled[] result = new BaseRecycled[parental.length];
		for (int i = parental.length - 1; i >= 0; --i) {
			result[i] = new RecycledCached( this, parental[i] );
		}
		return result;
	}
	
	@Override
	public final BaseRecycled getRecycledByGuid(final String guid) {
		final BaseRecycled parental = this.parent.getRecycledByGuid( guid );
		if (parental == null) {
			return null;
		}
		return new RecycledCached( this, parental );
	}
	
	@Override
	public final ModuleSchedule getScheduling() {
		return this.parent.getScheduling();
	}
	
	@Override
	public final ModuleSynchronizer getSynchronizer() {
		return this.parent.getSynchronizer();
	}
	
	@Override
	public final void invalidateCache() {
		this.parent.invalidateCache();
		this.cacheTreeLink.clear();
		this.cacheTreeIDA.clear();
		this.cacheTreeIDE.clear();
		this.cacheTreeSearch.clear();
		this.cacheExtra.clear();
		this.cacheDictionary.clear();
	}
	
	@Override
	public final boolean removeInterest(final Interest interest) {
		return this.parent.removeInterest( interest );
	}
	
	final Map.Entry<String, Object>[] search(
			final BaseLink searcher,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		return this.createSearch.search( this.cacheTreeSearch,
				searcher,
				limit,
				all,
				timeout,
				sort,
				dateStart,
				dateEnd,
				filter );
	}
	
	@Override
	public final String[] searchLinksForIdentity(final String guid, final boolean all) throws Exception {
		final Object result;
		result = all
				? this.cacheTreeIDA.get( guid, "$IDEA", Boolean.TRUE, guid, this.createIdentity )
				: this.cacheTreeIDE.get( guid, "$IDEN", Boolean.FALSE, guid, this.createIdentity );
		return result == ServerRamCache.NULL_OBJECT_STRS
				? null
				: (String[]) result;
	}
	
	final String[] searchLocal(
			final BaseLink searcher,
			final int limit,
			final boolean all,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String query) {
		return this.createSearchLocal.search( this.cacheTreeSearchLocal,
				searcher,
				limit,
				all,
				sort,
				dateStart,
				dateEnd,
				query );
	}
	
	@Override
	public final void start() {
		this.parent.start();
	}
	
	@Override
	public final void start(final String identity) {
		this.parent.start( identity );
	}
	
	@Override
	public final void stop() {
		this.parent.stop();
	}
	
	@Override
	public final void stop(final String identity) {
		this.parent.stop( identity );
	}
}
