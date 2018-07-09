/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ru.myx.ae1.schedule.Scheduling;
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingFinder;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.Lock;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.lock.Locker;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Server;
import ru.myx.xstore.s2.StorageLevel2;
import ru.myx.xstore.s2.Transaction;
import ru.myx.xstore.s2.indexing.IndexingS2;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public final class ServerJdbc implements Server {
	private final StorageLevel2			storage;
	
	private final String				identity;
	
	private final Object				issuer	= new Object();
	
	private final String				tnAliases;
	
	private final String				tnIndexed;
	
	private final String				tnExtra;
	
	private final String				tnExtraLink;
	
	private final String				tnObjects;
	
	private final String				tnObjectHistory;
	
	private final String				tnObjectVersions;
	
	private final String				tnRecycled;
	
	private final String				tnRecycledTree;
	
	private final String				tnTree;
	
	private final String				tnChangeQueue;
	
	private final String				tnChangeLog;
	
	private final String				tnScheduleQueue;
	
	private final String				tnScheduleLog;
	
	private final String				tnLocks;
	
	private final String				tnIndices;
	
	private final String				tnTreeSync;
	
	private final String				tnDictionary;
	
	private final IndexingDictionary	indexingDictionary;
	
	private final ExternalizerJdbc		externalizer;
	
	private final boolean				scheduling;
	
	private final boolean				update3;
	
	private IndexingS2					currentIndexing;
	
	private IndexingFinder				currentFinder;
	
	private ModuleSchedule				currentScheduling;
	
	private ModuleSynchronizer			currentSynchronizer;
	
	private LockManager					lockManager;
	
	/**
	 * @param storage
	 * @param prefix
	 * @param identity
	 * @param scheduling
	 * @param update3
	 */
	public ServerJdbc(final StorageLevel2 storage,
			final String prefix,
			final String identity,
			final boolean scheduling,
			final boolean update3) {
		this.storage = storage;
		this.tnAliases = prefix + "Aliases";
		this.tnIndexed = prefix + "Indexed";
		this.tnExtra = prefix + "Extra";
		this.tnExtraLink = prefix + "ExtraLink";
		this.tnObjects = prefix + "Objects";
		this.tnObjectHistory = prefix + "ObjectHistory";
		this.tnObjectVersions = prefix + "ObjectVersions";
		this.tnRecycled = prefix + "Recycled";
		this.tnRecycledTree = prefix + "RecycledTree";
		this.tnTree = prefix + "Tree";
		this.tnChangeQueue = prefix + "ChangeQueue";
		this.tnChangeLog = prefix + "ChangeLog";
		this.tnScheduleQueue = prefix + "ScheduleQueue";
		this.tnScheduleLog = prefix + "ScheduleLog";
		this.tnLocks = prefix + "Locks";
		this.tnIndices = prefix + "Indices";
		this.tnTreeSync = prefix + "TreeSync";
		this.tnDictionary = prefix + "Dictionary";
		this.identity = identity;
		this.indexingDictionary = new DictionaryJdbc( this, storage );
		this.externalizer = new ExternalizerJdbc( this, this.issuer, storage );
		this.scheduling = scheduling;
		this.update3 = update3;
	}
	
	@Override
	public boolean addInterest(final Interest interest) {
		return this.lockManager.addInterest( interest );
	}
	
	@Override
	public boolean areSynchronizationsSupported() {
		return true;
	}
	
	boolean checkExistance(final String guid) {
		try {
			return this.storage.getServerImpl().getLink( guid ) != null;
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	@Override
	public void clearAllRecycled() {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			try {
				conn.setAutoCommit( false );
				MatRecycled.clearRecycled( this, conn );
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	@Override
	public Locker createLock(final String guid, final int version) {
		return this.lockManager.createLock( guid, version );
	}
	
	@Override
	public Transaction createTransaction() {
		final Connection conn = this.storage.nextConnection();
		if (conn == null) {
			throw new RuntimeException( "Database is not available!" );
		}
		try {
			conn.setAutoCommit( false );
			return new TransactionJdbc( this, conn, this.issuer );
		} catch (final SQLException e) {
			throw new RuntimeException( e );
		}
	}
	
	final String[] getAliases(final String lnkId) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			return MatAlias.enlist( this, conn, lnkId );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	final DataJdbc getData(final String guid) throws Exception {
		try (final Connection conn = this.storage.nextConnection()) {
			return MatData.materialize( this, conn, guid );
		}
	}
	
	@Override
	public ExternalHandler getExternalizer() {
		return this.externalizer;
	}
	
	final IndexingFinder getFinder() {
		return this.currentFinder;
	}
	
	final String getIdentity() {
		return this.identity;
	}
	
	@Override
	public IndexingDictionary getIndexingDictionary() {
		return this.indexingDictionary;
	}
	
	@Override
	public IndexingStemmer getIndexingStemmer() {
		return IndexingStemmer.SIMPLE_STEMMER;
	}
	
	@Override
	public Object getIssuer() {
		return this.issuer;
	}
	
	@Override
	public final BaseLink getLink(final String guid) throws Exception {
		try (final Connection conn = this.storage.nextConnection()) {
			return MatLink.materialize( this, conn, guid );
		}
	}
	
	@Override
	public BaseLink getLinkForAlias(final String alias, final boolean all) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			final String lnkId = MatAlias.search( this, conn, alias, all );
			if (lnkId == null) {
				return null;
			}
			return MatLink.materialize( this, conn, lnkId );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	BaseHistory[] getObjectHistory(final String objId) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			return MatHistory.materialize( this, conn, objId );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	BaseLink getObjectSnapshot(final String lnkId, final String objId, final String historyId) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			final DataJdbc data = MatHistory.materializeSnapshot( this, conn, objId, historyId );
			if (data == null) {
				return null;
			}
			final LinkJdbc link = MatLink.materializeHistory( this, conn, lnkId, historyId );
			if (link == null) {
				return null;
			}
			link.setData( data );
			return link;
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	BaseLink getObjectVersion(final String lnkId, final String objId, final String versionId) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			final DataJdbc data = MatVersion.materializeSnapshot( this, conn, objId, versionId );
			if (data == null) {
				return null;
			}
			final LinkJdbc link = MatLink.materializeVersion( this, conn, lnkId, versionId );
			if (link == null) {
				return null;
			}
			link.setData( data );
			return link;
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	BaseVersion[] getObjectVersions(final String objId) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			return MatVersion.materialize( this, conn, objId );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	@Override
	public BaseRecycled[] getRecycled() {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			return MatRecycled.materializeAll( this, conn );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	@Override
	public BaseRecycled getRecycledByGuid(final String guid) {
		try (final Connection conn = this.storage.nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not available!" );
			}
			return MatRecycled.materialize( this, conn, guid );
		} catch (final Throwable t) {
			throw new RuntimeException( t );
		}
	}
	
	@Override
	public ModuleSchedule getScheduling() {
		return this.currentScheduling;
	}
	
	final StorageImpl getStorage() {
		return this.storage;
	}
	
	ExternalHandler getStorageExternalizer() {
		return this.storage.getServerImpl().getExternalizer();
	}
	
	@Override
	public ModuleSynchronizer getSynchronizer() {
		return this.currentSynchronizer;
	}
	
	final String getTnAliases() {
		return this.tnAliases;
	}
	
	final String getTnChangeLog() {
		return this.tnChangeLog;
	}
	
	final String getTnChangeQueue() {
		return this.tnChangeQueue;
	}
	
	final String getTnDictionary() {
		return this.tnDictionary;
	}
	
	final String getTnExtra() {
		return this.tnExtra;
	}
	
	final String getTnExtraLink() {
		return this.tnExtraLink;
	}
	
	final String getTnIndexed() {
		return this.tnIndexed;
	}
	
	final String getTnObjectHistory() {
		return this.tnObjectHistory;
	}
	
	final String getTnObjects() {
		return this.tnObjects;
	}
	
	final String getTnObjectVersions() {
		return this.tnObjectVersions;
	}
	
	final String getTnRecycled() {
		return this.tnRecycled;
	}
	
	final String getTnRecycledTree() {
		return this.tnRecycledTree;
	}
	
	final String getTnTree() {
		return this.tnTree;
	}
	
	final String getTnTreeSync() {
		return this.tnTreeSync;
	}
	
	final TreeJdbc getTree(final String guid, final int luid) throws Exception {
		try (final Connection conn = this.storage.nextConnection()) {
			return MatTree.materialize( this, conn, guid, luid );
		}
	}
	
	final int getVariantId(final String variant) {
		if (variant == null || "$common".equals( variant )) {
			return 0;
		}
		return this.storage.getIndexingDictionary().storeWordCode( variant, true, null );
	}
	
	@Override
	public void invalidateCache() {
		// ignore
	}
	
	final boolean isUpdate3() {
		return this.update3;
	}
	
	@Override
	public boolean removeInterest(final Interest interest) {
		return this.lockManager.removeInterest( interest );
	}
	
	@Override
	public final String[] searchLinksForIdentity(final String guid, final boolean all) throws Exception {
		try (final Connection conn = this.storage.nextConnection()) {
			return MatLink.searchIdentity( this, conn, guid, all );
		}
	}
	
	@Override
	public void start() {
		this.currentSynchronizer = new SynchronizerJdbc( this );
		this.currentIndexing = new IndexingS2( this.storage.getIndexingStemmer(),
				this.storage.getIndexingDictionary(),
				this.tnIndices );
		final Map<String, String> finderReplacementFields = new HashMap<>();
		finderReplacementFields.put( "$created", "o.objCreated" );
		finderReplacementFields.put( "$modified", "o.objDate" );
		finderReplacementFields.put( "$title", "o.objTitle" );
		finderReplacementFields.put( "$key", "t.lnkName" );
		finderReplacementFields.put( "$type", "o.objType" );
		this.currentFinder = new IndexingFinder( this.currentIndexing.getStemmer(),
				this.currentIndexing.getDictionary(),
				this.tnIndices,
				finderReplacementFields,
				"t.lnkId",
				"o.objCreated",
				this.tnTree
						+ " t, "
						+ this.tnObjects
						+ " o WHERE ix.luid=t.lnkLuid AND t.objId=o.objId AND o.objState IN ("
						+ ModuleInterface.STATE_PUBLISHED
						+ ','
						+ ModuleInterface.STATE_ARCHIEVED
						+ ") AND (",
				this.tnTree + " t, " + this.tnObjects + " o WHERE ix.luid=t.lnkLuid AND t.objId=o.objId AND (" );
		this.currentScheduling = Scheduling.getScheduling( this.storage, this.tnScheduleQueue, this.tnScheduleLog );
		this.lockManager = Lock.createManager( this.storage.getConnectionSource(), this.tnLocks, this.identity );
		if (this.lockManager != null) {
			this.lockManager.addInterest( new Interest( "update", new RunnerChangeUpdate( this.storage,
					this,
					this.currentIndexing ) ) );
			if (this.scheduling && this.currentScheduling != null) {
				this.lockManager
						.addInterest( new Interest( "schedule", new RunnerScheduling( this.currentScheduling ) ) );
			}
			this.lockManager.start( this.identity );
		}
	}
	
	@Override
	public void start(final String identity) {
		this.lockManager.start( identity );
	}
	
	@Override
	public void stop() {
		if (this.lockManager != null) {
			this.lockManager.stop( this.identity );
			this.lockManager = null;
		}
	}
	
	@Override
	public void stop(final String identity) {
		this.lockManager.stop( identity );
	}
}
