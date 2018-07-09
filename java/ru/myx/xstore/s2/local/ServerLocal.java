/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.local;

import java.io.File;

import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingDictionaryVfs;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.ae3.vfs.Entry;
import ru.myx.ae3.vfs.EntryContainer;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.Locker;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Server;
import ru.myx.xstore.s2.StorageLevel2;
import ru.myx.xstore.s2.Transaction;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public class ServerLocal implements Server {
	
	
	private final StorageLevel2 storage;

	private final Server parent;

	private final File folderExtra;

	private final IndexingDictionary indexingDictionary;

	private final ExternalizerLocal externalizer;

	private final RunnerCleaner cleaner;

	/**
	 * @param storage
	 * @param folder
	 * @param parent
	 */
	public ServerLocal(final StorageLevel2 storage, final File folder, final Server parent) {
		this.storage = storage;
		this.parent = parent;
		{
			final Entry rootEntry = storage.getServer().getVfsRootEntry();
			final EntryContainer storageRoot = rootEntry.relativeFolderEnsure("storage/cache/s2/" + storage.getIdentity());
			final EntryContainer dictExactRoot = storageRoot.relativeFolderEnsure("dict-exact");
			final EntryContainer dictInexactRoot = storageRoot.relativeFolderEnsure("dict-inxact");
			this.indexingDictionary = new IndexingDictionaryVfs(dictExactRoot, dictInexactRoot, parent.getIndexingDictionary());
		}
		this.folderExtra = new File(folder, "extra");
		this.folderExtra.mkdirs();
		this.externalizer = new ExternalizerLocal(this, parent.getExternalizer(), parent.getIssuer(), this.folderExtra);
		this.cleaner = new RunnerCleaner(this.externalizer);
	}

	@Override
	public boolean addInterest(final Interest interest) {
		
		
		return this.parent.addInterest(interest);
	}

	@Override
	public boolean areSynchronizationsSupported() {
		
		
		return this.parent.areSynchronizationsSupported();
	}

	@Override
	public void clearAllRecycled() {
		
		
		this.parent.clearAllRecycled();
	}

	@Override
	public Locker createLock(final String guid, final int version) {
		
		
		return this.parent.createLock(guid, version);
	}

	@Override
	public Transaction createTransaction() {
		
		
		return this.parent.createTransaction();
	}

	@Override
	public ExternalHandler getExternalizer() {
		
		
		return this.externalizer;
	}

	@Override
	public IndexingDictionary getIndexingDictionary() {
		
		
		return this.indexingDictionary;
	}

	@Override
	public IndexingStemmer getIndexingStemmer() {
		
		
		return this.parent.getIndexingStemmer();
	}

	@Override
	public Object getIssuer() {
		
		
		return this.parent.getIssuer();
	}

	@Override
	public final BaseLink getLink(final String guid) throws Exception {
		
		
		return this.parent.getLink(guid);
	}

	@Override
	public BaseLink getLinkForAlias(final String alias, final boolean all) {
		
		
		return this.parent.getLinkForAlias(alias, all);
	}

	@Override
	public BaseRecycled[] getRecycled() {
		
		
		return this.parent.getRecycled();
	}

	@Override
	public BaseRecycled getRecycledByGuid(final String guid) {
		
		
		return this.parent.getRecycledByGuid(guid);
	}

	@Override
	public ModuleSchedule getScheduling() {
		
		
		return this.parent.getScheduling();
	}

	ExternalHandler getStorageExternalizer() {
		
		
		return this.storage.getServerImpl().getExternalizer();
	}

	@Override
	public ModuleSynchronizer getSynchronizer() {
		
		
		return this.parent.getSynchronizer();
	}

	@Override
	public void invalidateCache() {
		
		
		this.parent.invalidateCache();
	}

	@Override
	public boolean removeInterest(final Interest interest) {
		
		
		return this.parent.removeInterest(interest);
	}

	@Override
	public String[] searchLinksForIdentity(final String guid, final boolean all) throws Exception {
		
		
		return this.parent.searchLinksForIdentity(guid, all);
	}

	@Override
	public void start() {
		
		
		this.cleaner.start();
		this.parent.start();
	}

	@Override
	public void start(final String identity) {
		
		
		this.parent.start(identity);
	}

	@Override
	public void stop() {
		
		
		this.cleaner.stop();
		this.parent.stop();
	}

	@Override
	public void stop(final String identity) {
		
		
		this.parent.stop(identity);
	}
}
