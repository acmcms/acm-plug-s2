/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2;

import ru.myx.ae1.storage.ModuleRecycler;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingStemmer;
import ru.myx.ae3.extra.ExternalHandler;
import ru.myx.jdbc.lock.LockManager;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface Server extends ModuleRecycler, LockManager {
	/**
	 * @return boolean
	 */
	boolean areSynchronizationsSupported();
	
	/**
	 * @return transaction
	 */
	Transaction createTransaction();
	
	/**
	 * @return handler
	 */
	ExternalHandler getExternalizer();
	
	/**
	 * @return dictionary
	 */
	IndexingDictionary getIndexingDictionary();
	
	/**
	 * @return stemmer
	 */
	IndexingStemmer getIndexingStemmer();
	
	/**
	 * @return issuer
	 */
	Object getIssuer();
	
	/**
	 * @param guid
	 *            - link guid
	 * @return link
	 * @throws Exception
	 */
	BaseLink getLink(final String guid) throws Exception;
	
	/**
	 * @param alias
	 * @param all
	 * @return link
	 */
	BaseLink getLinkForAlias(final String alias, final boolean all);
	
	/**
	 * @return scheduler
	 */
	ModuleSchedule getScheduling();
	
	/**
	 * @return synchronizer
	 */
	ModuleSynchronizer getSynchronizer();
	
	/**
     * 
     */
	void invalidateCache();
	
	/**
	 * @param guid
	 * @param all
	 * @return string array
	 * @throws Exception
	 */
	String[] searchLinksForIdentity(final String guid, final boolean all) throws Exception;
	
	/**
     * 
     */
	void start();
	
	/**
     * 
     */
	void stop();
}
