/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2;

import java.util.Map;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae3.base.BaseObject;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
public interface BaseLink {
	/**
	 * @return string array
	 */
	public String[] getAliases();
	
	/**
	 * @param name
	 * @return string
	 */
	String getChildByName(final String name);
	
	/**
	 * @param count
	 * @param sort
	 * @return string array
	 */
	String[] getChildren(final int count, final String sort);
	
	/**
	 * @param count
	 * @param sort
	 * @return string array
	 */
	String[] getChildrenListable(final int count, final String sort);
	
	/**
	 * @return date
	 */
	long getCreated();
	
	/**
	 * @return map
	 */
	BaseObject getData();
	
	/**
	 * @param count
	 * @param sort
	 * @return string array
	 */
	String[] getFiles(final int count, final String sort);
	
	/**
	 * @param count
	 * @param sort
	 * @return string array
	 */
	String[] getFilesListable(final int count, final String sort);
	
	/**
	 * @param count
	 * @param sort
	 * @return string array
	 */
	String[] getFolders(final int count, final String sort);
	
	/**
	 * @return string array
	 */
	String[] getFoldersListable();
	
	/**
	 * @return string
	 */
	String getGuid();
	
	/**
	 * @return history array
	 */
	public BaseHistory[] getHistory();
	
	/**
	 * @param historyId
	 * @return link
	 */
	public BaseLink getHistorySnapshot(final String historyId);
	
	/**
	 * @return string
	 */
	String getKey();
	
	/**
	 * @return string
	 */
	String getLinkedIdentity();
	
	/**
	 * @return date
	 */
	long getModified();
	
	/**
	 * @return string
	 */
	String getOwner();
	
	/**
	 * @return string
	 */
	String getParentGuid();
	
	/**
	 * @return int
	 */
	int getState();
	
	/**
	 * @return string
	 */
	String getTitle();
	
	/**
	 * @return string
	 */
	String getTypeName();
	
	/**
	 * @param versionId
	 * @return link
	 */
	public BaseLink getVersion(final String versionId);
	
	/**
	 * @return string
	 */
	public String getVersionId();
	
	/**
	 * @return boolean
	 */
	public boolean getVersioning();
	
	/**
	 * @return version array
	 */
	public BaseVersion[] getVersions();
	
	/**
     * 
     */
	public void invalidateThis();
	
	/**
     * 
     */
	public void invalidateTree();
	
	/**
	 * @return boolean
	 */
	boolean isFolder();
	
	/**
	 * @param limit
	 * @param all
	 * @param timeout
	 * @param sort
	 * @param dateStart
	 * @param dateEnd
	 * @param filter
	 * @return map
	 */
	public Map.Entry<String, Object>[] search(
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter);
	
	/**
	 * @param all
	 * @param timeout
	 * @param startDate
	 * @param endDate
	 * @param filter
	 * @return map
	 */
	public Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(
			final boolean all,
			final long timeout,
			final long startDate,
			final long endDate,
			final String filter);
	
	/**
	 * @param limit
	 * @param all
	 * @param sort
	 * @param startDate
	 * @param endDate
	 * @param query
	 * @return string array
	 */
	String[] searchLocal(
			final int limit,
			final boolean all,
			final String sort,
			final long startDate,
			final long endDate,
			final String query);
	
	/**
	 * @param all
	 * @param alphabetConversion
	 * @param defaultLetter
	 * @param filter
	 * @return map
	 */
	public Map<String, Number> searchLocalAlphabet(
			final boolean all,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filter);
	
	/**
	 * @param limit
	 * @param all
	 * @param sort
	 * @param alphabetConversion
	 * @param defaultLetter
	 * @param filterLetter
	 * @param query
	 * @return string array
	 */
	public String[] searchLocalAlphabet(
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query);
	
	/**
	 * @param all
	 * @param startDate
	 * @param endDate
	 * @param query
	 * @return map
	 */
	public Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(
			final boolean all,
			final long startDate,
			final long endDate,
			final String query);
}
