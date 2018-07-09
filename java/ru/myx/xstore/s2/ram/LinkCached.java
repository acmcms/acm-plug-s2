/*
 * Created on 18.10.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.ram;

import java.util.Map;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae3.base.BaseObject;
import ru.myx.xstore.s2.BaseLink;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class LinkCached implements BaseLink {
	private final BaseLink			parent;
	
	private final ServerRamCache	server;
	
	LinkCached(final ServerRamCache server, final BaseLink parent) {
		this.parent = parent;
		this.server = server;
	}
	
	@Override
	public final String[] getAliases() {
		return this.parent.getAliases();
	}
	
	@Override
	public final String getChildByName(final String name) {
		return this.parent.getChildByName( name );
	}
	
	@Override
	public final String[] getChildren(final int count, final String sort) {
		return this.parent.getChildren( count, sort );
	}
	
	@Override
	public final String[] getChildrenListable(final int count, final String sort) {
		return this.parent.getChildrenListable( count, sort );
	}
	
	@Override
	public final long getCreated() {
		return this.parent.getCreated();
	}
	
	@Override
	public final BaseObject getData() {
		return this.parent.getData();
	}
	
	@Override
	public final String[] getFiles(final int count, final String sort) {
		if (sort == null) {
			return this.parent.getFiles( count, sort );
		}
		return this.parent.getFiles( count, sort );
	}
	
	@Override
	public final String[] getFilesListable(final int count, final String sort) {
		return this.parent.getFilesListable( count, sort );
	}
	
	@Override
	public final String[] getFolders(final int count, final String sort) {
		return this.parent.getFolders( count, sort );
	}
	
	@Override
	public final String[] getFoldersListable() {
		return this.parent.getFoldersListable();
	}
	
	@Override
	public final String getGuid() {
		return this.parent.getGuid();
	}
	
	@Override
	public final BaseHistory[] getHistory() {
		return this.parent.getHistory();
	}
	
	@Override
	public final BaseLink getHistorySnapshot(final String historyId) {
		return this.parent.getHistorySnapshot( historyId );
	}
	
	@Override
	public final String getKey() {
		return this.parent.getKey();
	}
	
	@Override
	public final String getLinkedIdentity() {
		return this.parent.getLinkedIdentity();
	}
	
	@Override
	public final long getModified() {
		return this.parent.getModified();
	}
	
	@Override
	public final String getOwner() {
		return this.parent.getOwner();
	}
	
	@Override
	public final String getParentGuid() {
		return this.parent.getParentGuid();
	}
	
	final BaseLink getParentLink() {
		return this.parent;
	}
	
	@Override
	public final int getState() {
		return this.parent.getState();
	}
	
	@Override
	public final String getTitle() {
		return this.parent.getTitle();
	}
	
	@Override
	public final String getTypeName() {
		return this.parent.getTypeName();
	}
	
	@Override
	public final BaseLink getVersion(final String versionId) {
		return this.parent.getVersion( versionId );
	}
	
	@Override
	public final String getVersionId() {
		return this.parent.getVersionId();
	}
	
	@Override
	public final boolean getVersioning() {
		return this.parent.getVersioning();
	}
	
	@Override
	public final BaseVersion[] getVersions() {
		return this.parent.getVersions();
	}
	
	@Override
	public final void invalidateThis() {
		this.server.clear( this.getGuid() );
		this.parent.invalidateThis();
	}
	
	@Override
	public final void invalidateTree() {
		this.server.clear( this.getGuid() );
		this.server.clear( this.getParentGuid() );
		this.parent.invalidateTree();
	}
	
	@Override
	public final boolean isFolder() {
		return this.parent.isFolder();
	}
	
	@Override
	public final Map.Entry<String, Object>[] search(
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		if (timeout == 0L | limit == 0) {
			return this.parent.search( limit, all, timeout, sort, dateStart, dateEnd, filter );
		}
		return this.server.search( this.parent, limit, all, timeout, sort, dateStart, dateEnd, filter );
		// return
		// parent.search(limit,all,timeout,sort,dateStart,dateEnd,filter);
	}
	
	@Override
	public final Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(
			final boolean all,
			final long timeout,
			final long startDate,
			final long endDate,
			final String filter) {
		return this.parent.searchCalendar( all, timeout, startDate, endDate, filter );
	}
	
	@Override
	public final String[] searchLocal(
			final int limit,
			final boolean all,
			final String sort,
			final long startDate,
			final long endDate,
			final String query) {
		if (limit == 0) {
			return this.parent.searchLocal( limit, all, sort, startDate, endDate, query );
		}
		return this.server.searchLocal( this.parent, limit, all, sort, startDate, endDate, query );
	}
	
	@Override
	public final Map<String, Number> searchLocalAlphabet(
			final boolean all,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String query) {
		return this.parent.searchLocalAlphabet( all, alphabetConversion, defaultLetter, query );
	}
	
	@Override
	public final String[] searchLocalAlphabet(
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {
		return this.parent.searchLocalAlphabet( limit,
				all,
				sort,
				alphabetConversion,
				defaultLetter,
				filterLetter,
				query );
	}
	
	@Override
	public final Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(
			final boolean all,
			final long startDate,
			final long endDate,
			final String query) {
		return this.parent.searchLocalCalendar( all, startDate, endDate, query );
	}
	
	@Override
	public final String toString() {
		return "linkCached{ parent=" + this.parent + " }";
	}
}
