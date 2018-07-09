/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.util.Map;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.common.Value;
import ru.myx.ae3.help.Convert;
import ru.myx.xstore.s2.AbstractLink;
import ru.myx.xstore.s2.BaseLink;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class LinkJdbc extends AbstractLink {
	private final ServerJdbc	server;
	
	private final String		guid;
	
	private final int			lnkLuid;
	
	private final String		lnkCntId;
	
	private final String		lnkName;
	
	private final boolean		lnkFolder;
	
	private final String		objId;
	
	private final String		versionId;
	
	private final String		objTitle;
	
	private final long			objCreated;
	
	private final long			objDate;
	
	private final String		objOwner;
	
	private final String		objType;
	
	private final int			objState;
	
	private final String		extLink;
	
	private DataJdbc			data	= null;
	
	private TreeJdbc			tree	= null;
	
	LinkJdbc(final ServerJdbc server,
			final String guid,
			final int lnkLuid,
			final String lnkCntId,
			final String lnkName,
			final boolean lnkFolder,
			final String objId,
			final String versionId,
			final String objTitle,
			final long objCreated,
			final long objDate,
			final String objOwner,
			final String objType,
			final int objState,
			final String extLink) {
		this.server = server;
		this.guid = guid;
		this.lnkLuid = lnkLuid;
		this.lnkCntId = lnkCntId;
		this.lnkName = lnkName;
		this.lnkFolder = lnkFolder;
		this.objId = objId;
		this.versionId = versionId;
		this.objTitle = objTitle;
		this.objCreated = objCreated;
		this.objDate = objDate;
		this.objOwner = objOwner;
		this.objType = objType;
		this.objState = objState;
		this.extLink = extLink;
	}
	
	@Override
	public String[] getAliases() {
		return this.server.getAliases( this.guid );
	}
	
	@Override
	public String getChildByName(final String name) {
		return this.getInnerTree().getChildByName( name );
	}
	
	@Override
	public String[] getChildren(final int count, final String sort) {
		return this.getInnerTree().getChildren( null, count, sort );
	}
	
	@Override
	public String[] getChildrenListable(final int count, final String sort) {
		return this.getInnerTree().getChildrenListable( null, count, sort );
	}
	
	@Override
	public final long getCreated() {
		return this.objCreated;
	}
	
	@Override
	public final BaseObject getData() {
		return this.getInnerData().getData();
	}
	
	@Override
	public String[] getFiles(final int count, final String sort) {
		return this.getInnerTree().getFiles( null, count, sort );
	}
	
	@Override
	public String[] getFilesListable(final int count, final String sort) {
		return this.getInnerTree().getFilesListable( null, count, sort );
	}
	
	@Override
	public String[] getFolders(final int count, final String sort) {
		return this.getInnerTree().getFolders( null, count, sort );
	}
	
	@Override
	public String[] getFoldersListable() {
		return this.getInnerTree().getFoldersListable();
	}
	
	@Override
	public String getGuid() {
		return this.guid;
	}
	
	@Override
	public BaseHistory[] getHistory() {
		return this.server.getObjectHistory( this.objId );
	}
	
	@Override
	public BaseLink getHistorySnapshot(final String historyId) {
		return this.server.getObjectSnapshot( this.guid, this.objId, historyId );
	}
	
	private final DataJdbc getInnerData() {
		DataJdbc data = this.data;
		if (data == null) {
			synchronized (this) {
				data = this.data;
				if (data == null) {
					if (this.extLink == null || this.extLink.equals( "-" )) {
						try {
							data = this.server.getData( this.objId );
						} catch (final RuntimeException e) {
							throw e;
						} catch (final Exception e) {
							throw new RuntimeException( e );
						}
						if (data == null) {
							return this.data = new DataJdbc( this.server, this.objId, BaseObject.UNDEFINED );
						}
						return this.data = data;
					}
					if (this.extLink.equals( "*" )) {
						return this.data = new DataJdbc( this.server, this.objId, BaseObject.UNDEFINED );
					}
					final Object o;
					try {
						o = this.server.getStorageExternalizer().getExternal( null, this.extLink );
					} catch (final RuntimeException e) {
						throw e;
					} catch (final Exception e) {
						throw new RuntimeException( e );
					}
					if (o == null) {
						return this.data = new DataJdbc( this.server, this.objId, BaseObject.UNDEFINED );
					}
					final BaseObject map = Convert.Any.toAny( ((Value<?>) o).baseValue() );
					return this.data = new DataJdbc( this.server, this.objId, map );
				}
			}
		}
		return data;
	}
	
	private final TreeJdbc getInnerTree() {
		{
			final TreeJdbc tree = this.tree;
			if (tree != null) {
				return tree;
			}
		}
		synchronized (this) {
			try {
				final TreeJdbc tree = this.tree;
				return tree == null
						? this.tree = this.server.getTree( this.guid, this.lnkLuid )
						: tree;
			} catch (final RuntimeException e) {
				throw e;
			} catch (final Exception e) {
				throw new RuntimeException( e );
			}
		}
	}
	
	@Override
	public String getKey() {
		return this.lnkName;
	}
	
	int getKeyLocal() {
		return this.lnkLuid;
	}
	
	@Override
	public String getLinkedIdentity() {
		return this.objId;
	}
	
	@Override
	public final long getModified() {
		return this.objDate;
	}
	
	@Override
	public final String getOwner() {
		return this.objOwner;
	}
	
	@Override
	public String getParentGuid() {
		return this.lnkCntId;
	}
	
	@Override
	public final int getState() {
		return this.objState;
	}
	
	@Override
	public final String getTitle() {
		return this.objTitle;
	}
	
	@Override
	public final String getTypeName() {
		return this.objType;
	}
	
	@Override
	public BaseLink getVersion(final String versionId) {
		return this.server.getObjectVersion( this.guid, this.objId, versionId );
	}
	
	@Override
	public final String getVersionId() {
		return this.versionId;
	}
	
	@Override
	public final boolean getVersioning() {
		return !"*".equals( this.versionId );
	}
	
	@Override
	public BaseVersion[] getVersions() {
		return this.server.getObjectVersions( this.objId );
	}
	
	final boolean internChildrenPossible() {
		final TreeJdbc tree = this.tree;
		if (tree == null) {
			return true;
		}
		final String[] children = tree.getChildren( null, 0, null );
		return children != null && children.length > 0;
	}
	
	@Override
	public final void invalidateThis() {
		this.data = null;
	}
	
	@Override
	public final void invalidateTree() {
		this.tree = null;
	}
	
	@Override
	public boolean isFolder() {
		return this.lnkFolder;
	}
	
	@Override
	public Map.Entry<String, Object>[] search(
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		return this.getInnerTree().search( limit, all, timeout, sort, dateStart, dateEnd, filter );
	}
	
	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(
			final boolean all,
			final long timeout,
			final long startDate,
			final long endDate,
			final String query) {
		return this.getInnerTree().searchCalendar( all, timeout, startDate, endDate, query );
	}
	
	@Override
	public String[] searchLocal(
			final int limit,
			final boolean all,
			final String sort,
			final long startDate,
			final long endDate,
			final String query) {
		return this.getInnerTree().searchLocal( null, limit, all, sort, startDate, endDate, query );
	}
	
	@Override
	public Map<String, Number> searchLocalAlphabet(
			final boolean all,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String query) {
		return this.getInnerTree().searchLocalAlphabet( all, alphabetConversion, defaultLetter, query );
	}
	
	@Override
	public String[] searchLocalAlphabet(
			final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {
		return this.getInnerTree().searchLocal( null,
				limit,
				all,
				sort,
				alphabetConversion,
				defaultLetter,
				filterLetter,
				query );
	}
	
	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(
			final boolean all,
			final long startDate,
			final long endDate,
			final String query) {
		return this.getInnerTree().searchLocalCalendar( all, startDate, endDate, query );
	}
	
	void setData(final DataJdbc data) {
		this.data = data;
	}
	
	@Override
	public String toString() {
		return "linkJDBC{ id=" + this.guid + ", parent=" + this.lnkCntId + ", name=" + this.lnkName + " }";
	}
}
