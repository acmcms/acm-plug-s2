/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s2;

import java.util.List;
import java.util.Map;

import ru.myx.ae1.storage.AbstractEntry;
import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae1.storage.ListByMapEntryKey;
import ru.myx.ae1.storage.ListByValue;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.control.ControlBasic;
import ru.myx.ae3.exec.Exec;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
class EntryImpl extends AbstractEntry<EntryImpl> {
	
	
	private final StorageImpl storage;

	private final CurrentStorage system;

	private final BaseLink link;

	private final String lnkId;

	private final String ctnLnkId;

	private final String lnkName;

	private final String typeNameOverride;

	private BaseObject data = null;

	private Type<?> type;

	EntryImpl(final StorageImpl storage, final CurrentStorage system, final BaseLink link) {
		
		this.storage = storage;
		this.system = system;
		this.link = link;
		this.lnkId = link.getGuid();
		this.ctnLnkId = link.getParentGuid();
		this.lnkName = link.getKey();
		this.typeNameOverride = null;
	}

	EntryImpl(final StorageImpl storage, final CurrentStorage system, final BaseLink link, final String typeNameOverride) {
		
		this.storage = storage;
		this.system = system;
		this.link = link;
		this.lnkId = link.getGuid();
		this.ctnLnkId = link.getParentGuid();
		this.lnkName = link.getKey();
		this.typeNameOverride = typeNameOverride;
	}

	@Override
	public BaseChange createChange() {
		
		
		return new ChangeForEntry(this.system, this);
	}

	@Override
	public BaseChange createChild() {
		
		
		return new ChangeForNew(this.storage, this.system, this.getGuid(), Engine.createGuid(), this);
	}

	protected List<ControlBasic<?>> createListing(final String[] listing) {
		
		
		return new ListByValue<>(listing, this.system);
	}

	protected List<ControlBasic<?>> createListingMapEntry(final Map.Entry<String, Object>[] listing) {
		
		
		return new ListByMapEntryKey<>(listing, this.system);
	}

	@Override
	public String[] getAliases() {
		
		
		return this.link.getAliases();
	}

	@Override
	public BaseEntry<?> getChildByName(final String name) {
		
		
		final String guid = this.link.getChildByName(name);
		return guid == null
			? null
			: this.system.getByGuid(guid);
	}

	@Override
	public List<ControlBasic<?>> getChildren() {
		
		
		final Type<?> type = this.getType();
		final String[] result = this.link.getChildren(0, type == null
			? null
			: type.getTypeBehaviorListingSort());
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public List<ControlBasic<?>> getChildren(final int count, final String sort) {
		
		
		final String[] result = this.link.getChildren(count, sort);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public List<ControlBasic<?>> getChildrenListable(final int count, final String sort) {
		
		
		final String[] result = this.link.getChildrenListable(count, sort);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public long getCreated() {
		
		
		return this.link.getCreated();
	}

	BaseObject getDataReal() {
		
		
		return this.link.getData();
	}

	@Override
	public List<ControlBasic<?>> getFiles(final int count, final String sort) {
		
		
		final String[] result = this.link.getFiles(count, sort);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public List<ControlBasic<?>> getFilesListable(final int count, final String sort) {
		
		
		final String[] result = this.link.getFilesListable(count, sort);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public List<ControlBasic<?>> getFolders() {
		
		
		final String[] result = this.link.getFolders(0, null);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public List<ControlBasic<?>> getFoldersListable() {
		
		
		final String[] result = this.link.getFoldersListable();
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public String getGuid() {
		
		
		return this.lnkId;
	}

	@Override
	public BaseHistory[] getHistory() {
		
		
		return this.link.getHistory();
	}

	@Override
	public BaseEntry<?> getHistorySnapshot(final String historyId) {
		
		
		final BaseLink linkHistory = this.link.getHistorySnapshot(historyId);
		return linkHistory == null
			? null
			: new EntryImplHistory(this.storage, this.system, linkHistory, historyId);
	}

	@Override
	public String getKey() {
		
		
		return this.lnkName;
	}

	@Override
	public String getLinkedIdentity() {
		
		
		return this.link.getLinkedIdentity();
	}

	@Override
	public long getModified() {
		
		
		return this.link.getModified();
	}

	BaseLink getOriginalLink() {
		
		
		return this.link;
	}

	@Override
	public String getOwner() {
		
		
		return this.link.getOwner();
	}

	@Override
	public String getParentGuid() {
		
		
		return this.ctnLnkId;
	}

	@Override
	public int getState() {
		
		
		return this.link.getState();
	}

	@Override
	public StorageImpl getStorageImpl() {
		
		
		return this.storage;
	}

	@Override
	public String getTitle() {
		
		
		return this.link.getTitle();
	}

	@Override
	public Type<?> getType() {
		
		
		return this.type == null
			? this.type = Context.getServer(Exec.currentProcess()).getTypes().getType(this.getTypeName())
			: this.type;
	}

	@Override
	public String getTypeName() {
		
		
		return this.typeNameOverride == null
			? this.link.getTypeName()
			: this.typeNameOverride;
	}

	@Override
	public BaseEntry<?> getVersion(final String versionId) {
		
		
		final BaseLink linkVersion = this.link.getVersion(versionId);
		return linkVersion == null
			? null
			: new EntryImplVersion(this.storage, this.system, this, linkVersion, versionId);
	}

	String getVersionId() {
		
		
		return this.link.getVersionId();
	}

	boolean getVersioning() {
		
		
		return this.link.getVersioning();
	}

	@Override
	public BaseVersion[] getVersions() {
		
		
		return this.link.getVersions();
	}

	@Override
	public BaseObject internGetData() {
		
		
		return this.data == null
			? this.data = new FieldsReadable(this)
			: this.data;
	}

	final void invalidateThis() {
		
		
		this.link.invalidateThis();
		this.getStorageImpl().getObjectCache().remove(this.lnkId);
	}

	final void invalidateTree() {
		
		
		this.link.invalidateTree();
		this.getStorageImpl().getObjectCache().remove(this.lnkId);
	}

	@Override
	public boolean isFolder() {
		
		
		return this.link.isFolder();
	}

	@Override
	public final String restoreFactoryParameter() {
		
		
		return this.getStorageImpl().getMnemonicName() + ',' + this.getLinkedIdentity();
	}

	@Override
	public List<ControlBasic<?>> search(final int limit, final boolean all, final long timeout, final String sort, final long dateStart, final long dateEnd, final String filter) {
		
		
		final Map.Entry<String, Object>[] result = this.link.search(limit, all, timeout, sort, dateStart, dateEnd, filter);
		return result == null || result.length == 0
			? null
			: this.createListingMapEntry(result);
	}

	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchCalendar(final boolean all, final long timeout, final long startDate, final long endDate, final String filter) {
		
		
		return this.link.searchCalendar(all, timeout, startDate, endDate, filter);
	}

	@Override
	public List<ControlBasic<?>> searchLocal(final int limit, final boolean all, final String sort, final long startDate, final long endDate, final String query) {
		
		
		final String[] result = this.link.searchLocal(limit, all, sort, startDate, endDate, query);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public Map<String, Number> searchLocalAlphabet(final boolean all, final Map<String, String> alphabetConversion, final String defaultLetter, final String filter) {
		
		
		return this.link.searchLocalAlphabet(
				all,

				alphabetConversion,
				defaultLetter,
				filter);
	}

	@Override
	public List<ControlBasic<?>> searchLocalAlphabet(final int limit,
			final boolean all,
			final String sort,
			final Map<String, String> alphabetConversion,
			final String defaultLetter,
			final String filterLetter,
			final String query) {
		
		
		final String[] result = this.link.searchLocalAlphabet(limit, all, sort, alphabetConversion, defaultLetter, filterLetter, query);
		return result == null || result.length == 0
			? null
			: this.createListing(result);
	}

	@Override
	public Map<Integer, Map<Integer, Map<String, Number>>> searchLocalCalendar(final boolean all, final long startDate, final long endDate, final String query) {
		
		
		return this.link.searchLocalCalendar(all, startDate, endDate, query);
	}

	@Override
	public String toString() {
		
		
		return "[object " + this.baseClass() + "(" + "id=" + this.getGuid() + ", title=" + this.getTitle() + ")]";
	}
}
