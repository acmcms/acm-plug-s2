/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import ru.myx.ae1.storage.AbstractSchedule;
import ru.myx.ae1.storage.AbstractSync;
import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae1.storage.BaseSchedule;
import ru.myx.ae1.storage.BaseSync;
import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
class ChangeForHistory extends ChangeAbstract {
	
	private Set<String> aliasAdd = null;

	private Set<String> aliasRemove = null;

	BaseSchedule changeSchedule = null;

	BaseSync changeSync = null;

	private boolean local = false;

	private Set<String> linkedIn = null;

	private boolean modifyLogged = false;

	private final CurrentStorage storage;

	private final EntryImpl entry;

	private final String historyId;

	private final String initialKey;

	private final boolean initialVersioning;

	private final BaseObject original;

	private final BaseObject data;

	private final String initialParentGuid;

	private String parentGuid;

	private List<ChangeNested> children = null;

	ChangeForHistory(final CurrentStorage storage, final EntryImpl entry, final String historyId) {
		
		this.storage = storage;
		this.entry = entry;
		this.historyId = historyId;
		this.initialParentGuid = entry.getParentGuid();
		this.parentGuid = this.initialParentGuid;
		this.initialKey = entry.getKey();
		this.initialVersioning = entry.getVersioning();
		this.setVersioning(this.initialVersioning);
		this.original = new BaseNativeObject();
		this.original.baseDefineImportAllEnumerable(entry.getData());
		this.data = new BaseNativeObject();
		this.data.baseDefineImportAllEnumerable(this.original);
		this.data.baseDefine("$key", entry.getKey());
		this.data.baseDefine("$title", entry.getTitle());
		this.data.baseDefine("$state", entry.getState());
		this.data.baseDefine("$folder", entry.isFolder());
		this.data.baseDefine("$type", entry.getTypeName());
		this.data.baseDefine("$created", Base.forDateMillis(entry.getCreated()));
	}

	@Override
	public void aliasAdd(final String alias) {
		
		if (this.aliasAdd == null) {
			this.aliasAdd = Create.tempSet();
		}
		this.aliasAdd.add(alias);
		if (this.aliasRemove != null) {
			this.aliasRemove.remove(alias);
		}
	}

	@Override
	public void aliasRemove(final String alias) {
		
		if (this.aliasRemove == null) {
			this.aliasRemove = Create.tempSet();
		}
		this.aliasRemove.add(alias);
		if (this.aliasAdd != null) {
			this.aliasAdd.remove(alias);
		}
	}

	@Override
	public void commit() {
		
		final Transaction transaction = this.storage.createTransaction();
		try {
			String typeName = Base.getString(this.data, "$type", null);
			if (typeName == null) {
				typeName = Context.getServer(Exec.currentProcess()).getTypes().getTypeNameDefault();
			}
			final Type<?> type = Context.getServer(Exec.currentProcess()).getTypes().getType(typeName);
			if (type != null) {
				type.onBeforeModify(this.entry, this, this.data);
			}
			if (this.changeSchedule != null) {
				final BaseSchedule schedule = this.entry.getSchedule();
				this.changeSchedule.scheduleFill(schedule, true);
				schedule.commit();
			}
			if (this.changeSync != null) {
				final BaseSync sync = this.entry.getSynchronization();
				this.changeSync.synchronizeFill(sync);
				sync.commit();
			}
			String key = this.entry.getKey();
			String title = this.entry.getTitle();
			long created = this.entry.getCreated();
			int state = this.entry.getState();
			boolean folder = this.entry.isFolder();
			for (final Iterator<String> i = Base.keys(this.data); i.hasNext();) {
				final String currentKey = i.next();
				if (currentKey.length() > 0 && '$' == currentKey.charAt(0)) {
					if ("$key".equals(currentKey)) {
						key = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
					} else if ("$title".equals(currentKey)) {
						title = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
					} else if ("$created".equals(currentKey)) {
						created = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToNumber().longValue();
					} else if ("$state".equals(currentKey)) {
						state = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaInteger();
					} else if ("$type".equals(currentKey)) {
						typeName = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaString();
					} else if ("$folder".equals(currentKey)) {
						folder = this.data.baseGet(currentKey, BaseObject.UNDEFINED).baseToJavaBoolean();
					}
					i.remove();
				}
			}
			if (key == null) {
				key = this.createDefaultKey();
				this.data.baseDefine("$key", key);
			}
			if (this.initialParentGuid != this.parentGuid && !this.initialParentGuid.equals(this.parentGuid)) {
				transaction.move(this.entry.getOriginalLink(), this.parentGuid, key);
			} else if (key != this.initialKey && !this.initialKey.equals(key)) {
				transaction.rename(this.entry.getOriginalLink(), key);
			}
			final BaseObject data = this.data;
			final BaseObject removed = ChangeAbstract.changeGetRemoved(this.original, data);
			final BaseObject added = ChangeAbstract.changeGetAdded(this.original, data);
			if (this.modifyLogged) {
				transaction.record(this.entry.getLinkedIdentity());
			}
			if (!removed.baseHasKeysOwn() && !added.baseHasKeysOwn()) {
				transaction.revert(this.entry.getOriginalLink(), this.historyId, folder, created, state, title, typeName);
			} else {
				transaction.revert(this.entry.getOriginalLink(), this.historyId, folder, created, state, title, typeName, removed, added);
			}
			if (this.aliasAdd != null || this.aliasRemove != null) {
				transaction.aliases(this.entry.getGuid(), this.aliasAdd, this.aliasRemove);
				this.aliasAdd = null;
				this.aliasRemove = null;
			}
			if (this.linkedIn != null) {
				final String objId = this.getLinkedIdentity();
				for (final String current : this.linkedIn) {
					final int pos = current.indexOf('\n');
					if (pos == -1) {
						transaction.link(this.local, current, Engine.createGuid(), key, folder, objId);
					} else {
						transaction.link(this.local, current.substring(0, pos), Engine.createGuid(), current.substring(pos + 1), folder, objId);
					}
				}
				this.linkedIn = null;
			}
			if (this.children != null && !this.children.isEmpty()) {
				for (final ChangeNested child : this.children) {
					child.realCommit(transaction);
				}
			}
			transaction.commit();
			this.changeSchedule = null;
			this.changeSync = null;
			if (this.children != null) {
				this.children.clear();
			}
			this.modifyLogged = false;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException("Transaction cancelled", t);
		}
	}

	@Override
	public BaseChange createChange(final BaseEntry<?> entry) {
		
		if (entry == null) {
			return null;
		}
		if (entry.getClass() != EntryImpl.class || entry.getStorageImpl() != this.entry.getStorageImpl()) {
			return entry.createChange();
		}
		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		return new ChangeForEntryNested(this.children, this.storage, (EntryImpl) entry);
	}

	@Override
	public BaseChange createChild() {
		
		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		return new ChangeForNewNested(this, this.children, this.entry.getStorageImpl(), this.storage, this.entry.getGuid(), Engine.createGuid());
	}
	
	@Override
	public final void delete() {
		
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.createTransaction();
		try {
			transaction.delete(this.entry.getOriginalLink(), false);
			transaction.commit();
		} catch (final Error | RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}

	@Override
	public final void delete(final boolean soft) {
		
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.createTransaction();
		try {
			transaction.delete(this.entry.getOriginalLink(), soft);
			transaction.commit();
		} catch (final Error | RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}

	@Override
	public BaseObject getData() {
		
		return this.data;
	}

	@Override
	public String getGuid() {
		
		return this.entry.getGuid();
	}

	@Override
	public BaseHistory[] getHistory() {
		
		return this.entry.getHistory();
	}

	@Override
	public BaseChange getHistorySnapshot(final String historyId) {
		
		final BaseEntry<?> entry = this.entry.getHistorySnapshot(historyId);
		if (entry == null) {
			return null;
		}
		final BaseChange change = entry.createChange();
		if (change == null) {
			return null;
		}
		if (this.modifyLogged) {
			change.setCommitLogged();
		}
		return change;
	}

	@Override
	public String getLinkedIdentity() {
		
		return this.entry.getLinkedIdentity();
	}

	@Override
	public final String getLocationControl() {
		
		final StorageImpl parent = this.getStorageImpl();
		if (this.getGuid().equals(parent.getStorage().getRootIdentifier())) {
			return parent.getLocationControl();
		}
		final BaseEntry<?> entry = parent.getStorage().getByGuid(this.getParentGuid());
		if (entry == null || entry == this) {
			return null;
		}
		final String parentPath = entry.getLocationControl();
		if (parentPath == null) {
			return null;
		}
		return parentPath.endsWith("/")
			? this.isFolder()
				? parentPath + this.getKey() + '/'
				: parentPath + this.getKey()
			: this.isFolder()
				? parentPath + '/' + this.getKey() + '/'
				: parentPath + '/' + this.getKey();
	}

	@Override
	public BaseObject getParentalData() {
		
		final BaseEntry<?> parent = this.entry.getParent();
		return parent == null
			? null
			: parent.getData();
	}

	@Override
	public String getParentGuid() {
		
		return this.entry.getParentGuid();
	}

	@Override
	public BaseSchedule getSchedule() {
		
		return new AbstractSchedule(false, this.entry.getSchedule()) {
			
			@Override
			public void commit() {
				
				ChangeForHistory.this.changeSchedule = this;
			}
		};
	}

	@Override
	public StorageImpl getStorageImpl() {
		
		return this.entry.getStorageImpl();
	}

	@Override
	public BaseSync getSynchronization() {
		
		return new AbstractSync(this.storage.getSynchronizer().createChange(this.getGuid())) {
			
			@Override
			public void commit() {
				
				ChangeForHistory.this.changeSync = this;
			}
		};
	}

	@Override
	public String getVersionId() {
		
		return this.entry.getVersionId();
	}

	@Override
	public void nestUnlink(final BaseEntry<?> entry, final boolean soft) {
		
		if (entry == null) {
			return;
		}
		if (entry.getClass() != EntryImpl.class || entry.getStorageImpl() != this.entry.getStorageImpl()) {
			entry.createChange().unlink(soft);
			return;
		}
		if (this.children == null) {
			synchronized (this) {
				if (this.children == null) {
					this.children = new ArrayList<>();
				}
			}
		}
		this.children.add(new ChangeDoDelete((EntryImpl) entry, soft));
		return;
	}

	@Override
	public final void segregate() {
		
		throw new UnsupportedOperationException("'segregate' is unsupported on non-commited entries!");
	}

	@Override
	public void setCommitLogged() {
		
		this.modifyLogged = true;
	}

	@Override
	public void setCreateLinkedIn(final BaseEntry<?> folder) {
		
		if (this.linkedIn == null) {
			this.linkedIn = Create.tempSet();
		}
		this.linkedIn.add(folder.getGuid());
	}

	@Override
	public void setCreateLinkedIn(final BaseEntry<?> folder, final String key) {
		
		if (this.linkedIn == null) {
			this.linkedIn = Create.tempSet();
		}
		this.linkedIn.add(folder.getGuid() + '\n' + key);
	}

	@Override
	public void setCreateLinkedWith(final BaseEntry<?> entry) {
		
		throw new UnsupportedOperationException("Only new uncommited objects can be linked!");
	}

	@Override
	public void setCreateLocal(final boolean local) {
		
		this.local = local;
	}

	@Override
	public void setParentGuid(final String parentGuid) {
		
		this.parentGuid = parentGuid;
	}

	@Override
	public final void unlink() {
		
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.createTransaction();
		try {
			transaction.unlink(this.entry.getOriginalLink(), false);
			transaction.commit();
		} catch (final Error | RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}

	@Override
	public final void unlink(final boolean soft) {
		
		this.entry.getType().onBeforeDelete(this.entry);
		final Transaction transaction = this.storage.createTransaction();
		try {
			transaction.unlink(this.entry.getOriginalLink(), soft);
			transaction.commit();
		} catch (final Error | RuntimeException e) {
			transaction.rollback();
			throw e;
		} catch (final Throwable t) {
			transaction.rollback();
			throw new RuntimeException(t);
		}
	}

	@Override
	protected StorageImpl getPlugin() {
		
		return this.entry.getStorageImpl();
	}
}
