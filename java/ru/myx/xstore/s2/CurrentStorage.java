/*
 * Created on 27.06.2004
 */
package ru.myx.xstore.s2;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.BaseRecycled;
import ru.myx.ae1.storage.ListByValue;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.storage.ModuleRecycler;
import ru.myx.ae1.storage.ModuleSchedule;
import ru.myx.ae1.storage.ModuleSynchronizer;
import ru.myx.ae3.control.ControlBasic;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.lock.Interest;
import ru.myx.jdbc.lock.LockManager;
import ru.myx.jdbc.lock.Locker;

/**
 * @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class CurrentStorage implements ModuleInterface, ModuleRecycler, LockManager {
	
	private final StorageLevel2 parent;
	
	private final Server server;
	
	CurrentStorage(final StorageLevel2 parent, final Server server) {
		this.parent = parent;
		this.server = server;
	}
	
	@Override
	public final boolean addInterest(final Interest interest) {

		return this.server.addInterest(interest);
	}
	
	@Override
	public final BaseEntry<?> apply(final String key) {

		if (key == null) {
			return null;
		}
		try {
			final BaseLink link = this.server.getLink(key);
			if (link == null) {
				return null;
			}
			return new EntryImpl(this.parent, this, link);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public boolean areAliasesSupported() {

		return this.parent.areAliasesSupported();
	}
	
	@Override
	public boolean areHistoriesSupported() {

		return this.parent.areObjectHistoriesSupported();
	}
	
	@Override
	public boolean areLinksSupported() {

		return this.parent.areLinksSupported();
	}
	
	@Override
	public final boolean areSchedulesSupported() {

		return this.server.getScheduling() != null;
	}
	
	@Override
	public final boolean areSynchronizationsSupported() {

		return this.server.areSynchronizationsSupported();
	}
	
	@Override
	public boolean areVersionsSupported() {

		return this.parent.areObjectVersionsSupported();
	}
	
	@Override
	public final void clearAllRecycled() {

		this.server.clearAllRecycled();
	}
	
	@Override
	public final Locker createLock(final String guid, final int version) {

		return this.server.createLock(guid, version);
	}
	
	final Transaction createTransaction() {

		return this.server.createTransaction();
	}
	
	@Override
	public final BaseEntry<?> getByAlias(final String alias, final boolean all) {

		try {
			final BaseLink link = this.server.getLinkForAlias(alias, all);
			if (link == null) {
				return null;
			}
			return new EntryImpl(this.parent, this, link);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseEntry<?> getByGuid(final String key) {

		if (key == null) {
			return null;
		}
		try {
			final BaseLink link = this.server.getLink(key);
			if (link == null) {
				return null;
			}
			return new EntryImpl(this.parent, this, link);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseEntry<?> getByGuidClean(final String key, final String typeName) {

		try {
			final BaseLink link = this.server.getLink(key);
			if (link == null) {
				return null;
			}
			return new EntryImpl(this.parent, this, link, typeName);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Override
	public final BaseRecycled[] getRecycled() {

		return this.server.getRecycled();
	}
	
	@Override
	public final BaseRecycled getRecycledByGuid(final String guid) {

		return this.server.getRecycledByGuid(guid);
	}
	
	@Override
	public final BaseEntry<?> getRoot() {

		return this.getByGuid(this.getRootIdentifier());
	}
	
	@Override
	public final String getRootIdentifier() {

		return "$$ROOT_ENTRY";
	}
	
	final ModuleSchedule getScheduling() {

		return this.server.getScheduling();
	}
	
	final ModuleSynchronizer getSynchronizer() {

		return this.server.getSynchronizer();
	}
	
	@Override
	public final boolean removeInterest(final Interest interest) {

		return this.server.removeInterest(interest);
	}
	
	@Override
	public final Collection<ControlBasic<?>> searchForIdentity(final String guid, final boolean all) {

		try {
			final String[] links = this.server.searchLinksForIdentity(guid, all);
			if (links == null || links.length == 0) {
				return null;
			}
			if (links.length == 1) {
				final List<ControlBasic<?>> result = new ArrayList<>(1);
				result.add(this.getByGuid(links[0]));
				return result;
			}
			return new ListByValue<>(links, this);
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	final void start() {

		final String rootId = this.getRootIdentifier();
		final BaseEntry<?> root = this.getByGuid(rootId);
		if (root == null) {
			Report.info("S2/STORAGE", "No root (id=" + rootId + ") entry found, trying to create...");
			final BaseChange change = new ChangeForNew(this.parent, this, "*", rootId, null);
			change.setKey("root");
			change.setTypeName("SystemRoot");
			change.setTitle("Storage: " + this.parent.getMnemonicName());
			change.setState(ModuleInterface.STATE_READY);
			change.setFolder(true);
			change.commit();
			Report.info("S2/STORAGE", "No root (id=" + rootId + ") entry found, seems to be created, check=" + this.getByGuid(rootId));
		}
		this.server.start();
	}
	
	@Override
	public final void start(final String identity) {

		this.server.start(identity);
	}
	
	final void stop() {

		this.server.stop();
	}
	
	@Override
	public final void stop(final String identity) {

		this.server.stop(identity);
	}
}
