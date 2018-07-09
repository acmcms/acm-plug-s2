/*
 * Created on 30.06.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import ru.myx.ae1.storage.BaseChange;
import ru.myx.ae1.storage.BaseEntry;
import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae1.types.Type;
import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Act;
import ru.myx.ae3.base.Base;
import ru.myx.ae3.base.BaseMap;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.control.field.ControlField;
import ru.myx.ae3.control.fieldset.ControlFieldset;
import ru.myx.ae3.help.Convert;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.report.Report;
import ru.myx.jdbc.lock.Runner;
import ru.myx.util.EntrySimple;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.StorageLevel2;
import ru.myx.xstore.s2.indexing.IndexingS2;

/** @author myx */
final class RunnerChangeUpdate implements Runnable, Runner {

	private static final String OWNER = "S2/UPDATE";

	private static final int LIMIT_BULK_TASKS = 32;

	private static final int LIMIT_BULK_UPGRADE = 16;

	private static final long PERIOD_CUT_LOG = 1000L * 60L * 60L * 24L * 7L;

	private static final long PERIOD_CUT_HISTORY = 30L * 1000L * 60L * 60L * 24L;

	private static final long PERIOD_CUT_RECYCLED = 2L * 1000L * 60L * 60L * 24L * 7L;

	private static final int SEQ_HIGH = 4255397;

	private static final Set<String> SYSTEM_KEYS = RunnerChangeUpdate.createSystemKeys();

	private static final Set<String> createSystemKeys() {

		final Set<String> result = Create.tempSet();
		result.add("$key");
		result.add("$title");
		result.add("$folder");
		result.add("$type");
		result.add("$state");
		result.add("$owner");
		return result;
	}

	private static final String getEntryTypeName(final BaseLink link) {

		try {
			return link.getTypeName();
		} catch (final Throwable t) {
			return null;
		}
	}

	private final ServerJdbc server;

	private final StorageLevel2 storage;

	private final IndexingS2 indexing;

	private final int indexingVersion;

	private boolean destroyed = false;

	RunnerChangeUpdate(final StorageLevel2 storage, final ServerJdbc server, final IndexingS2 indexing) {
		
		this.storage = storage;
		this.server = server;
		this.indexing = indexing;
		this.indexingVersion = indexing.getVersion();
	}

	private final boolean analyzeCreateSyncs(final Connection conn,
			final int entryLuid,
			final String entryGuid,
			final String entryKey,
			final String parentGuid,
			final String updateName,
			final String taskName) throws Throwable {
		
		boolean heavyLoad = false;
		final List<Map.Entry<String, String>> list = new ArrayList<>();
		// Do synchronization
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT s.lnkTgtId FROM " + this.server.getTnTreeSync() + " s LEFT OUTER JOIN " + this.server.getTnTree()
						+ " t ON t.cntLnkId=s.lnkTgtId AND t.lnkName=? WHERE s.lnkSrcId=? AND t.lnkId is NULL",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, entryKey);
			ps.setString(2, parentGuid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					list.add(new EntrySimple<>(Engine.createGuid(), rs.getString(1)));
				}
			}
		}
		// Create links
		{
			for (final Map.Entry<String, String> current : list) {
				final String cntLnkId = current.getValue();
				final BaseEntry<?> target = this.server.getStorage().getInterface().getByGuid(cntLnkId);
				if (target != null) {
					final String newGuid = current.getKey();
					try (final PreparedStatement ps = conn.prepareStatement(
							"INSERT INTO " + this.server.getTnTree() + "(lnkId,cntLnkId,lnkName,objId,lnkSort,lnkFolder) SELECT ?,?,t.lnkName,t.objId,t.lnkSort,t.lnkFolder FROM "
									+ this.server.getTnTree() + " t WHERE t.lnkLuid=" + entryLuid)) {
						ps.setString(1, newGuid);
						ps.setString(2, cntLnkId);
						ps.executeUpdate();
						MatChange.serialize(this.server, conn, 0, updateName, newGuid, -1, 0);
						heavyLoad = true;
					}
				}
			}
		}
		this.createSyncs(conn, list, entryGuid, parentGuid, taskName);
		return heavyLoad;
	}

	private final void createSyncs(final Connection conn, final List<Map.Entry<String, String>> created, final String entryGuid, final String parentGuid, final String taskName)
			throws Throwable {
		
		for (final Map.Entry<String, String> item : created) {
			final String newGuid = item.getKey();
			final String cntLnkId = item.getValue();
			try (final PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO " + this.server.getTnTreeSync() + "(lnkSrcId,lnkTgtId) (" + "SELECT ?,? FROM " + this.server.getTnTreeSync() + " s LEFT OUTER JOIN "
							+ this.server.getTnTreeSync() + " j ON j.lnkSrcId=? AND j.lnkTgtId=? WHERE s.lnkSrcId=? AND s.lnkTgtId=? AND j.lnkSrcId is NULL " + "UNION "
							+ "SELECT ?,? FROM " + this.server.getTnTreeSync() + " s LEFT OUTER JOIN " + this.server.getTnTreeSync()
							+ " j ON j.lnkSrcId=? AND j.lnkTgtId=? WHERE s.lnkSrcId=? AND s.lnkTgtId=? AND j.lnkSrcId is NULL " + ")")) {
				ps.setString(0x1, entryGuid);
				ps.setString(0x2, newGuid);
				ps.setString(0x3, entryGuid);
				ps.setString(0x4, newGuid);
				ps.setString(0x5, parentGuid);
				ps.setString(0x6, cntLnkId);
				ps.setString(0x7, newGuid);
				ps.setString(0x8, entryGuid);
				ps.setString(0x9, newGuid);
				ps.setString(0xA, entryGuid);
				ps.setString(0xB, cntLnkId);
				ps.setString(0xC, parentGuid);
				final int updateCount = ps.executeUpdate();
				if (updateCount > 0) {
					Report.info(RunnerChangeUpdate.OWNER, updateCount + " syncs created (" + taskName + "), guid=" + entryGuid);
				}
			}
		}
	}

	private final void doClean(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int luid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		if (luid != -1) {
			this.doCleanIndices(conn, luid);
		}
		this.doUpdateObject(conn, guid);
	}

	private final void doCleanAll(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.lnkLuid FROM " + this.server.getTnTree() + " t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					linkData.add(rs.getString(1));
					linkData.add(new Integer(rs.getInt(2)));
				}
			}
		}
		for (final Object object : linkData) {
			final String lnkId = (String) object;
			final int luid = ((Integer) object).intValue();
			this.doCleanIndices(conn, luid);
			MatLink.unlink(this.server, conn, lnkId);
		}
		this.doUpdateObject(conn, guid);
	}

	private final void doCleanExtra(final Connection conn, final String objId) throws Exception {

		Report.event(RunnerChangeUpdate.OWNER, "CLEANING_EXTRA", "objId=" + objId);
		MatExtra.unlink(this.server, conn, null, objId);
	}

	private final void doCleanIndices(final Connection conn, final int lnkLuid) throws Exception {

		Report.event(RunnerChangeUpdate.OWNER, "CLEANING_INDEX", "luid=" + lnkLuid);
		this.indexing.doDelete(conn, lnkLuid);
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnIndexed() + " WHERE luid=?")) {
			ps.setInt(1, lnkLuid);
			ps.execute();
		}
	}

	private boolean doCleanStart(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String lnkId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> items = new ArrayList<>();
		this.fillTree(conn, items, lnkId);
		for (int i = items.size() - 1, sequence = 0; i >= 0; i--, sequence++) {
			final String object = (String) items.get(i);
			final Integer integer = (Integer) items.get(--i);
			MatChange.serialize(this.server, conn, sequence, "delete-item", object, integer.intValue(), 0L);
		}
		return items.size() >= RunnerChangeUpdate.LIMIT_BULK_TASKS;
	}

	private final boolean doCreateGlobal(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkJdbc entry = MatLink.materialize(this.server, conn, guid);
		if (entry != null) {
			final int luid = entry.getKeyLocal();
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "create/global, guid=" + guid + ", luid=" + luid);
			this.doIndex(conn, entry, luid);
			this.eventIndexed(conn, luid, true);
			final BaseLink parent = MatLink.materialize(this.server, conn, entry.getParentGuid());
			if (parent != null) {
				return this.analyzeCreateSyncs(conn, luid, guid, entry.getKey(), entry.getParentGuid(), "create-global", "create-global");
			}
		}
		return false;
	}

	private final boolean doCreateLocal(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkJdbc entry = MatLink.materialize(this.server, conn, guid);
		if (entry != null) {
			final int luid = entry.getKeyLocal();
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "create/local, guid=" + guid + ", luid=" + luid);
			this.doIndex(conn, entry, luid);
			this.eventIndexed(conn, luid, true);
		}
		return false;
	}

	private void doDeleteItem(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String objId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		MatAlias.deleteOrMove(this.server, conn, lnkLuid);
		MatLink.unlink(this.server, conn, lnkLuid);
		this.doCleanIndices(conn, lnkLuid);
		this.doUpdateObject(conn, objId);
	}

	private final boolean doIndex(final Connection conn, final BaseLink entry, final int luid) throws Throwable {

		final int records;
		{
			final Set<String> hierarchy = Create.tempSet();
			final StringBuilder parents = new StringBuilder();
			for (BaseLink current = entry;; current = this.server.getLink(current.getParentGuid())) {
				if (current == null) {
					Report.warning(RunnerChangeUpdate.OWNER, "Lost item detected! parents=" + parents);
					records = -1;
					this.makeLost(conn, entry, true);
					break;
				}
				final String parent = current.getParentGuid();
				if ("*".equals(parent)) {
					final Type<?> type = this.storage.getServer().getTypes().getType(RunnerChangeUpdate.getEntryTypeName(entry));
					final ControlFieldset<?> fieldset = type == null
						? null
						: type.getFieldsetLoad();
					final Set<String> excludeFields;
					if (fieldset == null) {
						excludeFields = Collections.emptySet();
					} else {
						final Set<String> exclude = Create.tempSet();
						final int length = fieldset.size();
						for (int i = 0; i < length; ++i) {
							final ControlField field = fieldset.get(i);
							if (!Convert.MapEntry.toBoolean(field.getAttributes(), "indexing", true)) {
								field.fillFields(exclude);
							}
						}
						if (exclude.isEmpty()) {
							excludeFields = Collections.emptySet();
						} else {
							excludeFields = exclude;
						}
					}
					final BaseMap data = new BaseNativeObject();
					data.baseDefineImportAllEnumerable(entry.getData());
					if (!excludeFields.isEmpty()) {
						for (final String key : excludeFields) {
							data.baseDelete(key);
						}
					}
					data.baseDefine("$key", entry.getKey());
					data.baseDefine("$title", entry.getTitle());
					data.baseDefine("$folder", entry.isFolder());
					data.baseDefine("$type", entry.getTypeName());
					data.baseDefine("$state", entry.getState());
					data.baseDefine("$owner", entry.getOwner());
					final boolean fullText;
					{
						if (fieldset == null) {
							fullText = false;
						} else {
							if (fieldset.getField("KEYWORDS") == null) {
								fullText = false;
							} else {
								fullText = true;
							}
						}
					}
					records = this.indexing.doIndex(conn, entry.getParentGuid(), hierarchy, entry.getState(), RunnerChangeUpdate.SYSTEM_KEYS, data, fullText, luid);
					break;
				}
				parents.append(parent).append(' ');
				if (!hierarchy.add(parent)) {
					Report.warning(RunnerChangeUpdate.OWNER, "Recursion detected! parents=" + parents);
					records = -1;
					this.makeLost(conn, current, false);
					break;
				}
			}
		}
		return records != -1;
	}

	private void doMaintainClearDead(final Connection conn, final BaseObject settings) {

		final int lastVersion = Convert.MapEntry.toInt(settings, "runnerVersion", 0);
		final long nextClean = Convert.MapEntry.toLong(settings, "deadCleanupDate", 0L);
		if (lastVersion < this.getVersion() || nextClean < Engine.fastTime()) {
			settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
			try {
				conn.setAutoCommit(false);
				try {
					this.doMaintainFix(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadSynchronizations(conn);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadLinks(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadRecycled(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadAliases(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadObjects(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadObjectHistories(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadObjectVersions(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadExtraLinksToObjects(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadExtraLinksToExtras(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
				try {
					this.doMaintainClearDeadExtra(conn, settings);
					conn.commit();
				} catch (final Throwable t) {
					try {
						conn.rollback();
					} catch (final Throwable tt) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
					settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
				}
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				Report.exception(RunnerChangeUpdate.OWNER, "Unexpected exception while collecting garbage in storage", t);
				settings.baseDefine("deadCleanupDate", Base.forDateMillis(Engine.fastTime() + 30L * 1000L * 60L));
			} finally {
				try {
					conn.setAutoCommit(true);
				} catch (final Throwable t) {
					// ignore
				}
			}
			settings.baseDefine("runnerVersion", this.getVersion());
			this.storage.commitProtectedSettings();
		}
	}

	private void doMaintainClearDeadAliases(final Connection conn, final BaseObject settings) {

		try {
			final List<String> links;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadAliasesGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadAliasesGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadAliasesGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadAliasesGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				links = found;
			}
			if (links != null && !links.isEmpty()) {
				for (final Iterator<String> i = links.iterator(); i.hasNext();) {
					final String alId = i.next();
					final String alLnkId = i.next();
					MatAlias.delete(this.server, conn, alId, alLnkId);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT a.alId, a.alLnkId FROM s2Aliases a LEFT OUTER JOIN s2Tree t ON a.alLnkId=t.lnkId LEFT
	 * OUTER JOIN s2RecycledTree r ON a.alLnkId=r.lnkId WHERE t.lnkId is NULL AND r.lnkId is NULL */
	private List<String> doMaintainClearDeadAliasesGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT a.alId,a.alLnkId FROM ").append(this.server.getTnAliases()).append(" a LEFT OUTER JOIN ")
				.append(this.server.getTnTree()).append(" t ON a.alLnkId=t.lnkId LEFT OUTER JOIN ").append(this.server.getTnRecycledTree())
				.append(" r ON a.alLnkId=r.lnkId WHERE t.lnkId is NULL AND r.lnkId is NULL").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private List<String> doMaintainClearDeadAliasesGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT a.alId,a.alLnkId FROM ").append(this.server.getTnAliases()).append(" a, ").append(this.server.getTnTree())
				.append(" t, ").append(this.server.getTnRecycledTree()).append(" r WHERE a.alLnkId=t.lnkId(+) AND a.alLnkId=r.lnkId(+) AND t.lnkId is NULL AND r.lnkId is NULL")
				.toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadExtra(final Connection conn, final BaseObject settings) {

		try {
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadExtraGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadExtraGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnExtra() + " WHERE recId=?")) {
						ps.setString(1, guid);
						ps.execute();
					}
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT e.recId FROM s2Extra e LEFT OUTER JOIN s2ExtraLink l ON e.recId = l.recId WHERE
	 * e.recDate < GETDATE() GROUP BY e.recId HAVING count(l.recId) = 0 */
	private List<String> doMaintainClearDeadExtraGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT e.recId FROM ").append(this.server.getTnExtra()).append(" e LEFT OUTER JOIN ").append(this.server.getTnExtraLink())
				.append(" l ON e.recId=l.recId WHERE e.recDate<? GROUP BY e.recId HAVING count(l.recId) = 0").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadExtraGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT e.recId FROM ").append(this.server.getTnExtra()).append(" e, ").append(this.server.getTnExtraLink())
				.append(" l WHERE e.recDate<? AND e.recId=l.recId(+) GROUP BY e.recId HAVING count(l.recId) = 0").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadExtraLinksToExtras(final Connection conn, final BaseObject settings) {

		try {
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadExtraLinksToExtrasGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraLinksToExtrasGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadExtraLinksToExtrasGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraLinksToExtrasGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatExtra.unlink(this.server, conn, guid, null);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT l.recId FROM s2ExtraLink l LEFT OUTER JOIN s2Extra e ON l.recId = e.recId LEFT OUTER
	 * JOIN s2Objects o ON l.objId = o.objId WHERE (o.objCreated is NULL OR o.objCreated <
	 * GETDATE()) AND e.recId is NULL GROUP BY l.recId */
	private List<String> doMaintainClearDeadExtraLinksToExtrasGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT l.recId FROM ").append(this.server.getTnExtraLink()).append(" l LEFT OUTER JOIN ").append(this.server.getTnExtra())
				.append(" e ON l.recId=e.recId LEFT OUTER JOIN ").append(this.server.getTnObjects())
				.append(" o ON l.objId=o.objId WHERE (o.objCreated is NULL OR o.objCreated<?) AND e.recId is NULL GROUP BY l.recId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadExtraLinksToExtrasGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT l.recId FROM ").append(this.server.getTnExtraLink()).append(" l, ").append(this.server.getTnExtra()).append(" e, ")
				.append(this.server.getTnObjects())
				.append(" o WHERE l.recId=e.recId(+) AND l.objId=o.objId(+) AND (o.objCreated is NULL OR o.objCreated<?) AND e.recId is NULL GROUP BY l.recId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadExtraLinksToObjects(final Connection conn, final BaseObject settings) {

		try {
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadExtraLinksToObjectsGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraLinksToObjectsGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadExtraLinksToObjectsGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadExtraLinksToObjectsGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatExtra.unlink(this.server, conn, null, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT l.objId FROM s2ExtraLink l LEFT OUTER JOIN s2Objects o ON l.objId = o.objId LEFT OUTER
	 * JOIN s2ObjectHistory h ON l.objId = h.hsId LEFT OUTER JOIN s2ObjectVersions v ON l.objId =
	 * v.vrId LEFT OUTER JOIN s2Extra e ON l.recId = e.recId WHERE (e.recDate is NULL OR e.recDate <
	 * GETDATE()) AND o.objId is NULL AND h.hsId is NULL AND v.vrId is NULL GROUP BY l.objId SELECT
	 * l.objId FROM s2ExtraLink l LEFT OUTER JOIN s2Objects o ON l.objId = o.objId LEFT OUTER JOIN
	 * s2Extra e ON l.recId = e.recId WHERE (e.recDate is NULL OR e.recDate < GETDATE()) AND o.objId
	 * is NULL GROUP BY l.objId */
	private List<String> doMaintainClearDeadExtraLinksToObjectsGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT l.objId FROM ").append(this.server.getTnExtraLink()).append(" l LEFT OUTER JOIN ")
				.append(this.server.getTnObjects()).append(" o ON l.objId=o.objId LEFT OUTER JOIN ").append(this.server.getTnObjectHistory())
				.append(" h ON l.objId=h.hsId LEFT OUTER JOIN ").append(this.server.getTnObjectVersions()).append(" v ON l.objId=v.vrId LEFT OUTER JOIN ")
				.append(this.server.getTnExtra())
				.append(" e ON l.recId=e.recId WHERE (e.recDate is NULL OR e.recDate<?) AND o.objId is NULL AND h.hsId is NULL AND v.vrId is NULL GROUP BY l.objId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadExtraLinksToObjectsGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT l.objId FROM ").append(this.server.getTnExtraLink()).append(" l, ").append(this.server.getTnObjects())
				.append(" o, ").append(this.server.getTnObjectHistory()).append(" h, ").append(this.server.getTnObjectVersions()).append(" v, ").append(this.server.getTnExtra())
				.append(
						" e WHERE l.objId=o.objId(+) AND l.objId=h.hsId(+) AND l.objId=v.vrId(+) AND l.recId=e.recId(+) AND (e.recDate is NULL OR e.recDate<?) AND o.objId is NULL AND h.hsId is NULL AND v.vrId is NULL GROUP BY l.objId")
				.toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	/* SELECT h.objId FROM s2ObjectHistory h LEFT OUTER JOIN s2Objects o ON h.objId = o.objId WHERE
	 * o.objId is NULL GROUP BY h.objId */
	private List<String> doMaintainClearDeadHistoryGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT h.objId FROM ").append(this.server.getTnObjectHistory()).append(" h LEFT OUTER JOIN ")
				.append(this.server.getTnObjects()).append(" o ON h.objId=o.objId WHERE o.objId is NULL GROUP BY h.objId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadHistoryGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT h.objId FROM ").append(this.server.getTnObjectHistory()).append(" h, ").append(this.server.getTnObjects())
				.append(" o WHERE h.objId=o.objId(+) AND o.objId is NULL GROUP BY h.objId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadLinks(final Connection conn, final BaseObject settings) {

		try {
			final List<Object> links;
			{
				List<Object> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadLinksGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadLinksGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadLinksGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadLinksGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				links = found;
			}
			if (links != null && !links.isEmpty()) {
				for (final Object object : links) {
					final int luid = ((Integer) object).intValue();
					final String guid = object.toString();
					MatLink.unlink(this.server, conn, luid);
					MatChange.serialize(this.server, conn, 0, "clean", guid, luid, 0);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT t1.lnkLuid, t1.objId FROM s2Tree t1 LEFT OUTER JOIN s2Tree t2 ON t1.cntLnkId=t2.lnkId
	 * LEFT OUTER JOIN s2Objects o ON t1.objId=o.objId WHERE (t2.lnkId is NULL OR o.objId is NULL)
	 * AND t1.cntLnkId != '*' */
	private List<Object> doMaintainClearDeadLinksGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t1.lnkLuid,t1.objId FROM ").append(this.server.getTnTree()).append(" t1 LEFT OUTER JOIN ")
				.append(this.server.getTnTree()).append(" t2 ON t1.cntLnkId=t2.lnkId LEFT OUTER JOIN ").append(this.server.getTnObjects())
				.append(" o ON t1.objId=o.objId WHERE (t2.lnkId is NULL OR o.objId is NULL) AND t1.cntLnkId != '*'").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(new Integer(rs.getInt(1)));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<Object> doMaintainClearDeadLinksGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t1.lnkLuid,t1.objId FROM ").append(this.server.getTnTree()).append(" t1, ").append(this.server.getTnTree())
				.append(" t2, ").append(this.server.getTnObjects())
				.append(" o WHERE t1.cntLnkId=t2.lnkId(+) AND t1.objId=o.objId(+) AND (t2.lnkId is NULL OR o.objId is NULL) AND t1.cntLnkId != '*'").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(new Integer(rs.getInt(1)));
						result.add(rs.getString(2));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadObjectHistories(final Connection conn, final BaseObject settings) {

		try {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnObjectHistory() + " WHERE hsDate<?")) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime() - RunnerChangeUpdate.PERIOD_CUT_HISTORY));
				ps.execute();
			}
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadHistoryGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadHistoryGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadHistoryGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadHistoryGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatHistory.clear(this.server, conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void doMaintainClearDeadObjects(final Connection conn, final BaseObject settings) {

		try {
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadObjectsGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadObjectsGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadObjectsGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadObjectsGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					this.doCleanExtra(conn, guid);
					MatData.delete(this.server, conn, guid);
					MatHistory.clear(this.server, conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT o.objId FROM s2Objects o LEFT OUTER JOIN s2Tree t ON o.objId = t.objId LEFT OUTER JOIN
	 * s2RecycledTree r ON o.objId = r.objId WHERE o.objCreated < GETDATE() AND t.objId is NULL AND
	 * r.objId is NULL */
	private List<String> doMaintainClearDeadObjectsGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT o.objId FROM ").append(this.server.getTnObjects()).append(" o LEFT OUTER JOIN ").append(this.server.getTnTree())
				.append(" t ON o.objId=t.objId LEFT OUTER JOIN ").append(this.server.getTnRecycledTree())
				.append(" r ON o.objId=r.objId WHERE o.objCreated<? AND t.objId is NULL AND r.objId is NULL").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 12L * 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadObjectsGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT o.objId FROM ").append(this.server.getTnObjects()).append(" o, ").append(this.server.getTnTree()).append(" t, ")
				.append(this.server.getTnRecycledTree()).append(" r WHERE o.objCreated<? AND o.objId=t.objId(+) AND o.objId=r.objId(+) AND t.objId is NULL AND r.objId is NULL")
				.toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 12L * 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private void doMaintainClearDeadObjectVersions(final Connection conn, final BaseObject settings) {

		try {
			final List<String> objects;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadVersionsGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadVersionsGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadVersionsGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadVersionsGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				objects = found;
			}
			if (objects != null && !objects.isEmpty()) {
				for (final String guid : objects) {
					MatVersion.clear(this.server, conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void doMaintainClearDeadRecycled(final Connection conn, final BaseObject settings) {

		try {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnRecycled() + " WHERE delDate<?")) {
				ps.setTimestamp(1, new Timestamp(Engine.fastTime() - RunnerChangeUpdate.PERIOD_CUT_RECYCLED));
				ps.execute();
			}
			final List<String> links;
			{
				List<String> found;
				final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
				if (joinType == 0) {
					try {
						found = this.doMaintainClearDeadRecycledGetThem0(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadRecycledGetThem1(conn);
							settings.baseDefine("dbmsOuterJoinType", 1);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				} else {
					try {
						found = this.doMaintainClearDeadRecycledGetThem1(conn);
					} catch (final SQLException e) {
						try {
							found = this.doMaintainClearDeadRecycledGetThem0(conn);
							settings.baseDefine("dbmsOuterJoinType", 0);
						} catch (final SQLException e2) {
							throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
						}
					}
				}
				links = found;
			}
			if (links != null && !links.isEmpty()) {
				for (final String guid : links) {
					MatRecycled.clearRecycled(this.server, conn, guid);
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT t.delId FROM s2RecycledTree t LEFT OUTER JOIN s2Recycled r ON t.delId=r.delRootId
	 * WHERE r.delRootId is NULL GROUP BY t.delId */
	private List<String> doMaintainClearDeadRecycledGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t.delId FROM ").append(this.server.getTnRecycledTree()).append(" t LEFT OUTER JOIN ")
				.append(this.server.getTnRecycled()).append(" r ON t.delId=r.delRootId WHERE r.delRootId is NULL GROUP BY t.delId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadRecycledGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t.delId FROM ").append(this.server.getTnRecycledTree()).append(" t, ").append(this.server.getTnRecycled())
				.append(" r WHERE t.delId=r.delRootId(+) AND r.delRootId is NULL GROUP BY t.delId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearDeadSynchronizations(final Connection conn) {

		try {
			try (final Statement st = conn.createStatement()) {
				{
					final StringBuilder query = new StringBuilder();
					query.append("DELETE FROM ");
					query.append(this.server.getTnTreeSync());
					query.append(" WHERE lnkSrcId in (SELECT s.lnkSrcId FROM ");
					query.append(this.server.getTnTreeSync());
					query.append(" s LEFT OUTER JOIN ");
					query.append(this.server.getTnTree());
					query.append(" t ON s.lnkSrcId=t.lnkId WHERE lnkId is NULL)");
					if (st.executeUpdate(query.toString()) > 0) {
						Report.warning(RunnerChangeUpdate.OWNER, "SYNC-SRC-DEAD happened!");
					}
				}
				{
					final StringBuilder query = new StringBuilder();
					query.append("DELETE FROM ");
					query.append(this.server.getTnTreeSync());
					query.append(" WHERE lnkTgtId in (SELECT s.lnkTgtId FROM ");
					query.append(this.server.getTnTreeSync());
					query.append(" s LEFT OUTER JOIN ");
					query.append(this.server.getTnTree());
					query.append(" t ON s.lnkTgtId=t.lnkId WHERE lnkId is NULL)");
					if (st.executeUpdate(query.toString()) > 0) {
						Report.warning(RunnerChangeUpdate.OWNER, "SYNC-TGT-DEAD happened!");
					}
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT v.objId FROM s2ObjectVersions v LEFT OUTER JOIN s2Objects o ON v.objId = o.objId WHERE
	 * o.objId is NULL GROUP BY v.objId */
	private List<String> doMaintainClearDeadVersionsGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT v.objId FROM ").append(this.server.getTnObjectVersions()).append(" v LEFT OUTER JOIN ")
				.append(this.server.getTnObjects()).append(" o ON v.objId=o.objId WHERE o.objId is NULL GROUP BY v.objId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<String> doMaintainClearDeadVersionsGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT v.objId FROM ").append(this.server.getTnObjectVersions()).append(" v, ").append(this.server.getTnObjects())
				.append(" o WHERE v.objId=o.objId(+) AND o.objId is NULL GROUP BY v.objId").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainClearLog(final Connection conn, final BaseObject settings) {

		final long nextClean = Convert.MapEntry.toLong(settings, "changeLogCleanupDate", 0L);
		if (nextClean < Engine.fastTime()) {
			try {
				try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnChangeLog() + " WHERE evtDate<?")) {
					ps.setTimestamp(1, new Timestamp(Engine.fastTime() - RunnerChangeUpdate.PERIOD_CUT_LOG));
					ps.execute();
				}
				settings.baseDefine("changeLogCleanupDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
			} catch (final SQLException e) {
				settings.baseDefine("changeLogCleanupDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
				throw new RuntimeException(e);
			}
		}
	}

	private void doMaintainFix(final Connection conn) {

		try {
			final StringBuilder query = new StringBuilder();
			query.append("UPDATE ");
			query.append(this.server.getTnTree());
			query.append(" SET cntLnkId=? ");
			query.append(" WHERE cntLnkId!=? AND lnkId=?");
			try (final PreparedStatement ps = conn.prepareStatement(query.toString())) {
				ps.setString(1, "*");
				ps.setString(2, "*");
				ps.setString(3, "$$ROOT_ENTRY");
				if (ps.executeUpdate() > 0) {
					Report.warning(RunnerChangeUpdate.OWNER, "ROOT-FIX happened!");
				}
			}
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	/* SELECT t.lnkId, t.lnkLuid FROM s2Tree t LEFT OUTER JOIN s2Indexed i ON t.lnkLuid=i.luid LEFT
	 * OUTER JOIN s2Objects o ON t.objId=o.objId WHERE i.luid is NULL AND o.objDate <GETDATE() */
	private List<Object> doMaintainFixIndicesGetThem0(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t.lnkId,t.lnkLuid FROM ").append(this.server.getTnTree()).append(" t LEFT OUTER JOIN ")
				.append(this.server.getTnIndexed()).append(" i  ON t.lnkLuid=i.luid LEFT OUTER JOIN ").append(this.server.getTnObjects())
				.append(" o ON t.objId=o.objId WHERE i.luid is NULL AND o.objDate<?").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(new Integer(rs.getInt(2)));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private List<Object> doMaintainFixIndicesGetThem1(final Connection conn) throws SQLException {

		final String query = new StringBuilder().append("SELECT t.lnkId,t.lnkLuid FROM ").append(this.server.getTnTree()).append(" t, ").append(this.server.getTnIndexed())
				.append(" i, ").append(this.server.getTnObjects()).append(" o WHERE t.lnkLuid=i.luid(+) AND t.objId=o.objId AND i.luid is NULL AND o.objDate<?").toString();
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime() - 1000L * 60L * 60L));
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add(rs.getString(1));
						result.add(new Integer(rs.getInt(2)));
					} while (rs.next());
					return result;
				}
				return null;
			}
		}
	}

	private void doMaintainIndexing(final Connection conn, final BaseObject settings) {

		final int indexingVersion = Convert.MapEntry.toInt(settings, "indexingVersion", 0);
		if (indexingVersion != this.indexingVersion) {
			try {
				MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", this.indexingVersion, 0L);
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
			settings.baseDefine("indexingVersion", this.indexingVersion);
			this.storage.commitProtectedSettings();
		}
		final long nextClean = Convert.MapEntry.toLong(settings, "indexCheckDate", 0L);
		if (nextClean < Engine.fastTime()) {
			try {
				try {
					final List<Object> links;
					{
						List<Object> found;
						final int joinType = Convert.MapEntry.toInt(settings, "dbmsOuterJoinType", 0);
						if (joinType == 0) {
							try {
								found = this.doMaintainFixIndicesGetThem0(conn);
							} catch (final SQLException e) {
								try {
									found = this.doMaintainFixIndicesGetThem1(conn);
									settings.baseDefine("dbmsOuterJoinType", 1);
								} catch (final SQLException e2) {
									throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
								}
							}
						} else {
							try {
								found = this.doMaintainFixIndicesGetThem1(conn);
							} catch (final SQLException e) {
								try {
									found = this.doMaintainFixIndicesGetThem0(conn);
									settings.baseDefine("dbmsOuterJoinType", 0);
								} catch (final SQLException e2) {
									throw new RuntimeException("Unsupported OUTER JOIN type in DBMS!", e2);
								}
							}
						}
						links = found;
					}
					if (links != null && !links.isEmpty()) {
						for (final Object object : links) {
							final String guid = object.toString();
							final int luid = ((Integer) object).intValue();
							MatChange.serialize(this.server, conn, 0, "update", guid, luid, 0);
						}
					}
				} catch (final RuntimeException e) {
					throw e;
				}
				settings.baseDefine("indexCheckDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
			} catch (final SQLException e) {
				settings.baseDefine("indexCheckDate", Base.forDateMillis(Engine.fastTime() + 1000L * 60L * 60L * 24L));
				this.storage.commitProtectedSettings();
				throw new RuntimeException(e);
			}
		}
	}

	private void doMaintainVersionUpdate(final Connection conn, final BaseObject settings) {

		final int systemVersion = Convert.MapEntry.toInt(settings, "systemVersion", 0);
		if (systemVersion != this.indexingVersion) {
			try {
				if (systemVersion < 24) {
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 24 start");
					int updated = 0;
					try (final Statement statement = conn.createStatement()) {
						updated += statement.executeUpdate(
								"INSERT INTO " + this.server.getTnTreeSync() + "(lnkSrcId,lnkTgtId) SELECT t1.lnkId,t2.lnkId FROM " + this.server.getTnTree() + " t1 INNER JOIN "
										+ this.server.getTnTree() + " t2 ON t1.objId=t2.objId LEFT OUTER JOIN " + this.server.getTnTreeSync()
										+ " s ON s.lnkSrcId=t1.lnkId AND s.lnkTgtId=t2.lnkId WHERE t1.lnkLuid!=t2.lnkLuid AND s.lnkSrcId IS NULL");
						updated += statement.executeUpdate(
								"INSERT INTO " + this.server.getTnTreeSync() + "(lnkTgtId,lnkSrcId) SELECT t1.lnkId,t2.lnkId FROM " + this.server.getTnTree() + " t1 INNER JOIN "
										+ this.server.getTnTree() + " t2 ON t1.objId=t2.objId LEFT OUTER JOIN " + this.server.getTnTreeSync()
										+ " s ON s.lnkTgtId=t1.lnkId AND s.lnkSrcId=t2.lnkId WHERE t1.lnkLuid!=t2.lnkLuid AND s.lnkTgtId IS NULL");
					}
					Report.info(RunnerChangeUpdate.OWNER, "Version update routine 24 done, updated=" + updated);
				}
			} catch (final SQLException e) {
				throw new RuntimeException(e);
			}
			settings.baseDefine("systemVersion", this.getVersion());
			this.storage.commitProtectedSettings();
		}
	}

	private final void doRecycleAll(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.lnkLuid FROM " + this.server.getTnTree() + " t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					linkData.add(rs.getString(1));
					linkData.add(new Integer(rs.getInt(2)));
				}
			}
		}
		for (final Object object : linkData) {
			final String lnkId = (String) object;
			final int luid = ((Integer) object).intValue();
			try {
				MatLink.recycle(this.server, conn, lnkId);
				MatChange.serialize(this.server, conn, 0, "recycle-start", lnkId, luid, 0);
			} catch (final SQLException e) {
				Report.exception(RunnerChangeUpdate.OWNER, "sql error while starting recycler process for link '" + lnkId + "', skipping", e);
			}
		}
	}

	private void doRecycleFinish(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doCleanIndices(conn, lnkLuid);
		MatLink.unlink(this.server, conn, delId);
	}

	private void doRecycleItem(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doCleanIndices(conn, lnkLuid);
		MatRecycled.recycleLink(this.server, conn, delId, lnkLuid);
	}

	private boolean doRecycleStart(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String delId = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int lnkLuid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doCleanIndices(conn, lnkLuid);
		final List<Object> items = new ArrayList<>();
		this.fillTree(conn, items, lnkLuid);
		if (!items.isEmpty()) {
			for (int i = items.size() - 1, sequence = 0; i >= 0; i--, sequence++) {
				final Integer integer = (Integer) items.get(--i);
				MatChange.serialize(this.server, conn, sequence, "recycle-item", delId, integer.intValue(), 0L);
			}
			MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "recycle-finish", delId, lnkLuid, 0L);
			return items.size() >= RunnerChangeUpdate.LIMIT_BULK_TASKS;
		}
		MatLink.unlink(this.server, conn, delId);
		this.doCleanIndices(conn, lnkLuid);
		return false;
	}

	private final boolean doResync(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final LinkJdbc entry = MatLink.materialize(this.server, conn, guid);
		if (entry != null) {
			final int luid = entry.getKeyLocal();
			final BaseLink parent = this.server.getLink(entry.getParentGuid());
			if (parent != null) {
				return this.analyzeCreateSyncs(conn, luid, guid, entry.getKey(), entry.getParentGuid(), "create", "resync");
			}
		}
		return false;
	}

	private final void doUpdate(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final int luid = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		final long date = Convert.MapEntry.toLong(task, "evtDate", 0L);
		this.doUpdate(conn, guid, luid, date);
	}

	private final void doUpdate(final Connection conn, final String lnkId, final int lnkLuid, final long date) throws Throwable {

		if (lnkLuid != -1 && date > 0L) {
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT lnkIndexed FROM " + this.server.getTnIndexed() + " WHERE luid=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setInt(1, lnkLuid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final Date indexed = rs.getDate(1);
						if (indexed != null) {
							if (indexed.getTime() > date) {
								Report.event(
										RunnerChangeUpdate.OWNER,
										"INDEXING",
										"skipped, already done, guid=" + lnkId + ", luid=" + lnkLuid + ", indexedAt=" + indexed + ", requestedAt=" + new Date(date));
								return;
							}
						}
					}
				}
			}
		}
		final LinkJdbc link = MatLink.materialize(this.server, conn, lnkId);
		if (link != null) {
			final int luid = link.getKeyLocal();
			Report.event(RunnerChangeUpdate.OWNER, "INDEXING", "update, guid=" + lnkId + ", luid=" + luid);
			this.doIndex(conn, link, luid);
			this.eventIndexed(conn, luid, false);
			return;
		}
		if (lnkLuid != -1 && !"$$ROOT_ENTRY".equals(lnkId)) {
			Report.event(RunnerChangeUpdate.OWNER, "CLEANING", "luid=" + lnkLuid);
			this.indexing.doDelete(conn, lnkLuid);
		}
	}

	private final void doUpdateAll(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		final List<Object> linkData = new ArrayList<>();
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT t.lnkId,t.lnkLuid FROM " + this.server.getTnTree() + " t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					linkData.add(rs.getString(1));
					linkData.add(new Integer(rs.getInt(2)));
				}
			}
		}
		if (linkData.size() > 16) {
			for (final Object object : linkData) {
				final String id = (String) object;
				final int luid = ((Integer) object).intValue();
				MatChange.serialize(this.server, conn, 0, "update", id, luid, 0);
			}
		} else {
			final long date = Convert.MapEntry.toLong(task, "evtDate", 0L);
			for (final Object object : linkData) {
				final String id = (String) object;
				final int luid = ((Integer) object).intValue();
				this.doUpdate(conn, id, luid, date);
			}
		}
	}

	private final void doUpdateObject(final Connection conn, final Map<String, Object> task) throws Throwable {

		final String guid = Convert.MapEntry.toString(task, "evtCmdGuid", "").trim();
		this.doUpdateObject(conn, guid);
	}

	private final void doUpdateObject(final Connection conn, final String guid) throws Throwable {

		int count = 0;
		if (conn.getMetaData().supportsUnionAll()) {
			try (final PreparedStatement ps = conn.prepareStatement(
					"SELECT COUNT(*) FROM " + this.server.getTnTree() + " t WHERE t.objId=? UNION ALL SELECT COUNT(*) FROM " + this.server.getTnRecycledTree()
							+ " r WHERE r.objId=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				ps.setString(2, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
		} else {
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT COUNT(*) FROM " + this.server.getTnTree() + " t WHERE t.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
			try (final PreparedStatement ps = conn
					.prepareStatement("SELECT COUNT(*) FROM " + this.server.getTnRecycledTree() + " r WHERE r.objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
				ps.setString(1, guid);
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						count += rs.getInt(1);
					}
				}
			}
		}
		if (count == 0) {
			this.doCleanExtra(conn, guid);
			MatData.delete(this.server, conn, guid);
			MatHistory.clear(this.server, conn, guid);
		}
	}

	private final void doUpgradeIndex(final Connection conn, final int toVersion) throws Throwable {

		if (this.indexingVersion < toVersion) {
			MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", toVersion, 0L);
			return;
		}
		final List<Map.Entry<String, Integer>> toUpgrade = new ArrayList<>();
		try (final PreparedStatement ps = conn.prepareStatement(
				"SELECT t.lnkId,t.lnkLuid FROM " + this.server.getTnTree() + " t, " + this.server.getTnIndexed() + " i WHERE t.lnkLuid=i.luid AND i.idxVersion<?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY)) {
			ps.setInt(1, toVersion);
			ps.setMaxRows(RunnerChangeUpdate.LIMIT_BULK_UPGRADE);
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					toUpgrade.add(new EntrySimple<>(rs.getString(1), new Integer(rs.getInt(2))));
				}
			}
		}
		if (!toUpgrade.isEmpty()) {
			if (toUpgrade.size() == RunnerChangeUpdate.LIMIT_BULK_UPGRADE) {
				MatChange.serialize(this.server, conn, RunnerChangeUpdate.SEQ_HIGH, "upgrade-index", "*", this.indexingVersion, 0L);
			}
			for (final Map.Entry<String, Integer> entry : toUpgrade) {
				MatChange.serialize(this.server, conn, 0, "update", entry.getKey(), entry.getValue().intValue(), 0L);
			}
		}
	}

	private final void doUpgradeIndex(final Connection conn, final Map<String, Object> task) throws Throwable {

		final int toVersion = Convert.MapEntry.toInt(task, "evtCmdLuid", -1);
		this.doUpgradeIndex(conn, toVersion);
	}

	private final void eventDone(final Connection conn, final String evtId) throws SQLException {

		try (final PreparedStatement ps = conn.prepareStatement(
				"INSERT INTO " + this.server.getTnChangeLog()
						+ "(evtId,evtDate,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid,evtCmdDate) SELECT evtId,evtDate,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid,evtCmdDate FROM "
						+ this.server.getTnChangeQueue() + " WHERE evtId=?")) {
			ps.setString(1, evtId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + this.server.getTnChangeQueue() + " WHERE evtId=?")) {
			ps.setString(1, evtId);
			ps.execute();
		}
	}

	private final void eventIndexed(final Connection conn, final int luid, final boolean created) throws SQLException {

		if (created) {
			try {
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO " + this.server.getTnIndexed() + "(luid,idxVersion,lnkIndexed) VALUES (?,?,?)")) {
					ps.setInt(1, luid);
					ps.setInt(2, this.indexingVersion);
					ps.setTimestamp(3, new Timestamp(Engine.fastTime()));
					ps.execute();
				}
			} catch (final SQLException e) {
				try (final PreparedStatement ps = conn.prepareStatement("UPDATE " + this.server.getTnIndexed() + " SET idxVersion=?, lnkIndexed=? WHERE luid=?")) {
					ps.setInt(1, this.indexingVersion);
					ps.setTimestamp(2, new Timestamp(Engine.fastTime()));
					ps.setInt(3, luid);
					ps.execute();
				}
			}
		} else {
			final int updated;
			try (final PreparedStatement ps = conn.prepareStatement("UPDATE " + this.server.getTnIndexed() + " SET idxVersion=?, lnkIndexed=? WHERE luid=?")) {
				ps.setInt(1, this.indexingVersion);
				ps.setTimestamp(2, new Timestamp(Engine.fastTime()));
				ps.setInt(3, luid);
				updated = ps.executeUpdate();
			}
			if (updated == 0) {
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO " + this.server.getTnIndexed() + "(luid,idxVersion,lnkIndexed) VALUES (?,?,?)")) {
					ps.setInt(1, luid);
					ps.setInt(2, this.indexingVersion);
					ps.setTimestamp(3, new Timestamp(Engine.fastTime()));
					ps.execute();
				}
			}
		}
	}

	private void fillTree(final Connection conn, final List<Object> list, final int luid) throws Throwable {

		final List<Object> children = MatTree.children(this.server, conn, luid);
		for (final Iterator<Object> i = children.iterator(); i.hasNext();) {
			final Integer integer = (Integer) i.next();
			final String object = (String) i.next();
			list.add(integer);
			list.add(object);
			this.fillTree(conn, list, integer.intValue());
		}
	}

	private void fillTree(final Connection conn, final List<Object> list, final String lnkId) throws Throwable {

		final List<Object> children = MatTree.children(this.server, conn, lnkId);
		for (final Iterator<Object> i = children.iterator(); i.hasNext();) {
			final Integer integer = (Integer) i.next();
			final String object = (String) i.next();
			list.add(integer);
			list.add(object);
			this.fillTree(conn, list, integer.intValue());
		}
	}

	@Override
	public int getVersion() {

		return 24;
	}

	private final void makeLost(final Connection conn, final BaseLink entry, final boolean searchUpper) throws SQLException {

		final boolean drop;
		final String query = "SELECT count(*) FROM " + this.server.getTnTree() + " t, " + this.server.getTnObjects() + " o WHERE t.objId=o.objId AND t.objId=?";
		try (final PreparedStatement ps = conn.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			/** Do not use it here, may be unsupported? */
			try {
				ps.setQueryTimeout(60);
			} catch (final Throwable t) {
				// ignore
			}
			ps.setString(1, entry.getLinkedIdentity());
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					drop = rs.getInt(1) > 1;
				} else {
					drop = false;
				}
			}
		}
		if (drop) {
			final BaseEntry<?> first = this.server.getStorage().getStorage().getByGuid(entry.getGuid());
			if (first != null) {
				final BaseChange change = first.createChange();
				change.unlink();
				Report.info(RunnerChangeUpdate.OWNER, "Lost item dropped, not last link: guid=" + entry.getGuid());
			}
			return;
		}
		final BaseEntry<?> root = this.storage.getInterface().getRoot();
		BaseEntry<?> lostAndFound = root.getChildByName("lost_found");
		if (lostAndFound == null) {
			final BaseChange change = root.createChild();
			change.setTypeName(this.storage.getServer().getTypes().getTypeNameDefault());
			change.setFolder(true);
			change.setKey("lost_found");
			change.setState(ModuleInterface.STATE_DRAFT);
			change.setTitle("! LOST + FOUND");
			change.setCreateLocal(true);
			change.commit();
			lostAndFound = root.getChildByName("lost_found");
		}
		final BaseEntry<?> first = this.server.getStorage().getStorage().getByGuid(entry.getGuid());
		BaseEntry<?> lost = first;
		if (searchUpper) {
			if (lost != null) {
				for (; lost.getParent() != null;) {
					lost = lost.getParent();
				}
				if ("$$ROOT_ENTRY".equals(lost.getGuid())) {
					lost = first;
				}
			}
		}
		if (lost != null) {
			final BaseEntry<?> existingLost = lostAndFound.getChildByName(lost.getKey());
			if (existingLost != null) {
				existingLost.createChange().delete();
			}
			final BaseChange change = lost.createChange();
			change.setParentGuid(lostAndFound.getGuid());
			change.setState(ModuleInterface.STATE_DRAFT);
			change.commit();
		}
	}

	@SuppressWarnings("resource")
	@Override
	public void run() {

		if (this.destroyed) {
			return;
		}
		final Connection conn;
		try {
			conn = this.storage.nextConnection();
			if (conn == null) {
				if (!this.destroyed) {
					Act.later(null, this, 10000L);
				}
				return;
			}
		} catch (final Throwable t) {
			if (!this.destroyed) {
				Act.later(null, this, 10000L);
			}
			return;
		}
		boolean highLoad = false;
		try {
			// do maintenance
			{
				final BaseObject settings = this.storage.getSettingsProtected();
				this.doMaintainVersionUpdate(conn, settings);
				this.doMaintainClearLog(conn, settings);
				this.doMaintainClearDead(conn, settings);
				this.doMaintainIndexing(conn, settings);
				this.storage.commitProtectedSettings();
			}
			// do update
			{
				final List<Map<String, Object>> tasks;
				try {
					try (final PreparedStatement ps = conn.prepareStatement(
							"SELECT evtId,evtCmdType,evtCmdGuid,evtCmdLuid,evtCmdDate FROM " + this.server.getTnChangeQueue() + " ORDER BY evtDate ASC, evtSequence ASC",
							ResultSet.TYPE_FORWARD_ONLY,
							ResultSet.CONCUR_READ_ONLY)) {
						ps.setMaxRows(RunnerChangeUpdate.LIMIT_BULK_TASKS);
						try (final ResultSet rs = ps.executeQuery()) {
							if (rs.next()) {
								tasks = new ArrayList<>();
								do {
									final Map<String, Object> task = Create.tempMap();
									task.put("evtId", rs.getString(1));
									task.put("evtCmdType", rs.getString(2));
									task.put("evtCmdGuid", rs.getString(3));
									task.put("evtCmdLuid", new Integer(rs.getInt(4)));
									task.put("evtCmdDate", new Long(rs.getTimestamp(5).getTime()));
									tasks.add(task);
								} while (rs.next());
							} else {
								tasks = null;
							}
						}
					}
				} catch (final SQLException e) {
					throw new RuntimeException(e);
				}
				if (tasks != null) {
					highLoad = tasks.size() >= RunnerChangeUpdate.LIMIT_BULK_TASKS;
					try {
						conn.setAutoCommit(false);
						for (final Map<String, Object> task : tasks) {
							final String taskType = Convert.MapEntry.toString(task, "evtCmdType", "").trim();
							if ("create".equals(taskType)) {
								highLoad |= this.doCreateLocal(conn, task);
							} else if ("create-global".equals(taskType)) {
								highLoad |= this.doCreateGlobal(conn, task);
							} else if ("resync".equals(taskType)) {
								highLoad |= this.doResync(conn, task);
							} else if ("update".equals(taskType)) {
								this.doUpdate(conn, task);
							} else if ("update-all".equals(taskType)) {
								this.doUpdateAll(conn, task);
							} else if ("update-object".equals(taskType)) {
								this.doUpdateObject(conn, task);
							} else if ("clean".equals(taskType)) {
								this.doClean(conn, task);
							} else if ("clean-start".equals(taskType)) {
								highLoad |= this.doCleanStart(conn, task);
							} else if ("clean-all".equals(taskType)) {
								this.doCleanAll(conn, task);
							} else if ("upgrade-index".equals(taskType)) {
								this.doUpgradeIndex(conn, task);
							} else if ("delete-item".equals(taskType)) {
								this.doDeleteItem(conn, task);
							} else if ("recycle-start".equals(taskType)) {
								highLoad |= this.doRecycleStart(conn, task);
							} else if ("recycle-all".equals(taskType)) {
								this.doRecycleAll(conn, task);
							} else if ("recycle-item".equals(taskType)) {
								this.doRecycleItem(conn, task);
							} else if ("recycle-finish".equals(taskType)) {
								this.doRecycleFinish(conn, task);
							}
							this.eventDone(conn, Convert.MapEntry.toString(task, "evtId", "").trim());
							conn.commit();
						}
					} catch (final Throwable e) {
						try {
							conn.rollback();
						} catch (final Throwable t) {
							// ignore
						}
						Report.exception(RunnerChangeUpdate.OWNER, "Exception while updating a storage", e);
					}
				}
				try {
					if (this.server.isUpdate3()) {
						Report.info("S2/UPDATE3", "update3 task loop");
						{
							final List<String> toUpdate = new ArrayList<>();
							try (final PreparedStatement ps = conn.prepareStatement("SELECT objId FROM " + this.server.getTnObjects() + " WHERE extLink=?")) {
								ps.setString(1, "-");
								ps.setMaxRows(50);
								try (final ResultSet rs = ps.executeQuery()) {
									while (rs.next()) {
										toUpdate.add(rs.getString(1));
									}
								}
							}
							Report.info("S2/UPDATE3", "update3 task objects: " + toUpdate);
							for (final String current : toUpdate) {
								highLoad = true;
								MatData.update3(this.server, conn, current);
							}
							conn.commit();
						}
						{
							final List<String> toUpdate = new ArrayList<>();
							try (final PreparedStatement ps = conn.prepareStatement("SELECT hsId FROM " + this.server.getTnObjectHistory() + " WHERE extLink=?")) {
								ps.setString(1, "-");
								ps.setMaxRows(50);
								try (final ResultSet rs = ps.executeQuery()) {
									while (rs.next()) {
										toUpdate.add(rs.getString(1));
									}
								}
							}
							Report.info("S2/UPDATE3", "update3 task objectHistory: " + toUpdate);
							for (final String current : toUpdate) {
								highLoad = true;
								MatHistory.update3(this.server, conn, current);
							}
							conn.commit();
						}
						{
							final List<String> toUpdate = new ArrayList<>();
							try (final PreparedStatement ps = conn.prepareStatement("SELECT vrId FROM " + this.server.getTnObjectVersions() + " WHERE extLink=?")) {
								ps.setString(1, "-");
								ps.setMaxRows(50);
								try (final ResultSet rs = ps.executeQuery()) {
									while (rs.next()) {
										toUpdate.add(rs.getString(1));
									}
								}
							}
							Report.info("S2/UPDATE3", "update3 task objectVersions: " + toUpdate);
							for (final String current : toUpdate) {
								highLoad = true;
								MatVersion.update3(this.server, conn, current);
							}
							conn.commit();
						}
					}
				} catch (final Throwable e) {
					try {
						conn.rollback();
					} catch (final Throwable t) {
						// ignore
					}
					Report.exception(RunnerChangeUpdate.OWNER, "Exception while updating a storage to s3", e);
				}
			}
		} finally {
			try {
				conn.close();
			} catch (final Throwable t) {
				// ignore
			}
			if (!this.destroyed) {
				Act.later(
						null,
						this,
						highLoad
							? 2500L
							: 15000L);
			}
		}
	}

	@Override
	public void start() {

		this.destroyed = false;
		Act.later(null, this, 15000L);
	}

	@Override
	public void stop() {

		this.destroyed = true;
	}

	@Override
	public String toString() {

		return RunnerChangeUpdate.OWNER + ": " + this.storage;
	}
}
