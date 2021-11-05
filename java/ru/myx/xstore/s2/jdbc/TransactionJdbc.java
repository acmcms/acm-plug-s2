package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.report.Report;
import ru.myx.util.WeakFinalizer;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Transaction;

final class TransactionJdbc implements Transaction {
	
	private static final void finalizeStatic(final TransactionJdbc x) {
		
		x.rollback();
	}

	private final ServerJdbc server;

	private final Connection conn;

	private final Object issuer;
	
	private boolean closed = false;

	{
		WeakFinalizer.register(this, TransactionJdbc::finalizeStatic);
	}

	TransactionJdbc(final ServerJdbc server, final Connection conn, final Object issuer) {
		
		this.server = server;
		this.conn = conn;
		this.issuer = issuer;
	}

	@Override
	public final void aliases(final String lnkId, final Set<String> added, final Set<String> removed) throws Throwable {
		
		MatAlias.update(this.server, this.conn, lnkId, added, removed);
	}

	@Override
	public final void commit() {
		
		if (this.closed) {
			return;
		}
		try {
			this.conn.commit();
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				this.closed = true;
				this.conn.close();
			} catch (final Throwable t) {
				// ignore
			}
		}
	}

	@Override
	public final void create(final boolean local,
			final String ctnLnkId,
			final String lnkId,
			final String name,
			final boolean folder,
			final long created,
			final String owner,
			final int state,
			final String title,
			final String typeName,
			final BaseObject added,
			final String vrId,
			final String vrComment,
			final BaseObject vrData) throws Throwable {
		
		Map<String, String> extraExisting = null;
		if (added != null) {
			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		final String objId = Engine.createGuid();
		if (vrId == null || "*".equals(vrId)) {
			MatData.serializeCreate(this.server, this.conn, objId, "*", title, created, typeName, owner, state, extraExisting, added);
		} else {
			this.versionCreate(vrId, "*", vrComment, objId, title, typeName, owner, vrData);
			MatData.serializeCreate(this.server, this.conn, objId, vrId, title, created, typeName, owner, state, extraExisting, added);
		}
		MatLink.serializeCreate(this.server, this.conn, ctnLnkId, lnkId, name, folder, objId);
		MatChange.serialize(
				this.server,
				this.conn,
				0,
				local
					? "create"
					: "create-global",
				lnkId,
				-1,
				0);
	}

	@Override
	public final void delete(final BaseLink linkJdbc, final boolean soft) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		if (soft) {
			MatLink.recycle(this.server, this.conn, link.getGuid());
			MatChange.serialize(this.server, this.conn, 10, "recycle-start", link.getGuid(), link.getKeyLocal(), 0);
			MatChange.serialize(this.server, this.conn, 15, "recycle-all", link.getLinkedIdentity(), link.getKeyLocal(), 0);
		} else {
			MatAlias.delete(this.server, this.conn, null, link.getGuid());
			MatLink.unlink(this.server, this.conn, link.getGuid());
			MatChange.serialize(this.server, this.conn, 10, "clean", link.getLinkedIdentity(), link.getKeyLocal(), 0);
			if (link.internChildrenPossible()) {
				MatChange.serialize(this.server, this.conn, 10, "clean-start", link.getGuid(), link.getKeyLocal(), 0);
			}
			MatChange.serialize(this.server, this.conn, 15, "clean-all", link.getLinkedIdentity(), link.getKeyLocal(), 0);
		}
	}

	final Connection getConnection() {
		
		return this.conn;
	}

	@Override
	public final void link(final boolean local, final String ctnLnkId, final String lnkId, final String name, final boolean folder, final String linkedIdentity) throws Throwable {
		
		MatLink.serializeCreate(this.server, this.conn, ctnLnkId, lnkId, name, folder, linkedIdentity);
		MatChange.serialize(
				this.server,
				this.conn,
				0,
				local
					? "create"
					: "create-global",
				lnkId,
				-1,
				0);
	}

	@Override
	public final void move(final BaseLink linkJdbc, final String cntLnkId, final String key) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.move(this.server, this.conn, link.getParentGuid(), link.getKey(), cntLnkId, key);
		MatChange.serialize(this.server, this.conn, 0, "update", link.getGuid(), link.getKeyLocal(), 0);
	}

	@Override
	public final void record(final String linkedIdentity) throws Throwable {
		
		MatHistory.record(this.server, this.conn, linkedIdentity);
	}

	@Override
	public void rename(final BaseLink linkJdbc, final String key) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.rename(this.server, this.conn, link.getParentGuid(), link.getKey(), key);
		MatChange.serialize(this.server, this.conn, 0, "update", link.getGuid(), link.getKeyLocal(), 0);
	}

	@Override
	public final void resync(final String lnkId) throws Throwable {
		
		MatChange.serialize(this.server, this.conn, 0, "resync", lnkId, -1, 0);
	}

	@Override
	public void revert(final BaseLink linkJdbc, final String historyId, final boolean folder, final long created, final int state, final String title, final String typeName)
			throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.update(this.server, this.conn, link.getGuid(), link.isFolder(), folder);
		MatData.update(
				this.server,
				this.conn,
				historyId,
				link.getLinkedIdentity(),
				"*",
				link.getTitle(),
				title,
				link.getCreated(),
				created,
				link.getTypeName(),
				typeName,
				Context.getUserId(Exec.currentProcess()),
				link.getState(),
				state,
				null,
				null,
				null,
				null);
		MatChange.serialize(this.server, this.conn, 0, "update-all", link.getLinkedIdentity(), link.getKeyLocal(), 0);
	}

	@Override
	public void revert(final BaseLink linkJdbc,
			final String historyId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final BaseObject removed,
			final BaseObject added) throws Throwable {
		
		Map<String, String> extraRemoved = null;
		if (removed != null) {
			extraRemoved = Differer.getExtraRemoved(removed, this.issuer);
		}
		Map<String, String> extraExisting = null;
		if (added != null) {
			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.update(this.server, this.conn, link.getGuid(), link.isFolder(), folder);
		MatData.update(
				this.server,
				this.conn,
				historyId,
				link.getLinkedIdentity(),
				"*",
				link.getTitle(),
				title,
				link.getCreated(),
				created,
				link.getTypeName(),
				typeName,
				Context.getUserId(Exec.currentProcess()),
				link.getState(),
				state,
				extraRemoved,
				extraExisting,
				removed,
				added);
		MatChange.serialize(this.server, this.conn, 0, "update-all", link.getLinkedIdentity(), link.getKeyLocal(), 0);
	}

	@Override
	public final void rollback() {
		
		if (this.closed) {
			return;
		}
		try {
			this.conn.rollback();
		} catch (final SQLException e) {
			throw new RuntimeException(e);
		} finally {
			try {
				this.closed = true;
				this.conn.close();
			} catch (final Throwable t) {
				// ignore
			}
		}
	}

	@Override
	public void segregate(final String guid, final String linkedIdentityOld, final String linkedIdentityNew) throws Throwable {
		
		try (final PreparedStatement ps = this.conn.prepareStatement(
				"INSERT INTO " + this.server.getTnObjects()
						+ " (objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,intDataType,intData) SELECT ?,?,objTitle,objCreated,objDate,objOwner,objType,objState,intDataType,intData FROM "
						+ this.server.getTnObjects() + " WHERE objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, "*");
			ps.setString(3, linkedIdentityOld);
			ps.executeUpdate();
		}
		try (final PreparedStatement ps = this.conn.prepareStatement(
				"INSERT INTO " + this.server.getTnExtraLink() + " (objId,fldId,recId) SELECT ?,fldId,recId FROM " + this.server.getTnExtraLink() + " WHERE objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, linkedIdentityOld);
			ps.executeUpdate();
		}
		try (final PreparedStatement ps = this.conn.prepareStatement("UPDATE " + this.server.getTnTree() + " SET objId=? WHERE lnkId=? AND objId=?")) {
			ps.setString(1, linkedIdentityNew);
			ps.setString(2, guid);
			ps.setString(3, linkedIdentityOld);
			ps.executeUpdate();
		}
	}

	@Override
	public final String toString() {
		
		return "Transaction jdbc{srv=" + this.server + "}";
	}

	@Override
	public final void unlink(final BaseLink linkJdbc, final boolean soft) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		if (soft) {
			MatAlias.stayOrMove(this.server, this.conn, link.getKeyLocal());
			MatLink.recycle(this.server, this.conn, link.getGuid());
			MatChange.serialize(this.server, this.conn, 0, "recycle-start", link.getGuid(), link.getKeyLocal(), 0);
		} else {
			MatAlias.deleteOrMove(this.server, this.conn, link.getKeyLocal());
			MatLink.unlink(this.server, this.conn, link.getGuid());
			MatChange.serialize(this.server, this.conn, 10, "clean", link.getLinkedIdentity(), link.getKeyLocal(), 0);
			if (link.internChildrenPossible()) {
				MatChange.serialize(this.server, this.conn, 15, "clean-start", link.getGuid(), link.getKeyLocal(), 0);
			}
		}
	}

	@Override
	public final void update(final BaseLink linkJdbc, final String linkedIdentity) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatData.update(this.server, this.conn, linkedIdentity);
		MatChange.serialize(this.server, this.conn, 0, "update", link.getGuid(), link.getKeyLocal(), 0);
	}

	@Override
	public final void update(final BaseLink linkJdbc,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership) throws Throwable {
		
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.update(this.server, this.conn, link.getGuid(), link.isFolder(), folder);
		MatData.update(
				this.server,
				this.conn,
				linkedIdentity,
				versionId,
				link.getTitle(),
				title,
				link.getCreated(),
				created,
				link.getTypeName(),
				typeName,
				ownership
					? Context.getUserId(Exec.currentProcess())
					: null,
				link.getState(),
				state);
		Report.info("S2-TRANSACTION", "update short: type=" + typeName);
		MatChange.serialize(this.server, this.conn, 0, "update-all", linkedIdentity, link.getKeyLocal(), 0);
	}

	@Override
	public final void update(final BaseLink linkJdbc,
			final String linkedIdentity,
			final String versionId,
			final boolean folder,
			final long created,
			final int state,
			final String title,
			final String typeName,
			final boolean ownership,
			final BaseObject removed,
			final BaseObject added) throws Throwable {
		
		Map<String, String> extraRemoved = null;
		if (removed != null) {
			extraRemoved = Differer.getExtraRemoved(removed, this.issuer);
		}
		Map<String, String> extraExisting = null;
		if (added != null) {
			extraExisting = Differer.getExtraDiff(extraExisting, added, "", this.issuer);
		}
		final LinkJdbc link = (LinkJdbc) linkJdbc;
		MatLink.update(this.server, this.conn, link.getGuid(), link.isFolder(), folder);
		MatData.update(
				this.server,
				this.conn,
				null,
				linkedIdentity,
				versionId,
				link.getTitle(),
				title,
				link.getCreated(),
				created,
				link.getTypeName(),
				typeName,
				ownership
					? Context.getUserId(Exec.currentProcess())
					: null,
				link.getState(),
				state,
				extraRemoved,
				extraExisting,
				removed,
				added);
		Report.info("S2-TRANSACTION", "update full: type=" + typeName + ", name=" + link.getKey());
		MatChange.serialize(this.server, this.conn, 0, "update-all", linkedIdentity, link.getKeyLocal(), 0);
	}

	@Override
	public final void versionClearAll(final BaseLink link) throws Throwable {
		
		MatVersion.clear(this.server, this.conn, link.getLinkedIdentity());
	}

	@Override
	public final void versionCreate(final String vrId,
			final String vrParentId,
			final String vrComment,
			final String objId,
			final String title,
			final String typeName,
			final String owner,
			final BaseObject vrData) throws Throwable {
		
		final Map<String, String> versionExtra = Differer.getExtraDiff(null, vrData, "", this.issuer);
		MatVersion.serializeCreate(this.server, this.conn, vrId, vrParentId, vrComment, objId, title, owner, typeName, versionExtra, vrData);
	}

	@Override
	public final void
			versionStart(final String vrId, final String vrComment, final String objId, final String title, final String typeName, final String owner, final BaseObject vrData)
					throws Throwable {
		
		this.versionCreate(vrId, "*", vrComment, objId, title, typeName, owner, vrData);
	}
}
