/*
 * Created on 20.08.2004
 *
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;

import ru.myx.ae1.storage.BaseRecycled;

/** @author myx
 *
 *         Window - Preferences - Java - Code Style - Code Templates */
final class RecycledJdbc implements BaseRecycled {
	
	private final ServerJdbc server;

	private final String guid;

	private final long date;

	private final String title;

	private final String folder;

	private final String owner;

	RecycledJdbc(final ServerJdbc server, final String guid, final long date, final String title, final String folder, final String owner) {
		
		this.server = server;
		this.guid = guid;
		this.date = date;
		this.title = title;
		this.folder = folder;
		this.owner = owner;
	}

	@Override
	public boolean canClean() {
		
		return true;
	}

	@Override
	public boolean canMove() {
		
		return true;
	}

	@Override
	public boolean canRestore() {
		
		return this.server.checkExistance(this.folder);
	}

	@Override
	public void doClean() {
		
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.clearRecycled(this.server, conn, this.guid);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error | RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void doMove(final String parentGuid) {
		
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.restoreRecycled(this.server, conn, this.guid, parentGuid);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error | RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public void doRestore() {
		
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			try {
				conn.setAutoCommit(false);
				MatRecycled.restoreRecycled(this.server, conn, this.guid);
				conn.commit();
			} catch (final Throwable t) {
				try {
					conn.rollback();
				} catch (final Throwable tt) {
					// ignore
				}
				throw t;
			}
		} catch (final Error | RuntimeException e) {
			throw e;
		} catch (final Throwable t) {
			throw new RuntimeException(t);
		}
	}

	@Override
	public long getDate() {
		
		return this.date;
	}

	@Override
	public String getFolder() {
		
		return this.folder;
	}

	@Override
	public String getGuid() {
		
		return this.guid;
	}

	@Override
	public String getOwner() {
		
		return this.owner;
	}

	@Override
	public String getTitle() {
		
		return this.title;
	}
}
