/*
 * Created on 22.08.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ru.myx.ae3.help.Create;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class MatAlias {
	static final void delete(final ServerJdbc server, final Connection conn, final String alId, final String alLnkId)
			throws Exception {
		if (alId == null) {
			try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
					+ server.getTnAliases()
					+ " WHERE alLnkId=?" )) {
				ps.setString( 1, alLnkId );
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
					+ server.getTnAliases()
					+ " WHERE alId=? AND alLnkId=?" )) {
				ps.setString( 1, alId );
				ps.setString( 2, alLnkId );
				ps.execute();
			}
		}
	}
	
	static final void deleteOrMove(final ServerJdbc server, final Connection conn, final int lnkLuid) throws Throwable {
		Set<String> aliases = null;
		String source = null;
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT a.alId,t.lnkId FROM "
						+ server.getTnAliases()
						+ " a, "
						+ server.getTnTree()
						+ " t WHERE t.lnkLuid=? AND a.alLnkId=t.lnkId",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setInt( 1, lnkLuid );
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (aliases == null) {
						aliases = Create.tempSet();
					}
					aliases.add( rs.getString( 1 ) );
					source = rs.getString( 2 );
				}
				if (aliases == null) {
					return;
				}
			}
		}
		final String target;
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT t2.lnkId "
				+ "FROM "
				+ server.getTnTree()
				+ " t, "
				+ server.getTnObjects()
				+ " o, "
				+ server.getTnTree()
				+ " t2 "
				+ "WHERE o.objId=t.objId AND o.objId=t2.objId AND t.lnkLuid=? AND t2.lnkLuid!=t.lnkLuid "
				+ "ORDER BY o.objState DESC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY )) {
			ps.setMaxRows( 1 );
			ps.setInt( 1, lnkLuid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					target = rs.getString( 1 );
				} else {
					target = null;
				}
			}
		}
		if (target == null) {
			try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
					+ server.getTnAliases()
					+ " WHERE alLnkId=?" )) {
				ps.setString( 1, source );
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
					+ server.getTnAliases()
					+ " SET alLnkId=? WHERE alLnkId=?" )) {
				ps.setString( 1, target );
				ps.setString( 2, source );
				ps.execute();
			}
		}
	}
	
	static final String[] enlist(final ServerJdbc server, final Connection conn, final String lnkId) throws Throwable {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT alId FROM "
				+ server.getTnAliases()
				+ " WHERE alLnkId=? ORDER BY alId ASC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, lnkId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<String> result = new ArrayList<>();
					do {
						result.add( rs.getString( 1 ) );
					} while (rs.next());
					return result.toArray( new String[result.size()] );
				}
				return null;
			}
		}
	}
	
	static final String search(final ServerJdbc server, final Connection conn, final String alias, final boolean all)
			throws Throwable {
		try (final PreparedStatement ps = all
				? conn.prepareStatement( "SELECT a.alLnkId FROM "
						+ server.getTnAliases()
						+ " a, "
						+ server.getTnTree()
						+ " t WHERE a.alId=? AND a.alLnkId=t.lnkId",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )
				: conn.prepareStatement( "SELECT a.alLnkId FROM "
						+ server.getTnAliases()
						+ " a, "
						+ server.getTnTree()
						+ " t, "
						+ server.getTnObjects()
						+ " o WHERE alId=? AND a.alLnkId=t.lnkId AND o.objId=t.objId AND o.objState>0",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, alias );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getString( 1 );
				}
				return null;
			}
		}
	}
	
	static final void stayOrMove(final ServerJdbc server, final Connection conn, final int lnkLuid) throws Throwable {
		Set<String> aliases = null;
		String source = null;
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT a.alId,t.lnkId FROM "
						+ server.getTnAliases()
						+ " a, "
						+ server.getTnTree()
						+ " t WHERE t.lnkLuid=? AND a.alLnkId=t.lnkId",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setInt( 1, lnkLuid );
			try (final ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					if (aliases == null) {
						aliases = Create.tempSet();
					}
					aliases.add( rs.getString( 1 ) );
					source = rs.getString( 2 );
				}
				if (aliases == null) {
					return;
				}
			}
		}
		final String target;
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT t2.lnkId "
				+ "FROM "
				+ server.getTnTree()
				+ " t, "
				+ server.getTnObjects()
				+ " o, "
				+ server.getTnTree()
				+ " t2 "
				+ "WHERE o.objId=t.objId AND o.objId=t2.objId AND t.lnkLuid=? AND t2.lnkLuid!=t.lnkLuid "
				+ "ORDER BY o.objState DESC", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY )) {
			ps.setMaxRows( 1 );
			ps.setInt( 1, lnkLuid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return;
				}
				target = rs.getString( 1 );
			}
		}
		try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
				+ server.getTnAliases()
				+ " SET alLnkId=? WHERE alLnkId=?" )) {
			ps.setString( 1, target );
			ps.setString( 2, source );
			ps.execute();
		}
	}
	
	static final void update(
			final ServerJdbc server,
			final Connection conn,
			final String lnkId,
			final Set<String> added,
			final Set<String> removed) throws Throwable {
		if (removed != null && !removed.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
					+ server.getTnAliases()
					+ " WHERE alId=?" )) {
				for (final String alias : removed) {
					ps.setString( 1, alias );
					ps.addBatch();
				}
				ps.executeBatch();
			}
		}
		if (added != null && !added.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
					+ server.getTnAliases()
					+ " WHERE alId=?" )) {
				for (final String alias : added) {
					ps.setString( 1, alias );
					ps.addBatch();
				}
				ps.executeBatch();
			}
			try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
					+ server.getTnAliases()
					+ "(alId,alLnkId) VALUES (?,?)" )) {
				for (final String alias : added) {
					ps.setString( 1, alias );
					ps.setString( 2, lnkId );
					ps.addBatch();
				}
				ps.executeBatch();
			}
		}
	}
}
