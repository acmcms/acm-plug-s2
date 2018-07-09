/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import ru.myx.ae3.Engine;
import ru.myx.ae3.act.Context;
import ru.myx.ae3.exec.Exec;
import ru.myx.ae3.help.Create;
import ru.myx.ae3.help.Text;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class MatLink {
	static final LinkJdbc materialize(final ServerJdbc server, final Connection conn, final String guid)
			throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,o.vrId,o.objTitle,o.objCreated,o.objDate,o.objOwner,o.objType,o.objState"
						+ (server.isUpdate3()
								? ",o.extLink"
								: "")
						+ " FROM "
						+ server.getTnTree()
						+ " t, "
						+ server.getTnObjects()
						+ " o WHERE t.objId=o.objId AND t.lnkId=?",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt( 1 );
					final String lnkCntId = rs.getString( 2 );
					final String lnkName = rs.getString( 3 );
					final boolean lnkFolder = "Y".equals( rs.getString( 4 ) );
					final String objId = rs.getString( 5 );
					final String vrId = rs.getString( 6 );
					final String objTitle = rs.getString( 7 );
					final long objCreated = rs.getTimestamp( 8 ).getTime();
					final long objDate = rs.getTimestamp( 9 ).getTime();
					final String objOwner = rs.getString( 10 );
					final String objType = rs.getString( 11 );
					final int objState = rs.getInt( 12 );
					final String extLink = server.isUpdate3()
							? rs.getString( 13 )
							: "-";
					return new LinkJdbc( server,
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							extLink );
				}
				return null;
			}
		}
	}
	
	static final LinkJdbc materializeHistory(
			final ServerJdbc server,
			final Connection conn,
			final String guid,
			final String historyId) throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,o.vrId,o.objTitle,o.objCreated,o.objDate,o.objOwner,o.objType,o.objState"
						+ (server.isUpdate3()
								? ",o.extLink"
								: "")
						+ " FROM "
						+ server.getTnTree()
						+ " t, "
						+ server.getTnObjectHistory()
						+ " o WHERE t.objId=o.objId AND t.lnkId=? AND o.hsId=?",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			ps.setString( 2, historyId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt( 1 );
					final String lnkCntId = rs.getString( 2 );
					final String lnkName = rs.getString( 3 );
					final boolean lnkFolder = "Y".equals( rs.getString( 4 ) );
					final String objId = rs.getString( 5 );
					final String vrId = rs.getString( 6 );
					final String objTitle = rs.getString( 7 );
					final long objCreated = rs.getTimestamp( 8 ).getTime();
					final long objDate = rs.getTimestamp( 9 ).getTime();
					final String objOwner = rs.getString( 10 );
					final String objType = rs.getString( 11 );
					final int objState = rs.getInt( 12 );
					final String extLink = server.isUpdate3()
							? rs.getString( 13 )
							: "-";
					return new LinkJdbc( server,
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							extLink );
				}
				return null;
			}
		}
	}
	
	static final LinkJdbc materializeVersion(
			final ServerJdbc server,
			final Connection conn,
			final String guid,
			final String versionId) throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT t.lnkLuid,t.cntLnkId,t.lnkName,t.lnkFolder,t.objId,v.vrId,v.objTitle,v.objCreated,v.objDate,v.objOwner,v.objType,o.objState"
						+ (server.isUpdate3()
								? ",o.extLink"
								: "")
						+ " FROM "
						+ server.getTnTree()
						+ " t, "
						+ server.getTnObjectVersions()
						+ " v, "
						+ server.getTnObjects()
						+ " o WHERE t.objId=o.objId AND o.objId=v.objId AND t.lnkId=? AND v.vrId=?",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			ps.setString( 2, versionId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int lnkLuid = rs.getInt( 1 );
					final String lnkCntId = rs.getString( 2 );
					final String lnkName = rs.getString( 3 );
					final boolean lnkFolder = "Y".equals( rs.getString( 4 ) );
					final String objId = rs.getString( 5 );
					final String vrId = rs.getString( 6 );
					final String objTitle = rs.getString( 7 );
					final long objCreated = rs.getTimestamp( 8 ).getTime();
					final long objDate = rs.getTimestamp( 9 ).getTime();
					final String objOwner = rs.getString( 10 );
					final String objType = rs.getString( 11 );
					final int objState = rs.getInt( 12 );
					final String extLink = server.isUpdate3()
							? rs.getString( 13 )
							: "-";
					return new LinkJdbc( server,
							guid,
							lnkLuid,
							lnkCntId,
							lnkName,
							lnkFolder,
							objId,
							vrId,
							objTitle,
							objCreated,
							objDate,
							objOwner,
							objType,
							objState,
							extLink );
				}
				return null;
			}
		}
	}
	
	static void move(
			final ServerJdbc server,
			final Connection conn,
			final String initialCntLnkId,
			final String initialKey,
			final String cntLnkId,
			final String key) throws Exception {
		if (cntLnkId.equals( initialCntLnkId )) {
			try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
					+ server.getTnTree()
					+ " SET lnkName=? WHERE cntLnkId=? AND lnkName=?" )) {
				ps.setString( 1, key );
				ps.setString( 2, initialCntLnkId );
				ps.setString( 3, initialKey );
				ps.execute();
			}
		} else {
			try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
					+ server.getTnTree()
					+ " SET cntLnkId=?, lnkName=? WHERE cntLnkId=? AND lnkName=? AND lnkId!=?" )) {
				ps.setString( 1, cntLnkId );
				ps.setString( 2, key );
				ps.setString( 3, initialCntLnkId );
				ps.setString( 4, initialKey );
				ps.setString( 5, "$$ROOT_ENTRY" );
				ps.execute();
			}
		}
	}
	
	static final void recycle(final ServerJdbc server, final Connection conn, final String lnkId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
				+ server.getTnRecycled()
				+ "(delRootId,delDate,delObjId,delCntId,delOwner) SELECT lnkId,?,objId,cntLnkId,? FROM "
				+ server.getTnTree()
				+ " WHERE lnkId=?" )) {
			ps.setTimestamp( 1, new Timestamp( Engine.fastTime() ) );
			ps.setString( 2, Context.getUserId( Exec.currentProcess() ) );
			ps.setString( 3, lnkId );
			ps.execute();
		}
		try (final PreparedStatement ps = conn
				.prepareStatement( "INSERT INTO "
						+ server.getTnRecycledTree()
						+ "(lnkId,delId,cntLnkId,lnkName,lnkFolder,objId,lnkSort) SELECT lnkId,lnkId,cntLnkId,lnkName,lnkFolder,objId,lnkSort FROM "
						+ server.getTnTree()
						+ " WHERE lnkId=?" )) {
			ps.setString( 1, lnkId );
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
				+ server.getTnTree()
				+ " SET cntLnkId='*' WHERE lnkId=?" )) {
			ps.setString( 1, lnkId );
			ps.execute();
		}
	}
	
	static void rename(
			final ServerJdbc server,
			final Connection conn,
			final String cntLnkId,
			final String initialKey,
			final String key) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
				+ server.getTnTree()
				+ " SET lnkName=? WHERE cntLnkId=? AND lnkName=?" )) {
			ps.setString( 1, key );
			ps.setString( 2, cntLnkId );
			ps.setString( 3, initialKey );
			ps.execute();
		}
	}
	
	static final String[] searchIdentity(
			final ServerJdbc server,
			final Connection conn,
			final String guid,
			final boolean all) throws Exception {
		Set<String> result = null;
		if (conn.getMetaData().supportsUnion() || conn.getMetaData().supportsUnionAll()) {
			final String unionString = conn.getMetaData().supportsUnion()
					? " UNION "
					: " UNION ALL ";
			final String query = new StringBuilder().append( "SELECT t.lnkId FROM " ).append( server.getTnTree() )
					.append( " t , " ).append( server.getTnObjects() ).append( all
							? " o WHERE o.objId=? AND t.objId=o.objId"
							: " o WHERE o.objId=? AND t.objId=o.objId AND o.objState>0" ).append( unionString )
					.append( "SELECT t.lnkId FROM " ).append( server.getTnTree() ).append( " t , " )
					.append( server.getTnAliases() ).append( " a WHERE a.alId=? AND t.lnkId=a.alLnkId" )
					.append( unionString ).append( "SELECT t.lnkId FROM " ).append( server.getTnTree() )
					.append( " t WHERE t.lnkId=?" ).toString();
			try (final PreparedStatement ps = conn.prepareStatement( query,
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY )) {
				ps.setString( 1, guid );
				ps.setString( 2, guid );
				ps.setString( 3, guid );
				try (final ResultSet rs = ps.executeQuery()) {
					while (rs.next()) {
						if (result == null) {
							result = Create.tempSet();
						}
						result.add( rs.getString( 1 ) );
					}
				}
			}
		} else {
			{
				final String query = new StringBuilder().append( "SELECT t.lnkId FROM " ).append( server.getTnTree() )
						.append( " t , " ).append( server.getTnObjects() ).append( all
								? " o WHERE o.objId=? AND t.objId=o.objId"
								: " o WHERE o.objId=? AND t.objId=o.objId AND o.objState>1" ).toString();
				try (final PreparedStatement ps = conn.prepareStatement( query,
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
					ps.setString( 1, guid );
					ps.setString( 2, guid );
					ps.setString( 3, guid );
					try (final ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							if (result == null) {
								result = Create.tempSet();
							}
							result.add( rs.getString( 1 ) );
						}
					}
				}
			}
			{
				final String query = new StringBuilder().append( "SELECT t.lnkId FROM " ).append( server.getTnTree() )
						.append( " t , " ).append( server.getTnAliases() )
						.append( " a WHERE a.alId=? AND t.lnkId=a.alLnkId" ).toString();
				try (final PreparedStatement ps = conn.prepareStatement( query,
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
					ps.setString( 1, guid );
					try (final ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							if (result == null) {
								result = Create.tempSet();
							}
							result.add( rs.getString( 1 ) );
						}
					}
				}
			}
			{
				final String query = new StringBuilder().append( "SELECT t.lnkId FROM " ).append( server.getTnTree() )
						.append( " t WHERE t.lnkId=?" ).toString();
				try (final PreparedStatement ps = conn.prepareStatement( query,
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
					ps.setString( 1, guid );
					try (final ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							if (result == null) {
								result = Create.tempSet();
							}
							result.add( rs.getString( 1 ) );
						}
					}
				}
			}
		}
		return result == null
				? null
				: result.toArray( new String[result.size()] );
	}
	
	static final void serializeCreate(
			final ServerJdbc server,
			final Connection conn,
			final String ctnLnkId,
			final String lnkId,
			final String name,
			final boolean folder,
			final String objId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
				+ server.getTnTree()
				+ "(lnkId,cntLnkId,lnkName,lnkFolder,objId,lnkSort) VALUES (?,?,?,?,?,0)" )) {
			ps.setString( 1, lnkId );
			ps.setString( 2, ctnLnkId );
			ps.setString( 3, name );
			ps.setString( 4, folder
					? "Y"
					: "N" );
			ps.setString( 5, objId );
			ps.execute();
		}
	}
	
	static final void unlink(final ServerJdbc server, final Connection conn, final int lnkLuid) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
				+ server.getTnTree()
				+ " WHERE lnkLuid=?" )) {
			ps.setInt( 1, lnkLuid );
			ps.execute();
		}
	}
	
	static final void unlink(final ServerJdbc server, final Connection conn, final String lnkId) throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "DELETE FROM " + server.getTnTree() + " WHERE lnkId=?" )) {
			ps.setString( 1, lnkId );
			ps.execute();
		}
	}
	
	static final void update(
			final ServerJdbc server,
			final Connection conn,
			final String lnkId,
			final boolean initialFolder,
			final boolean folder) throws Exception {
		final List<String> setPart = new ArrayList<>();
		if (initialFolder != folder) {
			setPart.add( "lnkFolder=?" );
		}
		if (!setPart.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
					+ server.getTnTree()
					+ " SET "
					+ Text.join( setPart, ", " )
					+ " WHERE lnkId=?" )) {
				int index = 1;
				if (initialFolder != folder) {
					ps.setString( index++, folder
							? "Y"
							: "N" );
				}
				ps.setString( index, lnkId );
				ps.execute();
			}
		}
	}
}
