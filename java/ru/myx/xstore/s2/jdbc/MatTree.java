/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import ru.myx.ae1.storage.ModuleInterface;
import ru.myx.ae3.help.Create;

/**
 * @author myx
 * 
 */
final class MatTree {
	private static final int	STATE_SYSTEM	= ModuleInterface.STATE_SYSTEM;
	
	private static final int	STATE_ARCHIVE	= ModuleInterface.STATE_ARCHIVE;
	
	private static final int	STATE_PUBLISH	= ModuleInterface.STATE_PUBLISH;
	
	static final List<Object> children(final ServerJdbc server, final Connection conn, final int luid) throws Throwable {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT t1.lnkLuid,t1.objId FROM "
				+ server.getTnTree()
				+ " t1, "
				+ server.getTnTree()
				+ " t2 WHERE t1.cntLnkId=t2.lnkId AND t2.lnkLuid=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setInt( 1, luid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add( new Integer( rs.getInt( 1 ) ) );
						result.add( rs.getString( 2 ) );
					} while (rs.next());
					return result;
				}
				return Collections.emptyList();
			}
		}
	}
	
	static final List<Object> children(final ServerJdbc server, final Connection conn, final String lnkId)
			throws Throwable {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT lnkLuid,objId FROM "
				+ server.getTnTree()
				+ " WHERE cntLnkId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, lnkId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<Object> result = new ArrayList<>();
					do {
						result.add( new Integer( rs.getInt( 1 ) ) );
						result.add( rs.getString( 2 ) );
					} while (rs.next());
					return result;
				}
				return Collections.emptyList();
			}
		}
	}
	
	private static final String getLetter(final String letter) {
		for (int i = 0; i < letter.length(); ++i) {
			final char c = letter.charAt( i );
			if (Character.isLetterOrDigit( c )) {
				return String.valueOf( c );
			}
		}
		return null;
	}
	
	static final TreeJdbc materialize(final ServerJdbc server, final Connection conn, final String guid, final int luid)
			throws Exception {
		final StringBuilder sql = new StringBuilder( 128 )
				.append( "SELECT t.lnkId,o.objTitle,o.objState,t.lnkFolder,t.lnkName,o.objCreated FROM " )
				.append( server.getTnTree() ).append( " t, " ).append( server.getTnObjects() )
				.append( " o WHERE t.cntLnkId=? AND t.objId=o.objId ORDER BY t.lnkSort ASC, o.objTitle ASC" );
		try (final PreparedStatement ps = conn.prepareStatement( sql.toString(),
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final Map<String, String> names = Create.tempMap();
					final List<TreeJdbcEntry> entries = new ArrayList<>();
					do {
						final String lnkId = rs.getString( 1 );
						final String letter = MatTree.getLetter( rs.getString( 2 ) );
						final int state = rs.getInt( 3 );
						final boolean listable = state == MatTree.STATE_SYSTEM || state == MatTree.STATE_PUBLISH;
						final boolean searchable = state == MatTree.STATE_ARCHIVE || state == MatTree.STATE_PUBLISH;
						final boolean lnkFolder = "Y".equals( rs.getString( 4 ) );
						final String name = rs.getString( 5 );
						final long objCreated = rs.getTimestamp( 6 ).getTime();
						entries.add( new TreeJdbcEntry( lnkId, letter, listable, searchable, lnkFolder, objCreated ) );
						names.put( name, lnkId );
					} while (rs.next());
					return new TreeJdbc( server, guid, luid, names, entries.toArray( new TreeJdbcEntry[entries.size()] ) );
				}
				return TreeJdbc.EMPTY;
			}
		}
	}
}
