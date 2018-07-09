/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

import ru.myx.ae3.Engine;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class MatChange {
	static final void serialize(
			final ServerJdbc server,
			final Connection conn,
			final int sequence,
			final String type,
			final String guid,
			final int luid,
			final long date) throws SQLException {
		try (final PreparedStatement ps = conn
				.prepareStatement( "INSERT INTO "
						+ server.getTnChangeQueue()
						+ "(evtId,evtDate,evtSequence,evtOwner,evtCmdType,evtCmdGuid,evtCmdLuid,evtCmdDate) VALUES (?,?,?,?,?,?,?,?)" )) {
			ps.setString( 1, Engine.createGuid() );
			ps.setTimestamp( 2, new Timestamp( System.currentTimeMillis() ) ); // exact
			// preciese
			// time!
			ps.setInt( 3, sequence );
			ps.setString( 4, server.getIdentity() );
			ps.setString( 5, type );
			ps.setString( 6, guid );
			ps.setInt( 7, luid );
			ps.setTimestamp( 8, new Timestamp( date ) );
			ps.execute();
		}
	}
}
