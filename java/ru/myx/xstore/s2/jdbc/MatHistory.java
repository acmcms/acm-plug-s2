/*
 * Created on 20.08.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.BaseHistory;
import ru.myx.ae3.Engine;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.xml.Xml;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class MatHistory {
	private static final int	TYPE_EMPTY	= 0;
	
	static final void clear(final ServerJdbc server, final Connection conn, final String objId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
				+ server.getTnObjectHistory()
				+ " WHERE objId=?" )) {
			ps.setString( 1, objId );
			ps.execute();
		}
	}
	
	static final BaseHistory[] materialize(final ServerJdbc server, final Connection conn, final String objId)
			throws Throwable {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT hsId,hsDate,objTitle FROM "
				+ server.getTnObjectHistory()
				+ " WHERE objId=? ORDER BY hsDate DESC",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, objId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<BaseHistory> result = new ArrayList<>();
					do {
						result.add( new HistoryJdbc( rs.getString( 1 ), rs.getTimestamp( 2 ).getTime(), rs
								.getString( 3 ) ) );
					} while (rs.next());
					return result.toArray( new BaseHistory[result.size()] );
				}
				return null;
			}
		}
	}
	
	static final DataJdbc materializeSnapshot(
			final ServerJdbc server,
			final Connection conn,
			final String objId,
			final String historyId) throws Throwable {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT intDataType,intData FROM "
				+ server.getTnObjectHistory()
				+ " WHERE hsId=? AND objId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, historyId );
			ps.setString( 2, objId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int intDataType = rs.getInt( 1 );
					if (intDataType == MatHistory.TYPE_EMPTY) {
						return new DataJdbc( server, objId, BaseObject.UNDEFINED );
					}
					final TransferCopier intData = Transfer.createBuffer( rs.getBinaryStream( 2 ) ).toBinary();
					return new DataJdbc( server, objId, intDataType, intData );
				}
				return null;
			}
		}
	}
	
	static void record(final ServerJdbc server, final Connection conn, final String objId) throws Throwable {
		final String historyId = Engine.createGuid();
		try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
				+ server.getTnObjectHistory()
				+ "(hsId,hsDate,objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,intDataType,intData"
				+ (server.isUpdate3()
						? ",extLink"
						: "")
				+ ") SELECT ?,?,objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,intDataType,intData"
				+ (server.isUpdate3()
						? ",extLink"
						: "")
				+ " FROM "
				+ server.getTnObjects()
				+ " WHERE objId=?" )) {
			ps.setString( 1, historyId );
			ps.setTimestamp( 2, new Timestamp( Engine.fastTime() ) );
			ps.setString( 3, objId );
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
				+ server.getTnExtraLink()
				+ "(objId,fldId,recId) SELECT ?,fldId,recId FROM "
				+ server.getTnExtraLink()
				+ " WHERE objId=?" )) {
			ps.setString( 1, historyId );
			ps.setString( 2, objId );
			ps.execute();
		}
	}
	
	static final void update3(final ServerJdbc server, final Connection conn, final String guid) throws Throwable {
		final BaseObject map;
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT intDataType,intData FROM "
				+ server.getTnObjectHistory()
				+ " WHERE hsId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (!rs.next()) {
					return;
				}
				final int intDataType = rs.getInt( 1 );
				if (intDataType == MatHistory.TYPE_EMPTY) {
					map = BaseObject.UNDEFINED;
				} else {
					map = Xml.toBase( "historyUpdate3",
							Transfer.wrapCopier( rs.getBytes( 2 ) ),
							StandardCharsets.UTF_8,
							null,
							server.getStorageExternalizer(),
							null );
				}
			}
		}
		try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
				+ server.getTnObjectHistory()
				+ " SET intDataType=?,intData=?,extLink=? WHERE hsId=?" )) {
			MatData.dataSerialize( server, conn, guid, ps, 1, map );
			ps.setString( 4, guid );
			ps.execute();
		}
	}
}
