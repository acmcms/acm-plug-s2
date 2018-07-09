/*
 * Created on 01.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferBuffer;
import ru.myx.ae3.binary.TransferCollector;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExtraBinary;
import ru.myx.ae3.extra.ExtraBytes;
import ru.myx.ae3.extra.ExtraSerialized;
import ru.myx.ae3.extra.ExtraTextBase;
import ru.myx.ae3.extra.ExtraXml;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;

/**
 * @author myx
 * 
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments
 */
final class MatExtra {
	static final boolean contains(final ServerJdbc server, final Connection conn, final String recId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT count(*) FROM "
				+ server.getTnExtra()
				+ " WHERE recId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, recId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					return rs.getInt( 1 ) > 0;
				}
				return false;
			}
		}
	}
	
	static final External materialize(
			final ServerJdbc server,
			final Object issuer,
			final Connection conn,
			final String recId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT recDate,recType,recBlob FROM "
				+ server.getTnExtra()
				+ " WHERE recId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, recId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final long recDate = rs.getTimestamp( 1 ).getTime();
					final String recType = rs.getString( 2 );
					final TransferCollector binary = Transfer.createCollector();
					final InputStream recBlob = rs.getBinaryStream( 3 );
					Transfer.toStream( recBlob, binary.getOutputStream(), true );
					if ("ae2/bytes".equals( recType )) {
						return new ExtraBytes( issuer, recId, recDate, binary.toCloneFactory() );
					}
					if ("ae2/binary".equals( recType )) {
						return new ExtraBinary( issuer, recId, recDate, binary.toCloneFactory() );
					}
					if ("text/plain".equals( recType )) {
						return new ExtraTextBase( issuer, recId, recDate, binary.toCloneFactory() );
					}
					if ("text/xml".equals( recType )) {
						return new ExtraXml( issuer,
								recId,
								recDate,
								binary.toCloneFactory(),
								server.getStorageExternalizer(),
								null );
					}
					return new ExtraSerialized( issuer, recId, recDate, recType, binary.toCloneFactory() );
				}
				return null;
			}
		}
	}
	
	static final void serialize(
			final ServerJdbc server,
			final Connection conn,
			final String recId,
			final String objId,
			final String fldId,
			final long recDate,
			final String type,
			final TransferCopier copier) throws Exception {
		{
			final String recType = type;
			final TransferBuffer recBuffer = copier.nextCopy();
			final long recBufferLength = recBuffer.remaining();
			if (recBufferLength > Integer.MAX_VALUE) {
				throw new RuntimeException( "Bigger than maximum byte array size, size=" + recBufferLength + "!" );
			}
			try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
					+ server.getTnExtra()
					+ "(recId,recDate,recType,recBlob) VALUES (?,?,?,?) " )) {
				ps.setString( 1, recId );
				ps.setTimestamp( 2, new Timestamp( recDate ) );
				ps.setString( 3, recType );
				ps.setBinaryStream( 4, recBuffer.toInputStream(), (int) recBufferLength );
				ps.execute();
			}
		}
		try {
			final String fldIdPrepared = Text.limitString( fldId, 32 );
			try {
				try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
						+ server.getTnExtraLink()
						+ "(recId,objId,fldId) VALUES (?,?,?) " )) {
					ps.setString( 1, recId );
					ps.setString( 2, objId );
					ps.setString( 3, fldIdPrepared );
					ps.execute();
				}
			} catch (final SQLException e) {
				try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
						+ server.getTnExtraLink()
						+ " SET recId=? WHERE objId=? AND fldId=?" )) {
					ps.setString( 1, recId );
					ps.setString( 2, objId );
					ps.setString( 3, fldIdPrepared );
					if (ps.executeUpdate() != 1) {
						throw new RuntimeException( "No insert and no update done!" );
					}
				}
			}
		} catch (final Throwable t) {
			Report.exception( "S2/JDBC/MAT_EXTRA", "Exception while linking extra record to an object, recId="
					+ recId
					+ ", objId="
					+ objId
					+ ", fldId="
					+ fldId, t );
		}
	}
	
	static final void unlink(final ServerJdbc server, final Connection conn, final String recId, final String objId)
			throws Exception {
		if (recId == null && objId == null) {
			return;
		}
		try {
			final StringBuilder query = new StringBuilder().append( "DELETE FROM " ).append( server.getTnExtraLink() )
					.append( " WHERE " );
			if (recId != null && objId != null) {
				query.append( "recId=? AND objId=?" );
			} else if (recId == null) {
				query.append( "objId=?" );
			} else {
				query.append( "recId=?" );
			}
			try (final PreparedStatement ps = conn.prepareStatement( query.toString() )) {
				if (recId != null && objId != null) {
					ps.setString( 1, recId );
					ps.setString( 2, objId );
				} else if (recId == null) {
					ps.setString( 1, objId );
				} else {
					ps.setString( 1, recId );
				}
				ps.execute();
			}
		} catch (final Throwable t) {
			Report.exception( "S2/JDBC/MAT_EXTRA", "Exception while unlinking extra record to an object", t );
		}
	}
	
	private MatExtra() {
		// emptry
	}
}
