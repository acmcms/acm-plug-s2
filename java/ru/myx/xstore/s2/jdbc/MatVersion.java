package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import ru.myx.ae1.storage.BaseVersion;
import ru.myx.ae3.Engine;
import ru.myx.ae3.base.BaseNativeObject;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.xml.Xml;

final class MatVersion {
	private static final int	TYPE_EMPTY	= 0;
	
	static final void clear(final ServerJdbc server, final Connection conn, final String objId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "DELETE FROM "
				+ server.getTnObjectVersions()
				+ " WHERE objId=?" )) {
			ps.setString( 1, objId );
			ps.execute();
		}
	}
	
	static final BaseVersion[] materialize(final ServerJdbc server, final Connection conn, final String objId)
			throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "SELECT vrId,vrDate,vrParentId,vrComment,vrTitle,vrOwner,vrType FROM "
						+ server.getTnObjectVersions()
						+ " WHERE objId=? ORDER BY vrDate ASC",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, objId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final List<BaseVersion> result = new ArrayList<>();
					do {
						result.add( new VersionJdbc( rs.getString( 1 ), rs.getTimestamp( 2 ).getTime(), rs
								.getString( 3 ), rs.getString( 4 ), rs.getString( 5 ), rs.getString( 6 ), rs
								.getString( 7 ) ) );
					} while (rs.next());
					return result.toArray( new BaseVersion[result.size()] );
				}
				return null;
			}
		}
	}
	
	static final DataJdbc materializeSnapshot(
			final ServerJdbc server,
			final Connection conn,
			final String objId,
			final String versionId) throws Exception {
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT o.intDataType,o.intData,v.vrForm FROM "
				+ server.getTnObjectVersions()
				+ " v, "
				+ server.getTnObjects()
				+ " o WHERE v.objId=o.objId AND v.vrId=? AND o.objId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, versionId );
			ps.setString( 2, objId );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int intDataType = rs.getInt( 1 );
					final BaseObject data;
					if (intDataType != MatVersion.TYPE_EMPTY) {
						data = new BaseNativeObject();
					} else {
						final TransferCopier intData = Transfer.createBuffer( rs.getBinaryStream( 2 ) ).toBinary();
						data = MatData.dataMaterialize( server, intDataType, intData, null );
					}
					Xml.toMap( "meterializeVersionSnapshot",
							Transfer.createCopier( rs.getBinaryStream( 3 ) ),
							Engine.CHARSET_UTF8,
							null,
							data,
							server.getStorageExternalizer(),
							null );
					return new DataJdbc( server, objId, data );
				}
				return null;
			}
		}
	}
	
	static final void serializeCreate(
			final ServerJdbc server,
			final Connection conn,
			final String vrId,
			final String vrParentId,
			final String vrComment,
			final String objId,
			final String vrTitle,
			final String vrOwner,
			final String vrType,
			final Map<String, String> vrExtra,
			final BaseObject vrData) throws Exception {
		try (final PreparedStatement ps = conn
				.prepareStatement( "INSERT INTO "
						+ server.getTnObjectVersions()
						+ "(vrId,vrDate,vrParentId,vrComment,objId,vrTitle,vrOwner,vrType,vrFormType,vrForm) VALUES (?,?,?,?,?,?,?,?,1,?)" )) {
			ps.setString( 1, vrId );
			ps.setTimestamp( 2, new Timestamp( Engine.fastTime() ) );
			ps.setString( 3, vrParentId );
			ps.setString( 4, vrComment );
			ps.setString( 5, objId );
			ps.setString( 6, vrTitle );
			ps.setString( 7, vrOwner );
			ps.setString( 8, vrType );
			ps.setBytes( 9,
					Xml.toXmlString( "data",
							vrData,
							true,
							server.getStorageExternalizer(),
							new StoreInfo( conn, objId ),
							512 ).getBytes( Engine.CHARSET_UTF8 ) );
			ps.execute();
		}
		if (vrExtra != null && !vrExtra.isEmpty()) {
			for (final Map.Entry<String, String> record : vrExtra.entrySet()) {
				final String fldId = record.getKey();
				final String recId = record.getValue();
				try {
					try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
							+ server.getTnExtraLink()
							+ "(recId,objId,fldId) VALUES (?,?,?)" )) {
						ps.setString( 1, recId );
						ps.setString( 2, vrId );
						ps.setString( 3, Text.limitString( fldId, 32 ) );
						ps.execute();
					}
				} catch (final Throwable t) {
					Report.exception( "S2/JDBC/MAT_DATA", "Exception while linking extra record to a version", t );
				}
			}
		}
	}
	
	static final void update3(final ServerJdbc server, final Connection conn, final String guid) throws Exception {
		final BaseObject map;
		try (final PreparedStatement ps = conn.prepareStatement( "SELECT vrFormType,vrForm FROM "
				+ server.getTnObjectVersions()
				+ " WHERE vrId=?",
				ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY )) {
			ps.setString( 1, guid );
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int intDataType = rs.getInt( 1 );
					if (intDataType == MatVersion.TYPE_EMPTY) {
						map = BaseObject.UNDEFINED;
					} else {
						map = Xml.toBase( "versionUpdate3",
								Transfer.wrapCopier( rs.getBytes( 2 ) ),
								Engine.CHARSET_UTF8,
								null,
								server.getStorageExternalizer(),
								null );
					}
				} else {
					return;
				}
			}
		}
		try (final PreparedStatement ps = conn.prepareStatement( "UPDATE "
				+ server.getTnObjectVersions()
				+ " SET vrFormType=?,vrForm=?,extLink=? WHERE vrId=?" )) {
			MatData.dataSerialize( server, conn, guid, ps, 1, map );
			ps.setString( 4, guid );
			ps.execute();
		}
	}
}
