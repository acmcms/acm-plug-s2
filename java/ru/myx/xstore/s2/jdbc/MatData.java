/*
 * Created on 12.07.2004
 */
package ru.myx.xstore.s2.jdbc;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import ru.myx.ae3.Engine;
import ru.myx.ae3.base.BaseObject;
import ru.myx.ae3.binary.Transfer;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.help.Text;
import ru.myx.ae3.report.Report;
import ru.myx.ae3.xml.Xml;

/** @author myx
 *
 *         To change the template for this generated type comment go to
 *         Window&gt;Preferences&gt;Java&gt;Code Generation&gt;Code and Comments */
final class MatData {

	private static final int TYPE_EMPTY = 0;

	private static final int TYPE_SIMPLE_UNKNOWN = 1;

	private static final int TYPE_MULTI_UNKNOWN = 2;

	private static final int TYPE_SIMPLE_SMALL = 3;

	private static final int TYPE_MULTI_SMALL = 4;

	static final BaseObject dataMaterialize(final ServerJdbc server, final int type, final TransferCopier data, final Object attachment) throws Exception {

		switch (type) {
			case TYPE_EMPTY :
				return BaseObject.UNDEFINED;
			case TYPE_SIMPLE_UNKNOWN :
			case TYPE_MULTI_UNKNOWN :
			case TYPE_SIMPLE_SMALL :
			case TYPE_MULTI_SMALL :
				return Xml.toBase("dataMaterialize", data, StandardCharsets.UTF_8, null, server.getStorageExternalizer(), attachment);
			default :
				throw new RuntimeException("Unknown data type: " + type);
		}
	}

	static final void dataSerialize(final ServerJdbc server, final Connection conn, final String objId, final PreparedStatement ps, final int index, final BaseObject data)
			throws Exception {

		if (data == null || !data.baseHasKeysOwn()) {
			ps.setInt(index + 0, MatData.TYPE_EMPTY);
			ps.setBytes(index + 1, Transfer.EMPTY_BYTE_ARRAY);
			if (server.isUpdate3()) {
				ps.setString(index + 2, "*");
			}
		} else {
			final StoreInfo attachment = new StoreInfo(conn, objId);
			final byte[] bytes = Xml.toXmlString("data", data, false, server.getStorageExternalizer(), attachment, 512).getBytes(StandardCharsets.UTF_8);
			if (bytes.length <= 4096) {
				ps.setInt(index + 0, MatData.TYPE_SIMPLE_SMALL);
			} else {
				ps.setInt(index + 0, MatData.TYPE_SIMPLE_UNKNOWN);
			}
			ps.setBytes(index + 1, bytes);
			if (server.isUpdate3()) {
				final String guid = server.getStorageExternalizer().putExternal(attachment, "$data", "text/xml", Transfer.wrapCopier(bytes));
				ps.setString(index + 2, guid);
			}
		}
	}

	static final void delete(final ServerJdbc server, final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + server.getTnObjects() + " WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
		try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + server.getTnExtraLink() + " WHERE objId=?")) {
			ps.setString(1, objId);
			ps.execute();
		}
	}

	static final DataJdbc materialize(final ServerJdbc server, final Connection conn, final String guid) throws Exception {

		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT intDataType,intData FROM " + server.getTnObjects() + " WHERE objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int intDataType = rs.getInt(1);
					if (intDataType == MatData.TYPE_EMPTY) {
						return new DataJdbc(server, guid, BaseObject.UNDEFINED);
					}
					final TransferCopier intData;
					if (intDataType == MatData.TYPE_SIMPLE_SMALL) {
						intData = Transfer.wrapCopier(rs.getBytes(2));
					} else {
						intData = Transfer.createBuffer(rs.getBinaryStream(2)).toBinary();
					}
					return new DataJdbc(server, guid, intDataType, intData);
				}
				return null;
			}
		}
	}

	static final void serializeCreate(final ServerJdbc server,
			final Connection conn,
			final String objId,
			final String vrId,
			final String title,
			final long created,
			final String typeName,
			final String owner,
			final int state,
			final Map<String, String> extraExisting,
			final BaseObject data) throws Exception {

		{
			try (final PreparedStatement ps = conn.prepareStatement(
					"INSERT INTO " + server.getTnObjects() + "(objId,vrId,objTitle,objCreated,objDate,objOwner,objType,objState,intDataType,intData" + (server.isUpdate3()
						? ",extLink"
						: "") + ") VALUES (?,?,?,?,?,?,?,?,?,?"
							+ (server.isUpdate3()
								? ",?"
								: "")
							+ ")")) {
				ps.setString(1, objId);
				ps.setString(2, vrId);
				ps.setString(3, title);
				ps.setTimestamp(4, new Timestamp(created));
				ps.setTimestamp(5, new Timestamp(Engine.fastTime()));
				ps.setString(6, owner);
				ps.setString(7, typeName);
				ps.setInt(8, state);
				MatData.dataSerialize(server, conn, objId, ps, 9, data);
				ps.execute();
			}
		}
		if (extraExisting != null && !extraExisting.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO " + server.getTnExtraLink() + "(recId,objId,fldId) VALUES (?,?,?) ")) {
				for (final Map.Entry<String, String> record : extraExisting.entrySet()) {
					final String fldId = record.getKey();
					final String recId = record.getValue();
					try {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, Text.limitString(fldId, 32));
						ps.execute();
						ps.clearParameters();
					} catch (final Throwable t) {
						Report.exception("S2/JDBC/MAT_DATA", "CREATE: while linking extra record to an object, objId=" + objId + ", fldId=" + fldId + ", recId=" + recId, t);
					}
				}
			}
		}
	}

	static final void update(final ServerJdbc server, final Connection conn, final String objId) throws Exception {

		try (final PreparedStatement ps = conn.prepareStatement("UPDATE " + server.getTnObjects() + " SET objDate=? WHERE objId=?")) {
			ps.setTimestamp(1, new Timestamp(Engine.fastTime()));
			ps.setString(2, objId);
			ps.execute();
		}
	}

	static final void update(final ServerJdbc server,
			final Connection conn,
			final String objId,
			final String vrId,
			final String initialTitle,
			final String title,
			final long initialCreated,
			final long created,
			final String initialTypeName,
			final String typeName,
			final String owner,
			final int oldState,
			final int newState) throws Exception {

		final List<String> setPart = new ArrayList<>();
		if (oldState != newState) {
			setPart.add("objState=?");
		}
		final boolean setTitle = initialTitle != title && !initialTitle.equals(title);
		if (setTitle) {
			setPart.add("objTitle=?");
		}
		final boolean setCreated = initialCreated != created;
		if (setCreated) {
			setPart.add("objCreated=?");
		}
		final boolean setTypeName = initialTypeName != typeName && !initialTypeName.equals(typeName);
		if (setTypeName) {
			setPart.add("objType=?");
		}
		if (owner != null) {
			setPart.add("objOwner=?");
		}
		if (!setPart.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement(//
					"UPDATE " + server.getTnObjects() + //
							" SET objDate=?, vrId=?, " + String.join(", ", setPart) + //
							" WHERE objId=?"//
			)) {
				int index = 1;
				ps.setTimestamp(index++, new Timestamp(Engine.fastTime()));
				ps.setString(index++, vrId);
				if (oldState != newState) {
					ps.setInt(index++, newState);
				}
				if (setTitle) {
					ps.setString(index++, title);
				}
				if (setCreated) {
					ps.setTimestamp(index++, new Timestamp(created));
				}
				if (setTypeName) {
					ps.setString(index++, typeName);
				}
				if (owner != null) {
					ps.setString(index++, owner);
				}
				ps.setString(index, objId);
				ps.execute();
			}
		}
	}

	static final void update(final ServerJdbc server,
			final Connection conn,
			final String historyId,
			final String objId,
			final String vrId,
			final String initialTitle,
			final String title,
			final long initialCreated,
			final long created,
			final String initialTypeName,
			final String typeName,
			final String owner,
			final int oldState,
			final int newState,
			final Map<String, String> extraRemoved,
			final Map<String, String> extraExisting,
			final BaseObject removed,
			final BaseObject added) throws Exception {

		final List<String> setPart = new ArrayList<>();
		if (historyId != null || oldState != newState) {
			setPart.add("objState=?");
		}
		final boolean setTitle = historyId != null || initialTitle != title && !initialTitle.equals(title);
		if (setTitle) {
			setPart.add("objTitle=?");
		}
		final boolean setCreated = historyId != null || initialCreated != created;
		if (setCreated) {
			setPart.add("objCreated=?");
		}
		final boolean setTypeName = historyId != null || initialTypeName != typeName && !initialTypeName.equals(typeName);
		if (setTypeName) {
			setPart.add("objType=?");
		}
		if (owner != null) {
			setPart.add("objOwner=?");
		}
		final BaseObject data;
		if (historyId != null || removed.baseHasKeysOwn() || added.baseHasKeysOwn()) {
			if (extraRemoved != null && !extraRemoved.isEmpty()) {
				for (final Map.Entry<String, String> record : extraRemoved.entrySet()) {
					final String fldId = record.getKey();
					final String recId = record.getValue();
					try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + server.getTnExtraLink() + " WHERE recId=? AND objId=? AND fldId=?")) {
						ps.setString(1, recId);
						ps.setString(2, objId);
						ps.setString(3, Text.limitString(fldId, 32));
						ps.execute();
					} catch (final Throwable t) {
						Report.exception("S2/JDBC/MAT_DATA", "Exception while unlinking extra record to an object", t);
					}
				}
			}
			final BaseObject dataMap;
			{
				final int intDataType;
				final TransferCopier intData;
				try (final PreparedStatement ps = conn.prepareStatement(
						historyId == null
							? "SELECT intDataType,intData FROM " + server.getTnObjects() + " WHERE objId=?"
							: "SELECT intDataType,intData FROM " + server.getTnObjectHistory() + " WHERE objId=? AND hsId=?",
						ResultSet.TYPE_FORWARD_ONLY,
						ResultSet.CONCUR_READ_ONLY)) {
					ps.setString(1, objId);
					if (historyId != null) {
						ps.setString(2, historyId);
					}
					try (final ResultSet rs = ps.executeQuery()) {
						if (rs.next()) {
							intDataType = rs.getInt(1);
							if (intDataType == MatData.TYPE_EMPTY) {
								intData = TransferCopier.NUL_COPIER;
							} else {
								if (intDataType == MatData.TYPE_SIMPLE_SMALL || intDataType == MatData.TYPE_MULTI_SMALL) {
									intData = Transfer.createBuffer(rs.getBytes(2)).toBinary();
								} else {
									intData = Transfer.createBuffer(rs.getBinaryStream(2)).toBinary();
								}
							}
						} else {
							throw new IllegalArgumentException("No entry found to update, objId=" + objId);
						}
					}
				}
				dataMap = MatData.dataMaterialize(server, intDataType, intData, new StoreInfo(conn, objId));
			}
			if (removed != null) {
				for (final Iterator<String> iterator = removed.baseKeysOwn(); iterator.hasNext();) {
					final String key = iterator.next();
					dataMap.baseDelete(key);
				}
			}
			if (added != null) {
				dataMap.baseDefineImportAllEnumerable(added);
			}
			data = dataMap;
			setPart.add("intDataType=?, intData=?");
			if (server.isUpdate3()) {
				setPart.add("extLink=?");
			}
		} else {
			data = null;
		}
		if (!setPart.isEmpty()) {
			try (final PreparedStatement ps = conn.prepareStatement(//
					"UPDATE " + server.getTnObjects() + //
							" SET objDate=?, vrId=?, " + String.join(", ", setPart) + //
							" WHERE objId=?"//
			)) {
				int index = 1;
				ps.setTimestamp(index++, new Timestamp(Engine.fastTime()));
				ps.setString(index++, vrId);
				if (historyId != null || oldState != newState) {
					ps.setInt(index++, newState);
				}
				if (setTitle) {
					ps.setString(index++, title);
				}
				if (setCreated) {
					ps.setTimestamp(index++, new Timestamp(created));
				}
				if (setTypeName) {
					ps.setString(index++, typeName);
				}
				if (owner != null) {
					ps.setString(index++, owner);
				}
				if (data != null) {
					MatData.dataSerialize(server, conn, objId, ps, index, data);
					index += 2;
					if (server.isUpdate3()) {
						index++;
					}
				}
				ps.setString(index, objId);
				ps.execute();
			}
		}
		if (historyId != null) {
			try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + server.getTnExtraLink() + " WHERE objId=?")) {
				ps.setString(1, objId);
				ps.execute();
			}
			try (final PreparedStatement ps = conn
					.prepareStatement("INSERT INTO " + server.getTnExtraLink() + "(objId,fldId,recId) SELECT ?,fldId,recId FROM " + server.getTnExtraLink() + " WHERE objId=?")) {
				ps.setString(1, objId);
				ps.setString(2, historyId);
				ps.execute();
			}
		}
		if (extraExisting != null && !extraExisting.isEmpty()) {
			for (final Map.Entry<String, String> record : extraExisting.entrySet()) {
				final String fldId = record.getKey();
				final String recId = record.getValue();
				final String fldIdPrepared = Text.limitString(fldId, 32);
				try (final PreparedStatement ps = conn.prepareStatement("DELETE FROM " + server.getTnExtraLink() + " WHERE objId=? AND fldId=?")) {
					ps.setString(1, objId);
					ps.setString(2, fldIdPrepared);
					ps.execute();
				} catch (final Throwable t) {
					Report.exception("S2/JDBC/MAT_DATA", "UPDATE: while preparing to link extra record to an object, objId=" + objId + ", fldId=" + fldId, t);
				}
				try (final PreparedStatement ps = conn.prepareStatement("INSERT INTO " + server.getTnExtraLink() + "(recId,objId,fldId) VALUES (?,?,?)")) {
					ps.setString(1, recId);
					ps.setString(2, objId);
					ps.setString(3, fldIdPrepared);
					ps.execute();
				} catch (final Throwable t) {
					Report.exception("S2/JDBC/MAT_DATA", "UPDATE: while linking extra record to an object, objId=" + objId + ", fldId=" + fldId + ", recId=" + recId, t);
				}
			}
		}
	}

	static final void update3(final ServerJdbc server, final Connection conn, final String guid) throws Exception {

		final BaseObject map;
		try (final PreparedStatement ps = conn
				.prepareStatement("SELECT intDataType,intData FROM " + server.getTnObjects() + " WHERE objId=?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
			ps.setString(1, guid);
			try (final ResultSet rs = ps.executeQuery()) {
				if (rs.next()) {
					final int intDataType = rs.getInt(1);
					if (intDataType == MatData.TYPE_EMPTY) {
						map = BaseObject.UNDEFINED;
					} else {
						map = Xml.toBase("dataUpdate3", Transfer.wrapCopier(rs.getBytes(2)), StandardCharsets.UTF_8, null, server.getStorageExternalizer(), null);
					}
				} else {
					return;
				}
			}
		}
		try (final PreparedStatement ps = conn.prepareStatement("UPDATE " + server.getTnObjects() + " SET intDataType=?,intData=?,extLink=? WHERE objId=?")) {
			MatData.dataSerialize(server, conn, guid, ps, 1, map);
			ps.setString(4, guid);
			ps.execute();
		}
	}
}
