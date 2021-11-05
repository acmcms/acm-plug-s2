/*
 * Created on 30.08.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae2.indexing.IndexingDictionaryAbstract;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class DictionaryJdbc extends IndexingDictionaryAbstract {
	private final ServerJdbc	server;
	
	private final StorageImpl	storage;
	
	DictionaryJdbc(final ServerJdbc server, final StorageImpl storage) {
		this.server = server;
		this.storage = storage;
	}
	
	@Override
	public boolean areCodesUnique() {
		return true;
	}
	
	@Override
	public int[] getPatternCodes(final String pattern, final boolean exact, final boolean required) {
		try (final Connection conn = this.storage.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement( "SELECT code FROM "
					+ this.server.getTnDictionary()
					+ " WHERE word LIKE ? AND exact=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY )) {
				ps.setString( 1, pattern.replace( '*', '%' ).replace( '?', '_' ) );
				ps.setString( 2, exact
						? "Y"
						: "N" );
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						final List<Integer> result = new ArrayList<>();
						do {
							result.add( Integer.valueOf( rs.getInt( 1 ) ) );
						} while (rs.next());
						final int size = result.size();
						if (size == 1) {
							return new int[] { result.get( 0 ).intValue() };
						}
						final int[] array = new int[size];
						for (int i = size - 1; i >= 0; --i) {
							array[i] = result.get( i ).intValue();
						}
						return array;
					}
					return required
							? new int[] { -1 }
							: null;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public int getWordCode(final String word, final boolean exact, final boolean required) {
		try (final Connection conn = this.storage.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement( "SELECT code FROM "
					+ this.server.getTnDictionary()
					+ " WHERE word=? AND exact=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY )) {
				ps.setString( 1, word );
				ps.setString( 2, exact
						? "Y"
						: "N" );
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getInt( 1 );
					}
					return required
							? -1
							: 0;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public int storeWordCode(final String word, final boolean exact, final Object attachment) {
		try (final Connection conn = this.storage.nextConnection()) {
			try (final PreparedStatement ps = conn.prepareStatement( "SELECT code FROM "
					+ this.server.getTnDictionary()
					+ " WHERE word=? AND exact=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY )) {
				ps.setString( 1, word );
				ps.setString( 2, exact
						? "Y"
						: "N" );
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getInt( 1 );
					}
				}
			}
			try (final PreparedStatement ps = conn.prepareStatement( "INSERT INTO "
					+ this.server.getTnDictionary()
					+ "(word,exact) VALUES (?,?)" )) {
				ps.setString( 1, word );
				ps.setString( 2, exact
						? "Y"
						: "N" );
				ps.execute();
			}
			try (final PreparedStatement ps = conn.prepareStatement( "SELECT code FROM "
					+ this.server.getTnDictionary()
					+ " WHERE word=? AND exact=?",
					ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_READ_ONLY )) {
				ps.setString( 1, word );
				ps.setString( 2, exact
						? "Y"
						: "N" );
				try (final ResultSet rs = ps.executeQuery()) {
					if (rs.next()) {
						return rs.getInt( 1 );
					}
					return 0;
				}
			}
		} catch (final SQLException e) {
			throw new RuntimeException( e );
		}
	}
}
