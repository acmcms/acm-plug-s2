package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;

import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae3.extra.AbstractExtra;
import ru.myx.ae3.extra.External;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class ExtraJdbc extends AbstractExtra {
	private final ServerJdbc	server;
	
	private final StorageImpl	storage;
	
	private External			extra	= null;
	
	private final Object		issuer;
	
	ExtraJdbc(final ServerJdbc server, final Object issuer, final StorageImpl storage, final String recId) {
		super( recId );
		this.issuer = issuer;
		this.server = server;
		this.storage = storage;
	}
	
	@Override
	public Object baseValue() {
		final External extra = this.getExtra();
		return extra == null
				? null
				: extra.baseValue();
	}
	
	private final External getExtra() {
		if (this.extra == null) {
			synchronized (this) {
				if (this.extra == null) {
					final Connection conn = this.storage.nextConnection();
					if (conn == null) {
						throw new RuntimeException( "Database is not available!" );
					}
					try {
						try {
							this.extra = MatExtra.materialize( this.server, this.issuer, conn, this.recId );
						} finally {
							conn.close();
						}
					} catch (final RuntimeException e) {
						throw e;
					} catch (final Exception e) {
						throw new RuntimeException( e );
					}
				}
			}
		}
		return this.extra;
	}
	
	@Override
	public long getRecordDate() {
		return this.getExtra().getRecordDate();
	}
	
	@Override
	public Object getRecordIssuer() {
		return this.issuer;
	}
	
	@Override
	public Object toBinary() {
		final External extra = this.getExtra();
		return extra == null
				? null
				: extra.toBinary();
	}
	
	@Override
	public String toString() {
		return String.valueOf( this.getExtra() );
	}
}
