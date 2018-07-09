/*
 * Created on 06.09.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.jdbc;

import java.sql.Connection;

import ru.myx.ae1.storage.StorageImpl;
import ru.myx.ae3.Engine;
import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class ExternalizerJdbc implements ExternalHandler {
	private final ServerJdbc	server;
	
	private final Object		issuer;
	
	private final StorageImpl	storage;
	
	ExternalizerJdbc(final ServerJdbc server, final Object issuer, final StorageImpl storage) {
		this.server = server;
		this.issuer = issuer;
		this.storage = storage;
	}
	
	@Override
	public boolean checkIssuer(final Object issuer) {
		return issuer != null && issuer == this.issuer;
	}
	
	@Override
	public External getExternal(final Object attachment, final String identifier) throws Exception {
		if (attachment != null) {
			final StoreInfo info = (StoreInfo) attachment;
			final Connection conn = info.getConnection();
			return MatExtra.materialize( this.server, this.issuer, conn, identifier );
		}
		return new ExtraJdbc( this.server, this.issuer, this.storage, identifier );
	}
	
	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {
		if (attachment != null) {
			final StoreInfo info = (StoreInfo) attachment;
			final Connection conn = info.getConnection();
			return MatExtra.contains( this.server, conn, identifier );
		}
		try (final Connection conn = this.server.getStorage().nextConnection()) {
			if (conn == null) {
				throw new RuntimeException( "Database is not accessible!" );
			}
			return MatExtra.contains( this.server, conn, identifier );
		}
	}
	
	@Override
	public String putExternal(final Object attachment, final String key, final String type, final TransferCopier copier)
			throws Exception {
		if (attachment != null) {
			final String identity = Engine.createGuid();
			final StoreInfo info = (StoreInfo) attachment;
			final Connection conn = info.getConnection();
			MatExtra.serialize( this.server, conn, identity, info.getObjId(), key, Engine.fastTime(), type, copier );
			return identity;
		}
		throw new RuntimeException( "StoreInfo is not available!" );
	}
}
