/*
 * Created on 07.09.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.binary.TransferCopier;
import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class ExternalizerCached implements ExternalHandler {
	
	private static final External	NULL_OBJECT	= new External() {
													@Override
													public Object baseValue() {
														return null;
													}
													
													@Override
													public String getIdentity() {
														return null;
													}
													
													@Override
													public long getRecordDate() {
														return 0;
													}
													
													@Override
													public Object getRecordIssuer() {
														return null;
													}
													
													@Override
													public Object toBinary() {
														return null;
													}
												};
	
	private final ExternalHandler	parent;
	
	private final CacheL2<External>	cache;
	
	private final CreatorExtra		creatorExtra;
	
	ExternalizerCached(final ExternalHandler parent, final CacheL2<External> cache) {
		this.parent = parent;
		this.cache = cache;
		this.creatorExtra = new CreatorExtra( parent, ExternalizerCached.NULL_OBJECT );
	}
	
	@Override
	public boolean checkIssuer(final Object issuer) {
		return this.parent.checkIssuer( issuer );
	}
	
	@Override
	public final External getExternal(final Object attachment, final String identifier) throws Exception {
		final External result = this.cache.get( identifier, "E", attachment, identifier, this.creatorExtra );
		return result == ExternalizerCached.NULL_OBJECT
				? null
				: result;
	}
	
	@Override
	public final boolean hasExternal(final Object attachment, final String identifier) throws Exception {
		final Object result = this.cache.get( identifier, "E", attachment, identifier, this.creatorExtra );
		return result != null && result != ExternalizerCached.NULL_OBJECT;
	}
	
	@Override
	public final String putExternal(
			final Object attachment,
			final String key,
			final String type,
			final TransferCopier copier) throws Exception {
		final String identifier = this.parent.putExternal( attachment, key, type, copier );
		this.cache.remove( identifier, "E" );
		return identifier;
	}
}
