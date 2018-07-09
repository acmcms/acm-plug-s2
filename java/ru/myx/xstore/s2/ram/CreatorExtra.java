/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.extra.External;
import ru.myx.ae3.extra.ExternalHandler;

final class CreatorExtra implements CreationHandlerObject<Object, External> {
	private static final long		TTL	= 30L * 1000L * 60L;
	
	private final ExternalHandler	parent;
	
	private final External			NULL_OBJECT;
	
	CreatorExtra(final ExternalHandler parent, final External NULL_OBJECT) {
		this.parent = parent;
		this.NULL_OBJECT = NULL_OBJECT;
	}
	
	@Override
	public External create(final Object attachment, final String key) {
		try {
			final External result = this.parent.getExternal( attachment, key );
			return result == null
					? this.NULL_OBJECT
					: result;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public long getTTL() {
		return CreatorExtra.TTL;
	}
}
