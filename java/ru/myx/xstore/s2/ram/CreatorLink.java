/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.xstore.s2.BaseLink;
import ru.myx.xstore.s2.Server;

final class CreatorLink implements CreationHandlerObject<Void, BaseLink> {
	private static final long		TTL	= 15L * 1000L * 60L;
	
	private final ServerRamCache	server;
	
	private final Server			parent;
	
	private final BaseLink			NULL_OBJECT;
	
	CreatorLink(final ServerRamCache server, final Server parent, final BaseLink NULL_OBJECT) {
		this.server = server;
		this.parent = parent;
		this.NULL_OBJECT = NULL_OBJECT;
	}
	
	@Override
	public BaseLink create(final Void attachment, final String key) {
		try {
			final BaseLink result = this.parent.getLink( key );
			return result == null
					? this.NULL_OBJECT
					: new LinkCached( this.server, result );
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException( e );
		}
	}
	
	@Override
	public long getTTL() {
		return CreatorLink.TTL;
	}
}
