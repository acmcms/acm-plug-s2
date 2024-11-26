/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.xstore.s2.Server;

final class CreatorIdentitySearch implements CreationHandlerObject<Boolean, String[]> {
	
	private static final long TTL = 15L * 60_000L;

	private final Server parent;

	private final String[] NULL_OBJECT;

	CreatorIdentitySearch(final Server parent, final String[] NULL_OBJECT) {
		
		this.parent = parent;
		this.NULL_OBJECT = NULL_OBJECT;
	}

	@Override
	public String[] create(final Boolean attachment, final String key) {
		
		try {
			final String[] result = this.parent.searchLinksForIdentity(key, attachment == Boolean.TRUE);
			return result == null
				? this.NULL_OBJECT
				: result;
		} catch (final RuntimeException e) {
			throw e;
		} catch (final Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public long getTTL() {
		
		return CreatorIdentitySearch.TTL;
	}
}
