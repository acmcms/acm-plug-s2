/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae3.cache.CreationHandlerObject;

final class CreatorDictionaryPattern implements CreationHandlerObject<Void, int[]> {
	
	private static final long TTL = 10L * 60_000L;

	private final IndexingDictionary parent;

	private final boolean exact;

	private final boolean required;

	private final int[] NULL_OBJECT;

	CreatorDictionaryPattern(final IndexingDictionary parent, final boolean exact, final boolean required, final int[] NULL_OBJECT) {
		
		this.parent = parent;
		this.exact = exact;
		this.required = required;
		this.NULL_OBJECT = NULL_OBJECT;
	}

	@Override
	public int[] create(final Void attachment, final String key) {
		
		try {
			final int[] result = this.parent.getPatternCodes(key, this.exact, this.required);
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
		
		return CreatorDictionaryPattern.TTL;
	}
}
