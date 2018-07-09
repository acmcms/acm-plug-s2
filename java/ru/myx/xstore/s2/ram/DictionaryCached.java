/*
 * Created on 31.08.2004
 * 
 * Window - Preferences - Java - Code Style - Code Templates
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae2.indexing.IndexingDictionary;
import ru.myx.ae2.indexing.IndexingDictionaryAbstract;
import ru.myx.ae3.cache.CacheL2;

/**
 * @author myx
 * 
 *         Window - Preferences - Java - Code Style - Code Templates
 */
final class DictionaryCached extends IndexingDictionaryAbstract {
	private static final int[]				NULL_OBJECT	= new int[] { 1, 1 };
	
	private final IndexingDictionary		parent;
	
	private final CacheL2<int[]>			cache;
	
	private final CreatorDictionaryPattern	creatorExactOptional;
	
	private final CreatorDictionaryPattern	creatorInexactOptional;
	
	private final CreatorDictionaryPattern	creatorExactRequired;
	
	private final CreatorDictionaryPattern	creatorInexactRequired;
	
	DictionaryCached(final IndexingDictionary parent, final CacheL2<int[]> cache) {
		this.parent = parent;
		this.cache = cache;
		this.creatorExactOptional = new CreatorDictionaryPattern( parent, true, false, DictionaryCached.NULL_OBJECT );
		this.creatorInexactOptional = new CreatorDictionaryPattern( parent, false, false, DictionaryCached.NULL_OBJECT );
		this.creatorExactRequired = new CreatorDictionaryPattern( parent, true, true, DictionaryCached.NULL_OBJECT );
		this.creatorInexactRequired = new CreatorDictionaryPattern( parent, false, true, DictionaryCached.NULL_OBJECT );
	}
	
	@Override
	public boolean areCodesUnique() {
		return this.parent.areCodesUnique();
	}
	
	@Override
	public int[] getPatternCodes(final String pattern, final boolean exact, final boolean required) {
		final int[] result;
		if (exact) {
			if (required) {
				result = this.cache.get( pattern, "#E", null, pattern, this.creatorExactRequired );
			} else {
				result = this.cache.get( pattern, "$E", null, pattern, this.creatorExactOptional );
			}
		} else {
			if (required) {
				result = this.cache.get( pattern, "#I", null, pattern, this.creatorInexactRequired );
			} else {
				result = this.cache.get( pattern, "$I", null, pattern, this.creatorInexactOptional );
			}
		}
		return result == DictionaryCached.NULL_OBJECT
				? null
				: result;
	}
	
	@Override
	public int getWordCode(final String word, final boolean exact, final boolean required) {
		return this.parent.getWordCode( word, exact, required );
	}
	
	@Override
	public int storeWordCode(final String word, final boolean exact, final Object attachment) {
		return this.parent.storeWordCode( word, exact, attachment );
	}
}
