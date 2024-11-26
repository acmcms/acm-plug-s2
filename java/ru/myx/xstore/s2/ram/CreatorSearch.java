/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import java.util.Map;

import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s2.BaseLink;

final class CreatorSearch implements CreationHandlerObject<AttachmentSearch, Map.Entry<String, Object>[]> {
	
	static final String OWNER = "S2-RAM-SRCH";

	private static final long TTL = 15L * 60_000L;

	private final Map.Entry<String, Object>[] NULL_OBJECT;

	CreatorSearch(final Map.Entry<String, Object>[] NULL_OBJECT) {
		
		this.NULL_OBJECT = NULL_OBJECT;
	}

	@Override
	public final Map.Entry<String, Object>[] create(final AttachmentSearch attachment, final String key) {
		
		try {
			final Map.Entry<String, Object>[] result = attachment.doSearch();
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
	public final long getTTL() {
		
		return CreatorSearch.TTL;
	}

	final Map.Entry<String, Object>[] search(final CacheL2<Object> cacheTree,
			final BaseLink searcher,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		
		final String guid = searcher.getGuid();
		final String key = new StringBuilder().append(limit).append('\n').append(all).append('\n').append(timeout).append('\n').append(sort).append('\n').append(dateStart)
				.append('\n').append(dateEnd).append('\n').append(filter).toString();
		final Map.Entry<String, Object>[] check1 = cacheTree.get(guid, key);
		if (check1 != null) {
			Report.info(CreatorSearch.OWNER, "HIT: " + limit + ", " + all + ", " + timeout + ", " + sort + ", " + dateStart + ", " + dateEnd + ", " + filter);
			return check1 == this.NULL_OBJECT
				? null
				: check1;
		}
		final AttachmentSearch attachment = new AttachmentSearch(searcher, limit, all, timeout, sort, dateStart, dateEnd, filter);
		final Map.Entry<String, Object>[] check2 = cacheTree.get(guid, key, attachment, key, this);
		return check2 == this.NULL_OBJECT
			? null
			: check2;
	}
}
