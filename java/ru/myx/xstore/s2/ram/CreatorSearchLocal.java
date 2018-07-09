/*
 * Created on 09.07.2004
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.cache.CacheL2;
import ru.myx.ae3.cache.CreationHandlerObject;
import ru.myx.ae3.report.Report;
import ru.myx.xstore.s2.BaseLink;

final class CreatorSearchLocal implements CreationHandlerObject<AttachmentSearchLocal, String[]> {
	static final String			OWNER	= "S2-RAM-SRCHLOCAL";
	
	private static final long	TTL		= 15L * 1000L * 60L;
	
	private final String[]		NULL_OBJECT;
	
	CreatorSearchLocal(final String[] NULL_OBJECT) {
		this.NULL_OBJECT = NULL_OBJECT;
	}
	
	@Override
	public final String[] create(final AttachmentSearchLocal attachment, final String key) {
		try {
			final String[] result = attachment.doSearch();
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
	public final long getTTL() {
		return CreatorSearchLocal.TTL;
	}
	
	final String[] search(
			final CacheL2<Object> cacheTree,
			final BaseLink searcher,
			final int limit,
			final boolean all,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		final String guid = searcher.getGuid();
		final String key = new StringBuilder().append( limit ).append( '\n' ).append( all ).append( '\n' )
				.append( sort ).append( '\n' ).append( dateStart ).append( '\n' ).append( dateEnd ).append( '\n' )
				.append( filter ).toString();
		final String[] check1 = cacheTree.get( guid, key );
		if (check1 != null) {
			Report.info( CreatorSearchLocal.OWNER, "HIT: "
					+ limit
					+ ", "
					+ all
					+ ", "
					+ sort
					+ ", "
					+ dateStart
					+ ", "
					+ dateEnd
					+ ", "
					+ filter );
			return check1 == this.NULL_OBJECT
					? null
					: check1;
		}
		final AttachmentSearchLocal attachment = new AttachmentSearchLocal( searcher,
				limit,
				all,
				sort,
				dateStart,
				dateEnd,
				filter );
		final String[] check2 = cacheTree.get( guid, key, attachment, key, this );
		return check2 == this.NULL_OBJECT
				? null
				: check2;
	}
}
