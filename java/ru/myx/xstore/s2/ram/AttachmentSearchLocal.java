/**
 * 
 */
package ru.myx.xstore.s2.ram;

import ru.myx.ae3.report.Report;
import ru.myx.xstore.s2.BaseLink;

final class AttachmentSearchLocal {
	private final BaseLink	searcher;
	
	private final int		limit;
	
	private final boolean	all;
	
	private final String	sort;
	
	private final long		dateStart;
	
	private final long		dateEnd;
	
	private final String	filter;
	
	AttachmentSearchLocal(final BaseLink searcher,
			final int limit,
			final boolean all,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		this.searcher = searcher;
		this.limit = limit;
		this.all = all;
		this.sort = sort;
		this.dateStart = dateStart;
		this.dateEnd = dateEnd;
		this.filter = filter;
	}
	
	final String[] doSearch() {
		Report.info( CreatorSearchLocal.OWNER, "MISS: "
				+ this.limit
				+ ", "
				+ this.all
				+ ", "
				+ this.sort
				+ ", "
				+ this.dateStart
				+ ", "
				+ this.dateEnd
				+ ", "
				+ this.filter );
		return this.searcher.searchLocal( this.limit, this.all, this.sort, this.dateStart, this.dateEnd, this.filter );
	}
}
