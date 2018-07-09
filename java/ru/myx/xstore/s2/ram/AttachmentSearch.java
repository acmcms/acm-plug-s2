/**
 * 
 */
package ru.myx.xstore.s2.ram;

import java.util.Map;

import ru.myx.ae3.report.Report;
import ru.myx.xstore.s2.BaseLink;

final class AttachmentSearch {
	private final BaseLink	searcher;
	
	private final int		limit;
	
	private final boolean	all;
	
	private final long		timeout;
	
	private final String	sort;
	
	private final long		dateStart;
	
	private final long		dateEnd;
	
	private final String	filter;
	
	AttachmentSearch(final BaseLink searcher,
			final int limit,
			final boolean all,
			final long timeout,
			final String sort,
			final long dateStart,
			final long dateEnd,
			final String filter) {
		this.searcher = searcher;
		this.limit = limit;
		this.all = all;
		this.timeout = timeout;
		this.sort = sort;
		this.dateStart = dateStart;
		this.dateEnd = dateEnd;
		this.filter = filter;
	}
	
	final Map.Entry<String, Object>[] doSearch() {
		Report.info( CreatorSearch.OWNER, "MISS: "
				+ this.limit
				+ ", "
				+ this.all
				+ ", "
				+ this.timeout
				+ ", "
				+ this.sort
				+ ", "
				+ this.dateStart
				+ ", "
				+ this.dateEnd
				+ ", "
				+ this.filter );
		return this.searcher.search( this.limit,
				this.all,
				this.timeout,
				this.sort,
				this.dateStart,
				this.dateEnd,
				this.filter );
	}
}
